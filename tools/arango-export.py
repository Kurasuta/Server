from pyArango.connection import Connection
import os
import logging
import json
import sys
import psycopg2

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), '..')))
from lib.repository import SampleRepository
from lib.sample import JsonFactory


class StateFile(object):
    def __init__(self, hash_file, state_file_name):
        self.hash_file = hash_file
        self.state_file_name = state_file_name

    def dump(self, state):
        if os.path.exists(self.state_file_name):
            with open(self.state_file_name, 'r') as fp:
                all_states = json.load(fp)
            all_states[self.hash_file] = state
        else:
            all_states = {self.hash_file: state}

        with open(self.state_file_name, 'w') as fp:
            json.dump(all_states, fp)

    def load(self):
        if os.path.exists(self.state_file_name):
            with open(self.state_file_name, 'r') as fp:
                all_states = json.load(fp)
            if self.hash_file in all_states:
                return all_states[self.hash_file]


logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaArangoExport')
logger.setLevel(logging.DEBUG if 'DEBUG' in os.environ else logging.WARNING)

hash_file = sys.argv[1]
state_file_name = sys.argv[2]

if not os.path.exists(hash_file):
    print('ERROR %s must exist and contain one hash per line' % hash_file)
    exit()

state_file = StateFile(hash_file, state_file_name)
state = state_file.load()
run_import = state is None
imported = 0
skipped = 0

logger.debug('Connecting to postgres database...')
db = psycopg2.connect(os.environ['POSTGRES_DATABASE_LINK'])

logger.debug('Connecting to arango database...')
arango_connection = Connection(
    arangoURL=os.environ['ARANGODB_URL'],
    username=os.environ['ARANGODB_USERNAME'],
    password=os.environ['ARANGODB_PASSWORD']
)

arango_database = arango_connection['kurasuta']
sample_repository = SampleRepository(db)
json_factory = JsonFactory()

with open(hash_file) as fp:
    hashes = [line.strip() for line in fp if line.strip()]
logger.info('Found %i hashes' % len(hashes))

for hashes_pos in range(len(hashes)):
    sha256 = hashes[hashes_pos]
    if sha256 == state:
        logger.info('Skipped %i hashes, starting import now' % skipped)
        run_import = True
        continue
    elif not run_import:
        skipped += 1
        continue

    current_one_exists = sha256 in arango_database['sample']
    next_one_exists = hashes_pos + 1 < len(hashes) and hashes[hashes_pos + 1] in arango_database['sample']
    if current_one_exists and next_one_exists:
        continue

    arango_sample = None
    # if next hash does not exist in ArangoDB but current one does, delete all relations for the current one and start
    # import with current one. The script will skip insertion of the sample itself and potentially also of its sections
    # and resources and ensure that edges are correctly (re-)created.
    if not next_one_exists and current_one_exists:
        for sample_edge_collection in ['has_resource', 'has_section', 'has_tag', 'has_source']:
            query = arango_database['sample_edge_collection'].fetchByExample({'_from': 'sample/%s' % sha256})
            for doc in query:
                doc.delete()
        arango_sample = arango_database['sample'][sha256]

    with db.cursor() as cursor:
        cursor.execute('SELECT id FROM sample WHERE (hash_sha256 = %s)', (sha256,))
        sample_id = cursor.fetchall()[0][0]
        sample = sample_repository.by_ids([sample_id])[0]

    if arango_sample is None:
        json_sample = json_factory.from_sample(sample)
        if 'sections' in json_sample:
            del json_sample['sections']
        if 'resources' in json_sample:
            del json_sample['resources']

        arango_sample = arango_database['sample'].createDocument(initValues=json_sample)
        arango_sample._key = sha256
        arango_sample.save()

    with db.cursor() as cursor:
        cursor.execute('''
            SELECT t.name
            FROM sample_tag t
            LEFT JOIN sample_has_tag ht ON (ht.tag_id = t.id)
            WHERE (ht.sample_id = %s)
        ''', (sample_id,))
        for row in cursor.fetchall():
            json_tag = {'name': row[0]}
            query = arango_database['tag'].fetchFirstExample(json_tag)
            if len(query):
                arango_tag = query[0]
            else:
                arango_tag = arango_database['tag'].createDocument(initValues=json_tag)
                arango_tag.save()
            edge = arango_database['has_tag'].createDocument(initValues={
                '_from': 'sample/%s' % sha256,
                '_to': arango_tag._id,
            })
            edge.save()

        cursor.execute('''
            SELECT s.identifier
            FROM sample_source s
            LEFT JOIN sample_has_source hs ON (hs.source_id = s.id)
            WHERE (hs.sample_id = %s)
        ''', (sample_id,))
        for row in cursor.fetchall():
            json_source = {'name': row[0]}
            query = arango_database['source'].fetchFirstExample(json_source)
            if len(query):
                arango_source = query[0]
            else:
                arango_source = arango_database['source'].createDocument(initValues=json_source)
                arango_source.save()
            edge = arango_database['has_source'].createDocument(initValues={
                '_from': 'sample/%s' % sha256,
                '_to': arango_source._id,
            })
            edge.save()

    for section_no, section in enumerate(sample.sections):
        try:
            arango_section = arango_database['section'][section.hash_sha256]
        except KeyError:
            arango_section = arango_database['section'].createDocument(
                initValues=json_factory.from_section(section)
            )
            arango_section._key = section.hash_sha256
            arango_section.save()

        edge = arango_database['has_section'].createDocument(initValues={
            '_from': 'sample/%s' % sha256,
            '_to': 'section/%s' % section.hash_sha256,
            'sort_order': section_no,
        })
        edge.save()

    for resource_no, resource in enumerate(sample.resources):
        try:
            arango_resource = arango_database['resource'][resource.hash_sha256]
        except KeyError:
            arango_resource = arango_database['resource'].createDocument(
                initValues=json_factory.from_resource(resource)
            )
            arango_resource._key = resource.hash_sha256
            arango_resource.save()

        edge = arango_database['has_resource'].createDocument(initValues={
            '_from': 'sample/%s' % sha256,
            '_to': 'resource/%s' % resource.hash_sha256,
            'sort_order': resource_no,
        })
        edge.save()
    logger.info('Imported %s (%i sections, %i resources)' % (
        sha256,
        len(sample.sections),
        len(sample.resources)
    ))

    imported += 1
    if imported % 100 == 0:
        logger.info('Imported %i records, recording state' % imported)
        state_file.dump(sha256)
