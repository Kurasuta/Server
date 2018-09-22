from pyArango.connection import Connection
from pyArango.theExceptions import CreationError
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
    for line in fp:
        sha256 = line.strip()
        if not sha256:
            continue
        if sha256 == state:
            logger.info('Skipped %i hashes, starting import now' % skipped)
            run_import = True
            continue
        elif not run_import:
            skipped += 1
            continue

        with db.cursor() as cursor:
            cursor.execute('SELECT id FROM sample WHERE (hash_sha256 = %s)', (sha256,))
            sample = sample_repository.by_ids([(cursor.fetchall()[0][0])])[0]

        json_sample = json_factory.from_sample(sample)
        if 'sections' in json_sample:
            del json_sample['sections']
        if 'resources' in json_sample:
            del json_sample['resources']
        arango_sample = arango_database['sample'].createDocument(initValues=json_sample)
        arango_sample._key = sample.hash_sha256
        arango_sample.save()

        for i, section in enumerate(sample.sections):
            try:
                arango_section = arango_database['section'][section.hash_sha256]
            except KeyError:
                arango_section = arango_database['section'].createDocument(
                    initValues=json_factory.from_section(section)
                )
                arango_section._key = section.hash_sha256
                arango_section.save()

            edge = arango_database['has_section'].createDocument(initValues={
                '_from': 'sample/%s' % sample.hash_sha256,
                '_to': 'section/%s' % section.hash_sha256,
                'sort_order': i,
            })
            edge.save()

        for i, resource in enumerate(sample.resources):
            try:
                arango_resource = arango_database['resource'][resource.hash_sha256]
            except KeyError:
                arango_resource = arango_database['resource'].createDocument(
                    initValues=json_factory.from_resource(resource)
                )
                arango_resource._key = resource.hash_sha256
                arango_resource.save()

            edge = arango_database['has_resource'].createDocument(initValues={
                '_from': 'sample/%s' % sample.hash_sha256,
                '_to': 'resource/%s' % resource.hash_sha256,
                'sort_order': i,
            })
            edge.save()
        logger.info('Imported %s (%i sections, %i resources)' % (
            sample.hash_sha256,
            len(sample.sections),
            len(sample.resources)
        ))

        imported += 1
        if imported % 100 == 0:
            logger.info('Imported %i records, recording state' % imported)
            state_file.dump(sha256)
