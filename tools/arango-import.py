from pyArango.connection import Connection, CreationError
import os
import logging
import sys
import psycopg2

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), '..')))
from lib.repository import SampleRepository
from lib.sample import JsonFactory

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaArangoExport')
logger.setLevel(logging.DEBUG if 'DEBUG' in os.environ else logging.WARNING)
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

for sha256 in sys.argv[1:]:
    with db.cursor() as cursor:
        cursor.execute('SELECT id FROM sample WHERE (hash_sha256 = %s)', (sha256,))
        sample_id = cursor.fetchall()[0][0]

    sample = sample_repository.by_ids([sample_id])[0]

    json_sample = json_factory.from_sample(sample)
    if 'id' in json_sample:
        del json_sample['id']
    if 'sections' in json_sample:
        del json_sample['sections']
    if 'resources' in json_sample:
        del json_sample['resources']

    arango_sample = arango_database['sample'].createDocument(initValues=json_sample)
    arango_sample._key = sha256
    try:
        arango_sample.save()
    except CreationError:
        logger.warning('sample with hash %s already exists, skipped.' % sha256)
        continue

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
