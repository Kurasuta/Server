from pyArango.connection import Connection, CreationError
import csv
import os
import logging
import sys

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaArangoCreationEnrichment')
logger.setLevel(logging.DEBUG if 'DEBUG' in os.environ and os.environ['DEBUG'] else logging.INFO)

logger.debug('Connecting to arango database...')
arango_connection = Connection(
    arangoURL=os.environ['ARANGODB_URL'],
    username=os.environ['ARANGODB_USERNAME'],
    password=os.environ['ARANGODB_PASSWORD']
)
arango_database = arango_connection['kurasuta']

update_count = 0
tasking_count = 0
tasking_skipped = 0
logger.info('Starting...')
with open(sys.argv[1], 'r') as fp:
    for row in csv.reader(fp):
        sha256, completed_at, created_at = row
        if completed_at:
            completed_at = '%s UTC' % completed_at[:19]
        if created_at:
            created_at = '%s UTC' % created_at[:19]
        if completed_at:
            sample = arango_database['sample'][sha256]
            if not sample:
                raise Exception('Expected to find sample with SHA256 %s' % sha256)
            logger.debug('Updating %s, it was completed at %s' % (sha256, completed_at))
            sample['completed_at'] = completed_at
            sample['completed_in_month'] = completed_at[:7]
            sample.save()
            update_count += 1
        elif created_at:
            logger.debug('Tasking %s since %s' % (sha256, created_at))
            task = arango_database['task'].createDocument()
            task['sha256'] = sha256
            task['created_at'] = created_at
            try:
                task.save()
            except CreationError as e:
                tasking_skipped += 1
                continue
            tasking_count += 1
logger.info('All done (updated %i samples, tasked %i hashes, %i taskings skipped).' % (
    update_count,
    tasking_count,
    tasking_skipped
))
