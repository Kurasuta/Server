from pyArango.connection import Connection, CreationError
import csv
import os
import logging
import sys

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaArangoDumpHashes')
logger.setLevel(logging.DEBUG if 'DEBUG' in os.environ and os.environ['DEBUG'] else logging.INFO)

logger.debug('Connecting to arango database...')
connection = Connection(
    arangoURL=os.environ['ARANGODB_URL'],
    username=os.environ['ARANGODB_USERNAME'],
    password=os.environ['ARANGODB_PASSWORD']
)
database = connection['kurasuta']

result = database.AQLQuery('FOR s IN sample RETURN s.hash_sha256', rawResults=True, batchSize=100)
for hash in result:
    print(hash)
