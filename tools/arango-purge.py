from pyArango.connection import Connection
import os
import logging

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('ArangoPurge')
logger.setLevel(logging.DEBUG if 'DEBUG' in os.environ else logging.WARNING)

arango_connection = Connection(
    arangoURL=os.environ['ARANGODB_URL'],
    username=os.environ['ARANGODB_USERNAME'],
    password=os.environ['ARANGODB_PASSWORD']
)

arango_database = arango_connection['kurasuta']
for collection in arango_database.collections:
    if collection.startswith('_'):
        continue

    logger.debug('Deleting all documents from %s...' % collection)
    aql = '''
        FOR o IN %(collection_name)s
            REMOVE { _key: o._key } IN %(collection_name)s
      ''' % {'collection_name': str(collection)}
    queryResult = arango_database.AQLQuery(aql)
logger.debug('all done.')
