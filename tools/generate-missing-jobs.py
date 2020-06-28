#!/usr/bin/env python3.7
import os
import datetime
import logging

from pyArango.connection import Connection, CreationError

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaArangoDumpHashes')
logger.setLevel(logging.DEBUG if 'DEBUG' in os.environ and os.environ['DEBUG'] else logging.INFO)
kurasuta_storage_dir = os.environ['KURASUTA_STORAGE_DIR']

if not os.path.exists(kurasuta_storage_dir) or not os.path.isdir(kurasuta_storage_dir):
    logger.error('specified directory "{}" not valid'.format(kurasuta_storage_dir))
    exit()

logger.debug('Connecting to arango database...')
connection = Connection(
    arangoURL=os.environ['ARANGODB_URL'],
    username=os.environ['ARANGODB_USERNAME'],
    password=os.environ['ARANGODB_PASSWORD']
)
database = connection['kurasuta']

for current_dir, file_names, _ in os.walk(kurasuta_storage_dir):
    logger.info(F'Traversing "{current_dir}"...')
    if file_names:
        for sha256 in file_names:
            current_files = os.path.join(kurasuta_storage_dir, current_dir, sha256)
            if not database.has_document({'_id': sha256}):
                task = database['task'].createDocument()
                task['sha256'] = sha256
                task['created_at'] = datetime.datetime.utcnow()
                try:
                    task.save()
                except CreationError as e:
                    logger.warning(F'Cannot create ask for {sha256}')
