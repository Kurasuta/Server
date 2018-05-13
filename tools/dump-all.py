#!/usr/bin/env python
import os
import psycopg2
import logging
import argparse
from ..lib.repository import SampleRepository
from ..lib.sample import JsonFactory

logging.basicConfig()
logger = logging.getLogger('KurasutaDumper')
logger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('target_file_name')
args = parser.parse_args()

target_file_name = args.target_file_name
if os.path.exists(target_file_name):
    raise Exception('File name "%s" exists' % target_file_name)

db = psycopg2.connect(os.environ['POSTGRES_DATABASE_LINK'])
sample_repository = SampleRepository(db)
json_factory = JsonFactory(db)
with db.cursor() as cursor:
    logger.info('Selecting all ids...')
    cursor.execute('SELECT id FROM sample')
    logger.info('Found %i ids.' % cursor.rowcount)
    for row in cursor:
        sample = sample_repository.by_ids([row[0]])
        with open(target_file_name, 'a') as fp:
            fp.write('%s\n' % json_factory.from_sample(sample))
