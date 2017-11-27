import os
import psycopg2
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('hash')
parser.add_argument('source')
args = parser.parse_args()
connection = psycopg2.connect(os.environ['DATABASE'])
with connection.cursor() as cursor:
    if len(args.hash) == 64:
        field_name = 'hash_sha256'
    elif len(args.hash) == 32:
        field_name = 'hash_md5'
    elif len(args.hash) == 40:
        field_name = 'hash_sha1'
    else:
        raise Exception('unknown hash format')

    cursor.execute('SELECT id FROM sample WHERE (%s = %%s)' % field_name, (args.hash,))
    row = cursor.fetchone()
    if not row:
        raise Exception('hash "%s" not found in database' % args.hash)
    sample_id = row[0]
    cursor.execute('SELECT id FROM sample_source WHERE (identifier = %s)', (args.source,))
    row = cursor.fetchone()
    if not row:
        raise Exception('source "%s" not found in database' % args.source)
    source_id = row[0]

    cursor.execute('INSERT INTO sample_has_source (sample_id, source_id) VALUES(%s, %s)', (sample_id, source_id))
connection.commit()
