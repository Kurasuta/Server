import os
import psycopg2
from psycopg2.extras import Json
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument('type', action='store', choices=['PEMetadata', 'R2Disassembly'])
parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--existing_hash', help='existing hash to process', action='store_true')
group.add_argument('--file_name', help='file to process', action='store_true')
args = parser.parse_args()

if args.existing_hash:
    connection = psycopg2.connect(os.environ['DATABASE'])
    for hash_sha256 in args.infile:
        hash_sha256 = hash_sha256.strip()
        if not hash_sha256:
            continue
        if len([True for c in hash_sha256 if c in '0123456789abcdef']) != len(hash_sha256):
            raise Exception('Argument "%s" is not a valid SHA256 hash')
        if len(hash_sha256) != 64:
            raise Exception('Argument "%s" is not of size 64')
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO task ("type", payload) VALUES(%s, %s)', (
                args.type,
                Json({'hash_sha256': hash_sha256})
            ))
    connection.commit()
if args.file_name:
    raise NotImplemented()
