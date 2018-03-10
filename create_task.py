import shutil
import hashlib
import os
import json
import psycopg2
import argparse
import sys

from lib.general import KurasutaSystem, KurasutaDatabase, SampleSourceRepository
from lib.sample import SampleMeta

parser = argparse.ArgumentParser()
parser.add_argument('type', action='store', choices=['PEMetadata', 'R2Disassembly'])
parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--existing_hash', help='existing hash to process', action='store_true')
group.add_argument('--skip_if_existing', help='skip if hash exists in storage', action='store_true')
group.add_argument('--file_name', help='file to process', action='store_true')
parser.add_argument('--source_identifier', help='identifier (name) of source')
parser.add_argument('--user_and_group', help='chown target file to this if specified')
args = parser.parse_args()

db = KurasutaDatabase(psycopg2.connect(os.environ['DATABASE']))
if args.existing_hash:
    for hash_sha256 in args.infile:
        hash_sha256 = hash_sha256.strip()
        if not hash_sha256:
            continue
        if len([True for c in hash_sha256 if c in '0123456789abcdef']) != len(hash_sha256):
            raise Exception('Argument "%s" is not a valid SHA256 hash')
        if len(hash_sha256) != 64:
            raise Exception('Argument "%s" is not of size 64')

        db.create_task(args.type, hash_sha256)

if args.file_name:
    if 'KURASUTA_STORAGE' not in os.environ:
        raise Exception('environment variable KURASUTA_STORAGE missing')

    kurasuta_sys = KurasutaSystem(os.environ['KURASUTA_STORAGE'])
    sample_source_repository = SampleSourceRepository(db.connection)

    for file_name in args.infile:
        # calculate hash
        file_name = file_name.strip()
        with open(file_name, 'rb') as fp:
            content = fp.read()
        hash_sha256 = hashlib.sha256(content).hexdigest()

        # calculate target file name
        target_folder = kurasuta_sys.get_hash_dir(hash_sha256)
        target_file_name = os.path.join(target_folder, hash_sha256)
        if args.skip_if_existing and os.path.exists(target_file_name):
            continue

        # read metadata, if it exists
        meta = SampleMeta()
        if os.path.exists(file_name + '.json'):
            with open(file_name + '.json', 'r') as fp:
                j = json.loads(fp.read())

            if 'tags' in j.keys():
                assert isinstance(j['tags'], list)
                meta.tags = j['tags']
            if 'file_names' in j.keys():
                assert isinstance(j['file_names'], list)
                meta.tags = j['file_names']

            if 'source_identifier' in j.keys():
                if args.source_identifier:
                    raise Exception('source_identifier set in meta json file and as argument')
                meta.source_id = sample_source_repository.get_by_identifier(j['source_identifier'])
        if args.source_identifier:
            meta.source_id = sample_source_repository.get_by_identifier(args.source_identifier)

        # create task and move file
        db.create_task(args.type, hash_sha256, meta)
        kurasuta_sys.mkdir_p(target_folder)
        if args.user_and_group:
            shutil.chown(file_name, args.user_and_group, args.user_and_group)
        shutil.move(file_name, target_file_name)
db.connection.commit()
