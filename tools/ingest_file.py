import sys
import shutil
import os
import hashlib

from lib.general import KurasutaSystem

if 'KURASUTA_STORAGE' not in os.environ:
    raise Exception('environment variable KURASUTA_STORAGE missing')

kurasuta_sys = KurasutaSystem(os.environ['KURASUTA_STORAGE'])


for file_name in sys.argv[1:]:
    with open(file_name, 'rb') as fp:
        content = fp.read()
        fp.close()
    sha256 = hashlib.sha256(content).hexdigest()

    target_folder = kurasuta_sys.get_hash_dir(sha256)
    kurasuta_sys.mkdir_p(target_folder)
    shutil.move(file_name, os.path.join(target_folder, sha256))
