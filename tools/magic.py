import sys
import os
import hashlib
import subprocess

mapping = {
    'HTML': 'html',
    'ASCII': 'txt',
    'UTF-8 Unicode': 'txt',
    'XML': 'xml',
    'Composite': 'doc',
    'Microsoft Word': 'doc',
    'Microsoft Excel': 'doc',
    'Microsoft Powerpoint': 'doc',
    'PDF document': 'pdf',
    'PE32': 'pe',
    'gzip': 'packed',
    '7-zip': 'packed',
    'xar archive': 'packed',
    'GIF': 'image',
    'data': 'data',
    'Macromedia': 'flv',
    'RAR': 'packed',
    'Zip': 'packed',
    'ELF': 'elf',
}
for file_name in sys.argv[1:]:
    if os.path.isdir(file_name):
        continue
    out, err = subprocess.Popen(['file', file_name], stdout=subprocess.PIPE).communicate()
    _, magic = out.split(':')[:2]
    hash_sha256 = hashlib.sha256(open(file_name, 'r').read()).hexdigest()
    found = False
    for needle, directory_name in mapping.iteritems():
        if needle in magic:
            if not os.path.exists(directory_name):
                os.mkdir(directory_name)
            os.rename(file_name, os.path.join(directory_name, hash_sha256))
            found = True
            break
    if not found:
        print('%s: %s (%s)' % (file_name, magic.strip(), hash_sha256))
