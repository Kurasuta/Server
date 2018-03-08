import sys
import os
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
    'MS Windows shortcut': 'lnk',
    'CDFV2 Microsoft Outlook Message': 'doc',
    'PHP script': 'code',
    'perl': 'code',
    'MS-DOS executable': 'msdox-exe',
    'ISO-8859 text': 'txt',
}
for file_name in sys.argv[1:]:
    if os.path.isdir(file_name):
        continue
    out, err = subprocess.Popen(['file', file_name], stdout=subprocess.PIPE).communicate()
    _, magic = out.decode('utf-8').split(':')[:2]
    found = False
    for needle, directory_name in mapping.items():
        if needle in magic:
            if not os.path.exists(directory_name):
                os.mkdir(directory_name)
            target = os.path.join(directory_name, os.path.basename(file_name))
            # print('INFO Moving %s -> %s' % (file_name, target))
            os.rename(file_name, target)
            found = True
            break
    if not found:
        print('WARNING %s: %s' % (file_name, magic.strip()))
