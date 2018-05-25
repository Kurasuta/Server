import argparse
from datetime import datetime, timedelta
from lib.zabbix import ZabbixSender

parser = argparse.ArgumentParser()
parser.add_argument('log_file')
parser.add_argument('--hour-offset', type=int, default=1)
args = parser.parse_args()

hour_matcher = (datetime.now() - timedelta(hours=args.hour_offset)).strftime('%d/%B/%Y:%H')

status_codes = {}
request_times = []
with open(args.log_file, 'r') as fp:
    while True:
        line = fp.readline().strip()
        if not line:
            break
        if hour_matcher not in line:
            continue

        s = line.split(' ')
        if len(s) != 17:
            continue

        status_code = int(s[8])
        if status_code not in status_codes:
            status_codes[status_code] = 0
        status_codes[status_code] += 1

        request_times.append(float(s[14]))

zs = ZabbixSender()
zs.send_item('kurasuta.backend_rest_api.max_response_time', int(max(request_times)))
zs.send_item('kurasuta.backend_rest_api.avg_response_time', int(sum(request_times) / float(len(request_times))))
known_status_codes = [200, 504]
other_status_codes = 0
for status_code in status_codes:
    if status_code in known_status_codes:
        zs.send_item('kurasuta.backend_rest_api.response_count_%i' % status_code, status_codes[status_code])
    else:
        other_status_codes += status_codes[status_code]
zs.send_item('kurasuta.backend_rest_api.response_count_other', other_status_codes)
