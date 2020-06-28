import subprocess
import re
import os
import csv
import platform
from dateutil.parser import parse as parse_date
import datetime


class ZabbixSenderException(Exception):
    pass


class ZabbixSender(object):
    def __init__(self, sender_path=None, config_path=None):
        self.r_processed = re.compile('processed: (\d+);')
        self.r_failed = re.compile('failed: (\d+);')
        self.r_total = re.compile('total: (\d+);')

        if sender_path is None:
            self.sender_path = 'C:\\zabbix_sender.exe' \
                if platform.system() == 'Windows' \
                else '/usr/bin/zabbix_sender'
        else:
            self.sender_path = sender_path

        if config_path is None:
            self.config_path = 'C:\\zabbix_agentd.conf' \
                if platform.system() == 'Windows' \
                else '/etc/zabbix/zabbix_agentd.conf'
        else:
            self.config_path = config_path

        self.last_command = None

    def _execute_sender(self, arguments):
        self.last_command = [self.sender_path, '-c', self.config_path] + arguments
        output, error = subprocess.Popen(
            self.last_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        ).communicate()
        if error: raise ZabbixSenderException(error)

        return output

    def _parse_output(self, output):
        processed_item_count = int(self.r_processed.search(str(output)).group(1))
        failed_item_count = int(self.r_failed.search(str(output)).group(1))
        total_item_count = int(self.r_total.search(str(output)).group(1))

        if failed_item_count:
            raise ZabbixSenderException('%i failed Items during %s' % (failed_item_count, self.last_command))
        if processed_item_count != total_item_count:
            raise ZabbixSenderException('Missmatching: %i != %i' % (processed_item_count, total_item_count))

    def send_item(self, name, value):
        self._parse_output(self._execute_sender(['-k', name, '-o', '%s' % value]))

        return '%s: %s' % (name, value)
