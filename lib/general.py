import subprocess
import socket
import json
import os
import errno
from datetime import datetime
from lib.sample import SampleMeta


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


class KurasutaApi(object):
    def __init__(self, base_url):
        self.base_url = base_url

    def get_sha256_url(self, hash_sha256):
        return '%s/sha256/%s' % (self.base_url, hash_sha256)

    def get_task_url(self):
        return '%s/task' % self.base_url

    def get_user_agent(self):
        return 'Kurasuta Worker (%s-%s)' % (KurasutaSystem.get_host(), KurasutaSystem.git_revision())


class KurasutaSystem(object):
    def __init__(self, storage):
        if not storage:
            raise Exception('KURASUTA_STORAGE location "%s" missing' % storage)
        if not storage:
            raise Exception('KURASUTA_STORAGE location "%s" is not a directory' % storage)
        self.storage = storage

    @staticmethod
    def get_host():
        return socket.gethostname()

    @staticmethod
    def git_revision():
        return subprocess.check_output([
            'git', '-C', os.path.dirname(os.path.realpath(__file__)),
            'rev-parse', '--short', 'HEAD'
        ]).strip().decode('utf-8')

    def get_hash_dir(self, hash_sha256):
        return os.path.join(self.storage, hash_sha256[0], hash_sha256[1], hash_sha256[2])

    @staticmethod
    def mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise


class KurasutaDatabase(object):
    def __init__(self, connection):
        self.connection = connection

    def delete_sample(self, hash_sha256):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT id FROM sample WHERE (hash_sha256 = %s)', (hash_sha256,))
            sample_id = cursor.fetchone()[0]
            cursor.execute('DELETE FROM sample_has_peyd WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM sample_has_heuristic_ioc WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM debug_directory WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM section WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM resource WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM guid WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM export_symbol WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM import WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM sample_function WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM sample_has_file_name WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM sample_has_tag WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM sample_has_source WHERE (sample_id = %s)', (sample_id,))
            cursor.execute('DELETE FROM sample WHERE (id = %s)', (sample_id,))

    def ensure_row(self, table, field, value):
        select_sql = 'SELECT id FROM %s WHERE %s = %%s' % (table, field)
        insert_sql = 'INSERT INTO %s (%s) VALUES(%%s) RETURNING id' % (table, field)
        with self.connection.cursor() as cursor:
            cursor.execute(select_sql, (value,))
            result = cursor.fetchone()
        if not result:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, (value,))
                result = cursor.fetchone()
        return result[0]

    def ensure_resource_pair(self, pair_name, content_id, content_str):
        select_sql = 'SELECT id FROM resource_%s_pair WHERE (content_id = %%s) AND (content_str = %%s)' % (pair_name,)
        insert_sql = 'INSERT INTO resource_%s_pair (content_id, content_str) VALUES(%%s) RETURNING id' % (pair_name,)
        with self.connection.cursor() as cursor:
            cursor.execute(select_sql, (content_id, content_str))
            result = cursor.fetchone()
        if not result:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, (content_id, content_str))
                result = cursor.fetchone()
        return result[0]

    def create_task(self, task_type, hash_sha256, meta=None):
        """
        :type task_type: str
        :type hash_sha256: str
        :type meta: SampleMeta
        """
        from psycopg2.extras import Json

        payload = meta.to_dict() if meta else {}
        payload['hash_sha256'] = hash_sha256
        with self.connection.cursor() as cursor:
            cursor.execute('INSERT INTO task ("type", payload) VALUES(%s, %s)', (task_type, Json(payload)))


class SampleSourceRepository(object):
    def __init__(self, connection):
        with connection.cursor() as cursor:
            cursor.execute('SELECT identifier, id FROM sample_source')
            self.cache = dict(cursor.fetchall())

    def get_by_identifier(self, identifier):
        if identifier not in self.cache.keys():
            raise Exception('sample source identifier "%s" not found' % identifier)
        return self.cache[identifier]
