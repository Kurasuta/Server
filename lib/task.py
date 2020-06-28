import random
from .flask import InvalidUsage
from .sample import FrozenClass


class TaskRequest(FrozenClass):
    def __init__(self, task_consumer_id, task_consumer_name, plugins):
        self.task_consumer_id = task_consumer_id  # type: int
        self.task_consumer_name = task_consumer_name  # type: str
        self.plugins = tuple(plugins) if isinstance(plugins, list) else plugins  # type: tuple(str)
        self._freeze()


class TaskResponse(FrozenClass):
    def __init__(self, id, type, payload):
        self.id = id  # type: int
        self.type = type  # type: str
        self.payload = payload  # type: dict
        self._freeze()

    def to_json(self):
        return {'id': self.id, 'type': self.type, 'payload': self.payload}


class Task(FrozenClass):
    def __init__(self, task_id, task_type, payload, created_at, assigned_at, consumer_name):
        self.id = task_id  # type: int
        self.type = task_type  # type: str
        self.payload = payload  # type: str
        self.created_at = created_at  # type: datetime
        self.assigned_at = assigned_at  # type: datetime
        self.consumer_name = consumer_name  # type: str

        self._freeze()


class TaskFactory(object):
    def __init__(self, connection=None):
        self.connection = connection

    @staticmethod
    def get_types_sorted_by_priority():
        return ['PEMetadata', 'R2Disassembly']

    def request_from_json(self, d):
        if 'name' not in d:
            raise InvalidUsage('Key "name" missing in request.')
        if 'plugins' not in d:
            raise InvalidUsage('Key "plugins" missing in request.')

        task_consumer_id = 0
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT id FROM task_consumer WHERE (name = %s)', (d['name'],))
            row = cursor.fetchone()
            if not row:
                raise InvalidUsage('Consumer with name "%s" does not exist' % d['name'])
            task_consumer_id = int(row[0])
        return TaskRequest(task_consumer_id, d['name'], d['plugins'])

    @staticmethod
    def response_from_json(d):
        if 'id' not in d:
            raise Exception('Keu "id" missing in response.')
        if 'payload' not in d:
            raise Exception('Keu "payload" missing in response.')

        return TaskResponse(d['id'], d['type'], d['payload'])

    def random_unassigned(self, plugin, consumer_id):
        """
        :type consumer_id: int
        :type plugin: str
        :return: TaskResponse
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT COUNT(id) FROM task WHERE (assigned_at IS NULL) AND (type = %s)',
                (plugin,)
            )
            count = cursor.fetchone()[0]
            if not count:
                return None
            offset = random.randint(0, count - 1)
            cursor.execute(
                'SELECT id, "type", payload FROM task WHERE (assigned_at IS NULL) AND (type = %s) LIMIT 1 OFFSET %s',
                (plugin, offset)
            )
            task_row = cursor.fetchone()

            if not task_row:
                return None
            task = TaskResponse(task_row[0], task_row[1], task_row[2])
            cursor.execute(
                'UPDATE task SET assigned_at = now(), consumer_id = %s WHERE (id = %s)',
                (consumer_id, task.id)
            )

            return task

    @staticmethod
    def by_row(row):
        return Task(row[0], row[1], row[2], row[3], row[4], row[5])

    def by_id(self, task_id):
        with self.connection.cursor() as cursor:
            cursor.execute(
                '''
                SELECT
                    t.id, t.type, t.payload, t.created_at, t.assigned_at,
                    tc.name AS consumer_name
                FROM task t
                LEFT JOIN task_consumer tc ON (t.consumer_id = tc.id)
                WHERE (t.id = %s)
                ''',
                (task_id,)
            )
            return self.by_row(cursor.fetchone())

    def mark_as_completed(self, task_id):
        with self.connection.cursor() as cursor:
            task = self.by_id(task_id)
            if not task:
                raise InvalidUsage('Task with id %s does not exist' % task_id)

            cursor.execute('UPDATE task SET completed_at = now() WHERE (id = %s)', (task.id,))
            return task
