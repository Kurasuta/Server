from flask import Flask, jsonify, request, abort, g
from lib.data import SampleFactory
import os
import logging
import psycopg2

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('KurasutaBackendApi')
debugging_enabled = 'FLASK_DEBUG' in os.environ
logger.setLevel(logging.DEBUG if debugging_enabled else logging.WARNING)

app = Flask(__name__)
app.config.from_object(__name__)  # load config from this file , flaskr.py

# Load default config and override config from an environment variable
app.config.update(dict(
    DATABASE=os.environ['POSTGRES_DATABASE_LINK'],
    SECRET_KEY=os.environ['FLASK_SECRET_KEY']
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)


def connect_db():
    db = psycopg2.connect(app.config['DATABASE'])
    return db


def get_db():
    if not hasattr(g, 'db'):
        g.db = connect_db()
    return g.db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()


tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]
supported = ["test"]


@app.route('/', methods=['POST'])
def get_task():
    selected_plugin = ""
    if not request.json or not 'plugins' in request.json:
        abort(400)
    if len(request.json.get("plugins")) == 0:
        abort(400)
    for plugin in request.json.get("plugins"):
        if plugin in supported:
            selected_plugin = plugin
            break
    else:
        abort(400)
    return jsonify({'tasks': tasks})


def ensure_row(conn, table, field, value):
    select_sql = 'SELECT id FROM %s WHERE %s = %%s' % (table, field)
    insert_sql = 'INSERT INTO %s (%s) VALUES(%%s) RETURNING id' % (table, field)
    with conn.cursor() as cursor:
        cursor.execute(select_sql, (value,))
        result = cursor.fetchone()
    if not result:
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, (value,))
            result = cursor.fetchone()
    return result[0]


def ensure_resource_pair(conn, pair_name, content_id, content_str):
    select_sql = 'SELECT id FROM resource_%s_pair WHERE (content_id = %%s) AND (content_str = %%s)' % (pair_name,)
    insert_sql = 'INSERT INTO resource_%s_pair (content_id, content_str) VALUES(%%s) RETURNING id' % (pair_name,)
    with conn.cursor() as cursor:
        cursor.execute(select_sql, (content_id, content_str))
        result = cursor.fetchone()
    if not result:
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, (content_id, content_str))
            result = cursor.fetchone()
    return result[0]


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route('/count', methods=['GET'])
def count():
    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(id) FROM sample')
        return jsonify({'count': cursor.fetchone()[0]})


@app.route('/persist', methods=['POST'])
def persist():
    f = SampleFactory()
    json_data = request.get_json()
    if json_data is None:
        raise InvalidUsage('JSON data could not be decoded (None)', status_code=400)

    sample = f.from_json(json_data)
    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute('''SELECT id FROM sample WHERE (hash_sha256 = %s)''', (sample.hash_sha256,))
        if cursor.fetchone():
            return jsonify({'status': 'EXISTS'})

    magic_id = ensure_row(conn, 'magic', 'description', sample.magic) if sample.magic else None
    export_name_id = ensure_row(conn, 'export_name', 'content', sample.export_name) if sample.export_name else None

    with conn.cursor() as cursor:
        if sample.code_histogram:
            cursor.execute('INSERT INTO byte_histogram (%s) VALUES(%s) RETURNING id' % (
                ', '.join(['byte_%02x' % i for i in range(256)]),
                ', '.join(['%s'] * 256)
            ), [sample.code_histogram['%s' % i] for i in range(256)])
            code_histogram_id = cursor.fetchone()[0]
        else:
            code_histogram_id = None

        cursor.execute(
            '''
            INSERT INTO sample (
                hash_sha256,
                hash_md5,
                hash_sha1,
                size,
                code_histogram_id,
                magic_id,
                ssdeep,
                imphash,
                entropy,
                file_size,
                entry_point,
                first_kb,
                overlay_sha256,
                overlay_size,
                overlay_ssdeep,
                overlay_entropy,
                build_timestamp,
                strings_count_of_length_at_least_10,
                strings_count,
                export_name_id
            ) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            RETURNING id
            ''', (
                sample.hash_sha256,
                sample.hash_md5,
                sample.hash_sha1,
                sample.size,
                code_histogram_id,
                magic_id,
                sample.ssdeep,
                sample.imphash,
                sample.entropy,
                sample.file_size,
                sample.entry_point,
                bytearray(sample.first_kb),
                sample.overlay_sha256,
                sample.overlay_size,
                sample.overlay_ssdeep,
                sample.overlay_entropy,
                sample.build_timestamp,
                sample.strings_count_of_length_at_least_10,
                sample.strings_count,
                export_name_id
            )
        )
        sample_id = cursor.fetchone()[0]

        for peyd_description in sample.peyd:
            peyd_id = ensure_row(conn, 'peyd', 'description', peyd_description)
            cursor.execute('INSERT INTO sample_has_peyd (sample_id, peyd_id) VALUES(%s, %s)', (sample_id, peyd_id))

        if sample.debug_directories:
            for debug_directory in sample.debug_directories:
                path_id = ensure_row(conn, 'path', 'content', debug_directory.path)
                cursor.execute('''
                    INSERT INTO debug_directory
                        (sample_id, timestamp, path_id, age, signature, guid)
                        VALUES(%s, %s, %s, %s, %s, %s)
                ''', (
                    sample_id,
                    debug_directory.timestmap,
                    path_id,
                    debug_directory.age,
                    debug_directory.signature,
                    debug_directory.guid
                ))

        if sample.exports:
            for export in sample.exports:
                name_id = ensure_row(conn, 'export_symbol_name', 'content', export.name)
                cursor.execute('''
                    INSERT INTO export_symbol (sample_id, address, ordinal, name_id) VALUES(%s, %s, %s, %s)
                ''', (sample_id, export.address, export.ordinal, name_id))

        if sample.imports:
            for imp in sample.imports:
                import_name_id = ensure_row(conn, 'import_name', 'content', imp.name)
                dll_name_id = ensure_row(conn, 'dll_name', 'content', imp.dll_name)
                cursor.execute('''
                    INSERT INTO import 
                    (sample_id, dll_name_id, address, name_id) 
                    VALUES(%s, %s, %s, %s) 
                    RETURNING id
                ''', (sample_id, dll_name_id, imp.address, import_name_id))

        if sample.heuristic_iocs:
            for ioc in sample.heuristic_iocs:
                ioc_id = ensure_row(conn, 'ioc', 'content', ioc)
                cursor('INSERT INTO sample_has_heuristic_ioc (sample_id, ioc_id) VALUES(%s, %s)', (sample_id, ioc_id))

        if sample.sections:
            for i, section in enumerate(sample.sections):
                section_name_id = ensure_row(conn, 'section_name', 'content', section.name)
                cursor.execute('''
                    INSERT INTO section (
                        sample_id, 
                        hash_sha256, 
                        name_id, 
                        virtual_address, 
                        virtual_size, 
                        raw_size, 
                        entropy, 
                        ssdeep, 
                        sort_order
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    sample_id,
                    section.hash_sha256,
                    section_name_id,
                    section.virtual_address,
                    section.virtual_size,
                    section.raw_size,
                    section.entropy,
                    section.ssdeep,
                    i
                ))
        if sample.resources:
            for i, resource in enumerate(sample.resources):
                type_pair_id = ensure_resource_pair(conn, 'type', resource.type_id, resource.type_str) \
                    if resource.type_id and resource.type_str \
                    else None

                name_pair_id = ensure_resource_pair(conn, 'name', resource.type_id, resource.type_str) \
                    if resource.type_id and resource.type_str \
                    else None

                language_pair_id = ensure_resource_pair(conn, 'language', resource.type_id, resource.type_str) \
                    if resource.type_id and resource.type_str \
                    else None

                cursor.execute('''
                    INSERT INTO resource (
                        sample_id, 
                        hash_sha256,
                        offset,
                        size,
                        actual_size,
                        entropy,
                        ssdeep,
                        type_pair_id,
                        name_pair_id,
                        language_pair_id,
                        sort_order
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)                     
                ''', (
                    sample_id,
                    resource.hash_sha256,
                    resource.offset,
                    resource.size,
                    resource.actual_size,
                    resource.entropy,
                    resource.ssdeep,
                    type_pair_id,
                    name_pair_id,
                    language_pair_id,
                    i
                ))
    conn.commit()

    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    if 'RAVEN_CLIENT_STRING' in os.environ:
        from raven.contrib.flask import Sentry

        sentry = Sentry(app, dsn=os.environ['RAVEN_CLIENT_STRING'])
    else:
        logger.warning('Environment variable RAVEN_CLIENT_STRING does not exist. No logging to Sentry is performed.')
    app.run(
        port=int(os.environ['FLASK_PORT']) if 'FLASK_PORT' in os.environ else None,
        debug=debugging_enabled
    )
