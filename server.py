from flask import Flask, jsonify, request, g
from lib.sample import SampleFactory
from lib.task import TaskFactory
from lib.flask import InvalidUsage
from lib.general import KurasutaDatabase
import string
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


@app.route('/task', methods=['POST'])
def get_task():
    supported = ('PEMetadata', 'R2Disassembly')

    json_data = request.get_json()
    if json_data is None:
        raise InvalidUsage('JSON data could not be decoded (None)', status_code=400)

    connection = get_db()
    task_factory = TaskFactory(connection)
    task_request = task_factory.request_from_json(json_data)
    if set(task_request.plugins) - set(supported):
        raise InvalidUsage('Invalid plugin array')

    task = task_factory.random_unassigned(task_request)
    connection.commit()
    return jsonify(task.to_json() if task else {})


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


@app.route('/sha256/<sha256>', methods=['GET'])
def get_sha256(sha256):
    if len(sha256) != 64:
        raise InvalidUsage('SHA256 hash needs to be of length 64', status_code=400)
    if not all(c in string.hexdigits for c in sha256):
        raise InvalidUsage('SHA256 hash may only contain hex chars', status_code=400)

    return jsonify({'TODO': 1})


@app.route('/sha256/<sha256>', methods=['POST'])
def persist(sha256):
    if len(sha256) != 64:
        raise InvalidUsage('SHA256 hash needs to be of length 64', status_code=400)
    if not all(c in string.hexdigits for c in sha256):
        raise InvalidUsage('SHA256 hash may only contain hex chars', status_code=400)

    json_data = request.get_json()
    if json_data is None:
        raise InvalidUsage('JSON data could not be decoded (None)', status_code=400)

    connection = get_db()
    kurasuta_database = KurasutaDatabase(connection)
    task = TaskFactory(connection).mark_as_completed(int(json_data['task_id'])) if 'task_id' in json_data else None
    # TODO check if task.consumer_name matches client

    sample = SampleFactory().from_json(json_data)
    if sha256 != sample.hash_sha256:
        raise InvalidUsage('SHA256 in URL and body missmatch', status_code=400)

    with connection.cursor() as cursor:
        cursor.execute('''SELECT id FROM sample WHERE (hash_sha256 = %s)''', (sample.hash_sha256,))
        if cursor.fetchone():
            if not task:
                return jsonify({'status': 'EXISTS'})
            if task.type == 'PEMetadata':  # in case of new PE metadata, delete existing database entry
                kurasuta_database.delete_sample(sample.hash_sha256)

        if task.type == 'PEMetadata':
            store_metadata(cursor, kurasuta_database, sample)
        elif task.type == 'R2Disassembly':
            store_assembly(cursor, kurasuta_database, sample)

    connection.commit()

    return jsonify({'status': 'ok'})


def store_assembly(cursor, kurasuta_database, sample):
    pass


def store_metadata(cursor, kurasuta_database, sample):
    magic_id = kurasuta_database.ensure_row('magic', 'description', sample.magic) if sample.magic else None
    export_name_id = kurasuta_database.ensure_row('export_name', 'content', sample.export_name) \
        if sample.export_name else None

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

    if sample.peyd:
        for peyd_description in sample.peyd:
            peyd_id = kurasuta_database.ensure_row('peyd', 'description', peyd_description)
            cursor.execute('INSERT INTO sample_has_peyd (sample_id, peyd_id) VALUES(%s, %s)', (sample_id, peyd_id))

    if sample.debug_directories:
        for debug_directory in sample.debug_directories:
            path_id = kurasuta_database.ensure_row('path', 'content', debug_directory.path)
            cursor.execute('''
                INSERT INTO debug_directory
                    (sample_id, timestamp, path_id, age, signature, guid)
                    VALUES(%s, %s, %s, %s, %s, %s)
            ''', (
                sample_id,
                debug_directory.timestamp,
                path_id,
                debug_directory.age,
                debug_directory.signature,
                debug_directory.guid
            ))

    if sample.exports:
        for export in sample.exports:
            name_id = kurasuta_database.ensure_row('export_symbol_name', 'content', export.name)
            cursor.execute('''
                INSERT INTO export_symbol (sample_id, address, ordinal, name_id) VALUES(%s, %s, %s, %s)
            ''', (sample_id, export.address, export.ordinal, name_id))

    if sample.imports:
        for imp in sample.imports:
            import_name_id = kurasuta_database.ensure_row('import_name', 'content', imp.name)
            dll_name_id = kurasuta_database.ensure_row('dll_name', 'content', imp.dll_name)
            cursor.execute('''
                INSERT INTO import 
                (sample_id, dll_name_id, address, name_id) 
                VALUES(%s, %s, %s, %s) 
                RETURNING id
            ''', (sample_id, dll_name_id, imp.address, import_name_id))

    if sample.heuristic_iocs:
        for ioc in sample.heuristic_iocs:
            ioc_id = kurasuta_database.ensure_row('ioc', 'content', ioc)
            cursor.execute(
                'INSERT INTO sample_has_heuristic_ioc (sample_id, ioc_id) VALUES(%s, %s)',
                (sample_id, ioc_id)
            )

    if sample.sections:
        for i, section in enumerate(sample.sections):
            section_name_id = kurasuta_database.ensure_row('section_name', 'content', section.name)
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
            type_pair_id = kurasuta_database.ensure_resource_pair('type', resource.type_id, resource.type_str) \
                if resource.type_id and resource.type_str \
                else None

            name_pair_id = kurasuta_database.ensure_resource_pair('name', resource.type_id, resource.type_str) \
                if resource.type_id and resource.type_str \
                else None

            language_pair_id = kurasuta_database.ensure_resource_pair(
                'language', resource.type_id, resource.type_str
            ) if resource.type_id and resource.type_str else None

            cursor.execute('''
                INSERT INTO resource (
                    sample_id, 
                    hash_sha256,
                    "offset",
                    "size",
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
