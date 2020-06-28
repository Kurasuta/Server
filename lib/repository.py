from .sample import SampleFactory, Sample
import random
from datetime import datetime


class PostgresRepository(object):
    def __init__(self, db):
        self.db = db

    def approx_count(self, table):
        with self.db.cursor() as cursor:
            cursor.execute('SELECT reltuples AS approximate_row_count FROM pg_class WHERE (relname = %s)', (table,))
            return int(cursor.fetchall()[0][0])


class SampleRepository(PostgresRepository):
    def __init__(self, db):
        super().__init__(db)
        self.factory = SampleFactory()

    def by_ids(self, ids):
        ids = tuple(ids)

        # read base samples
        samples = {}
        with self.db.cursor() as cursor:
            histogram_sql = ', '.join(['byte_histogram.byte_%02x' % i for i in range(256)])
            cursor.execute('''
                SELECT
                    sample.id,
                    sample.hash_sha256,
                    sample.hash_md5,
                    sample.hash_sha1,
                    sample.size,
                    sample.ssdeep,
                    sample.imphash,
                    sample.entropy,
                    sample.file_size,
                    sample.entry_point,
                    sample.overlay_sha256,
                    sample.overlay_size,
                    sample.overlay_ssdeep,
                    sample.overlay_entropy,
                    sample.build_timestamp,
                    sample.strings_count_of_length_at_least_10,
                    sample.strings_count,
                    sample.first_kb,
                    magic.description,
                    export_name.content,
                    %s
                FROM sample
                LEFT JOIN magic ON (sample.magic_id = magic.id)
                LEFT JOIN export_name ON (sample.magic_id = export_name.id)
                LEFT JOIN byte_histogram ON (byte_histogram.id = sample.code_histogram_id)
                WHERE (sample.id IN %%s)
            ''' % histogram_sql, (ids,))
            for row in cursor.fetchall():
                sample = self.factory.from_row(
                    row,
                    [
                        'id',
                        'hash_sha256',
                        'hash_md5',
                        'hash_sha1',
                        'size',
                        'ssdeep',
                        'imphash',
                        'entropy',
                        'file_size',
                        'entry_point',
                        'overlay_sha256',
                        'overlay_size',
                        'overlay_ssdeep',
                        'overlay_entropy',
                        'build_timestamp',
                        'strings_count_of_length_at_least_10',
                        'strings_count'
                    ]
                )
                sample.first_kb = row[17]
                sample.magic = row[18]
                sample.export_name = row[19]
                sample.code_histogram = dict((i, row[20 + i]) for i in range(256))
                samples[row[0]] = sample

            # read debug directories
            cursor.execute('''
                SELECT
                    debug_directory.timestamp,
                    path.content,
                    debug_directory.age,
                    debug_directory.signature,
                    debug_directory.guid,
                    debug_directory.sample_id
                FROM debug_directory
                LEFT JOIN path ON (path.id = debug_directory.path_id)
                WHERE (sample_id IN %s)
            ''', (ids,))
            for row in cursor.fetchall():
                if samples[row[5]].debug_directories is None:
                    samples[row[5]].debug_directories = []
                samples[row[5]].debug_directories.append(self.factory.create_debug_directory(*row[0:5]))

            # read export symbols
            cursor.execute('''
                SELECT
                    export_symbol.address,
                    export_symbol_name.content,
                    export_symbol.ordinal,
                    export_symbol.sample_id
                FROM export_symbol
                LEFT JOIN export_symbol_name ON (export_symbol.name_id = export_symbol_name.id)
                WHERE (sample_id IN %s)
            ''', (ids,))
            for row in cursor.fetchall():
                if samples[row[3]].exports is None:
                    samples[row[3]].exports = []
                samples[row[3]].exports.append(self.factory.create_export(*row[0:3]))

            # read imports
            cursor.execute('''
                SELECT
                    dll_name.content,
                    import.address,
                    import_name.content,
                    import.sample_id
                FROM import
                LEFT JOIN dll_name ON (dll_name.id = import.dll_name_id)
                LEFT JOIN import_name ON (import_name.id = import.name_id)
                WHERE (sample_id IN %s)
            ''', (ids,))
            for row in cursor.fetchall():
                if samples[row[3]].imports is None:
                    samples[row[3]].imports = []
                samples[row[3]].imports.append(self.factory.create_import(*row[0:3]))

            # read resources
            cursor.execute('''
                SELECT
                    resource.hash_sha256,
                    resource.offset,
                    resource.size,
                    resource.actual_size,
                    resource.entropy,
                    resource.ssdeep,
                    resource_type_pair.content_id,
                    resource_type_pair.content_str,
                    resource_name_pair.content_id,
                    resource_name_pair.content_str,
                    resource_language_pair.content_id,
                    resource_language_pair.content_str,
                    resource.sample_id
                FROM resource
                LEFT JOIN resource_type_pair ON (resource_type_pair.id = resource.type_pair_id)
                LEFT JOIN resource_name_pair ON (resource_name_pair.id = resource.name_pair_id)
                LEFT JOIN resource_language_pair ON (resource_language_pair.id = resource.language_pair_id)
                WHERE (resource.sample_id IN %s)
                ORDER BY resource.sort_order
            ''', (ids,))
            for row in cursor.fetchall():
                if samples[row[12]].resources is None:
                    samples[row[12]].resources = []
                samples[row[12]].resources.append(self.factory.create_resource(*row[0:12]))

            # read sections
            cursor.execute('''
                SELECT
                    section.hash_sha256,
                    section_name.content,
                    section.virtual_address,
                    section.virtual_size,
                    section.raw_size,
                    section.entropy,
                    section.ssdeep,
                    section.sample_id
                FROM section
                LEFT JOIN section_name ON (section_name.id = section.name_id)
                WHERE (section.sample_id IN %s)
            ''', (ids,))
            for row in cursor.fetchall():
                if samples[row[7]].sections is None:
                    samples[row[7]].sections = []
                samples[row[7]].sections.append(self.factory.create_section(*row[0:7]))

            # read heuristic IOCs
            cursor.execute('''
                SELECT
                    sample_has_heuristic_ioc.sample_id,
                    ioc.content
                FROM ioc
                LEFT JOIN sample_has_heuristic_ioc ON (sample_has_heuristic_ioc.ioc_id = ioc.id)
                WHERE (sample_has_heuristic_ioc.sample_id IN %s)
            ''', (ids,))
            for row in cursor.fetchall():
                if samples[row[0]].heuristic_iocs is None:
                    samples[row[0]].heuristic_iocs = []
                samples[row[0]].heuristic_iocs.append(row[1])

        return [sample for sample in samples.values()]

    def by_section_hash(self, sha256):
        with self.db.cursor() as cursor:
            cursor.execute('''
                SELECT sample.hash_sha256, sample.build_timestamp
                FROM section
                LEFT JOIN sample ON (sample.id = section.sample_id)
                WHERE (section.hash_sha256 = %s)
            ''', (sha256,))
            ret = []
            for row in cursor.fetchall():
                sample = Sample()
                sample.hash_sha256 = row[0]
                sample.build_timestamp = row[1]
                ret.append(sample)
            return ret

    def by_hash_sha256(self, sha256):
        return self.by_hash_type('sha256', sha256)

    def by_hash_md5(self, md5):
        return self.by_hash_type('md5', md5)

    def by_hash_sha1(self, sha1):
        return self.by_hash_type('sha1', sha1)

    def by_hash_type(self, hash_type, sha256):
        with self.db.cursor() as cursor:
            cursor.execute('''
                SELECT
                    sample.id,
                    sample.hash_sha256,
                    sample.hash_md5,
                    sample.hash_sha1,
                    sample.size,
                    sample.ssdeep,
                    sample.imphash,
                    sample.entropy,
                    sample.file_size,
                    sample.entry_point,
                    sample.overlay_sha256,
                    sample.overlay_size,
                    sample.overlay_ssdeep,
                    sample.overlay_entropy,
                    sample.build_timestamp,
                    sample.strings_count_of_length_at_least_10,
                    sample.strings_count
                FROM sample
                WHERE (sample.hash_%s = %%s)
            ''' % hash_type, (sha256,))

            # TODO join more tables and propagate to sample object

            sample = Sample()
            row = cursor.fetchone()
            if not row:
                return None
            sample_id, sample.hash_sha256, sample.hash_md5, sample.hash_sha1, sample.size, sample.ssdeep, sample.imphash, \
            sample.entropy, sample.file_size, sample.entry_point, sample.overlay_sha256, sample.overlay_size, \
            sample.overlay_ssdeep, sample.overlay_entropy, sample.build_timestamp, \
            sample.strings_count_of_length_at_least_10, sample.strings_count = row

            cursor.execute('''
                SELECT
                    s.hash_sha256,
                    sn.content AS name,
                    s.virtual_address,
                    s.virtual_size,
                    s.raw_size,
                    s.entropy,
                    s.ssdeep
                FROM section s
                LEFT JOIN section_name sn ON (s.name_id = sn.id)
                WHERE (s.sample_id = %s)
                ORDER BY s.sort_order
            ''', (sample_id,))
            for row in cursor.fetchall():
                sample.sections.append(self.factory.create_section(*row))

            cursor.execute('''
                SELECT
                    r.hash_sha256,
                    r.offset,
                    r.size,
                    r.actual_size,
                    r.ssdeep,
                    r.entropy,
                    tp.content_id AS type_id,
                    tp.content_str AS type_str,
                    np.content_id AS name_id,
                    np.content_str AS name_str,
                    lp.content_id AS language_id,
                    lp.content_str AS language_str
                FROM resource r
                LEFT JOIN resource_type_pair tp ON (r.type_pair_id = tp.id)
                LEFT JOIN resource_name_pair np ON (r.name_pair_id = tp.id)
                LEFT JOIN resource_language_pair lp ON (r.language_pair_id = tp.id)
                WHERE (r.sample_id = %s)
                ORDER BY r.sort_order
            ''', (sample_id,))
            for row in cursor.fetchall():
                sample.resources.append(self.factory.create_resource(*row))

            cursor.execute('''
                SELECT
                    d.timestamp,
                    p.content AS path,
                    d.age,
                    d.signature,
                    d.guid
                FROM debug_directory d
                LEFT JOIN path p ON (d.path_id = p.id)
                WHERE (d.sample_id = %s)
            ''', (sample_id,))
            for row in cursor.fetchall():
                sample.debug_directories.append(self.factory.create_debug_directory(*row))

            return sample

    def newest(self, count):
        with self.db.cursor() as cursor:
            cursor.execute('''
                SELECT s.hash_sha256, s.build_timestamp 
                FROM sample s
                ORDER BY s.id DESC 
                LIMIT %s
            ''', (count,))
            ret = []
            for row in cursor.fetchall():
                sample = Sample()
                sample.hash_sha256 = row[0]
                sample.build_timestamp = row[1]
                ret.append(sample)
            return ret

    def random_by_offset(self, output_count):
        random.seed(datetime.now())
        with self.db.cursor() as cursor:
            approximate_row_count = self.approx_count('sample')
            ret = []
            while len(ret) < output_count:
                rand = random.randint(0, approximate_row_count)
                cursor.execute('SELECT id, hash_sha256, build_timestamp FROM sample LIMIT 1 OFFSET %s', (rand,))
                ret += [
                    self.factory.from_row(row, ['id', 'hash_sha256', 'build_timestamp'])
                    for row in cursor.fetchall()
                ]

            return ret[:output_count]

    def random_by_id(self, output_count):
        random.seed(datetime.now())
        with self.db.cursor() as cursor:
            cursor.execute('SELECT MIN(id), MAX(id) FROM sample')
            min_id, max_id = cursor.fetchall()[0]
            random_ids = []
            while len(random_ids) < output_count:
                random_potential_ids = random.sample(range(min_id, max_id), output_count)
                cursor.execute('SELECT id FROM sample WHERE (id IN %s)', (tuple(random_potential_ids),))
                random_ids += [row[0] for row in cursor.fetchall()]
                random_ids = list(set(random_ids))

            cursor.execute(
                'SELECT id, hash_sha256, build_timestamp FROM sample WHERE (id IN %s)',
                (tuple(random_ids[:output_count]),)
            )

            return [
                self.factory.from_row(row, ['id', 'hash_sha256', 'build_timestamp'])
                for row in cursor.fetchall()
            ]

    def ids_by_hashes(self, hashes):
        sha256 = []
        md5 = []
        sha1 = []
        for line in hashes:
            hash = line.strip().decode('utf-8')
            if len(hash) == 64:
                sha256.append(hash)
            elif len(hash) == 32:
                md5.append(hash)
            elif len(hash) == 40:
                sha1.append(hash)
        where = []
        args = []
        if sha256:
            where.append('(hash_sha256 IN %s)')
            args.append(tuple(sha256))
        if md5:
            where.append('(hash_md5 IN %s)')
            args.append(tuple(md5))
        if sha1:
            where.append('(hash_sha1 IN %s)')
            args.append(tuple(sha1))

        with self.db.cursor() as cursor:
            cursor.execute('SELECT sample.id FROM sample WHERE (%s)' % (' OR '.join(where)), args)
            ids = [row[0] for row in cursor]
        return ids


class ApiKeyRepository(PostgresRepository):
    def exists(self, api_key):
        with self.db.cursor() as cursor:
            cursor.execute('SELECT id FROM api_key WHERE (content = %s)', (api_key,))
            return len(cursor.fetchall()) == 1
