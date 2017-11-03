import os
import psycopg2

connection = psycopg2.connect(os.environ['DATABASE'])
with connection.cursor() as cursor:
    cursor.execute('SELECT hash_sha256 FROM sample ORDER BY hash_sha256')
    for row in cursor:
        print(row[0])
