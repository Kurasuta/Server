#!/usr/bin/env python
import argparse
import psycopg2
import os
import logging
from wordcloud import WordCloud

logging.basicConfig()
logger = logging.getLogger('KurasutaWordcloudGenerator')
logger.setLevel(logging.DEBUG)

tasks = {
    'section-names': '''
        SELECT section_name.content FROM section
        LEFT JOIN section_name ON (section_name.id = section.name_id)
    ''',
    'resource-names': '''
        SELECT resource_name.content FROM section
        LEFT JOIN section_name ON (section_name.id = section.name_id)
    ''',
    # 'pdb-paths': '''
    #     SELECT path.content FROM debug_directory
    #     LEFT JOIN path ON (path.id = debug_directory.path_id)
    # ''',
}
parser = argparse.ArgumentParser()
parser.add_argument('image_directory')
args = parser.parse_args()

if not os.path.exists(args.image_directory):
    raise Exception('image directory "%s" does not exist')
if not os.path.isdir(args.image_directory):
    raise Exception('path "%s" is not a directory')

db = psycopg2.connect(os.environ['POSTGRES_DATABASE_LINK'])


def clean_word(word):
    word = word.replace('.', ' ').replace('\\', ' ')
    while ' ' in word:
        word.replace('  ', ' ')
    return word


for name, sql in tasks.items():
    logger.info('Selecting data for %s...' % name)
    with db.cursor() as cursor:
        cursor.execute(sql)
        logger.info('Found %i rows.' % cursor.rowcount)
        wordlist = []
        for row in cursor:
            if not row[0]: continue
            word = clean_word(row[0])
            if not word: continue
            wordlist += [w for w in word.strip().split(' ') if w]
        logger.info('Found %i unique words for %s.' % (len(wordlist), name))
        wordcloud = WordCloud(height=768, width=1024).generate(' '.join(list(set(wordlist))))
        file_name = os.path.join(args.image_directory, '%s.png' % name)
        wordcloud.to_image().save(file_name, format='png', optimize=True)
        logger.info('Wrote generated wordcloud to %s.' % file_name)
