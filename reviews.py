
import gzip
import json
from toolz.itertoolz import first
import time
from mysql.connector import connect, Error


connection = connect(
    host='localhost',
    user='root',
    password='',
    database='loco',
)

query = """
        INSERT INTO bravo_review
        (object_id,object_slug,object_model,title,content,rate_number,author_ip,
        status,publish_date,create_user,update_user,deleted_at,	lang,created_at,
        updated_at,vendor_id,is_ostrovok)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
invalid_json_counter = 0

print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' + 'Старт')

with connection.cursor() as cursor:
    cursor.execute("""DELETE FROM bravo_review WHERE is_ostrovok = 1""")
    connection.commit()

print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' +
      'Удалили старые отзывы, запуск процесса добавления новых')


def save_to_db(data: dict) -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, data)
            connection.commit()
        return True
    except:
        return False


def prepareData(slug: str, reviews: dict) -> bool:

    for review in reviews:
        try:
            save_to_db([(
                None,
                slug,
                None,
                review['review_plus'][:35] + '...',  # title
                review['review_plus'],
                int(review['rating']),
                None,
                'approved',
                time.strftime(review['created'] + ' %H:%M:%S'),
                1,
                1,
                None,
                None,
                time.strftime(review['created'] + ' %H:%M:%S'),
                time.strftime(review['created'] + ' %H:%M:%S'),
                1,
                1
            )])
        except:
            continue
    return True


with gzip.open('feed_ru.json.gz', 'r') as fin:
    for line in fin:
        try:
            raw_data = line.decode("utf-8", 'ignore')
            line = '{' + raw_data[:-2] + '}'

            data = json.loads(line)

            slug = first(data)

            if (data[slug]):
                prepareData(slug, data[slug]['reviews'])
            else:
                continue
        except:
            invalid_json_counter += 1
            continue

print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' + 'Стоп, отзывы добавлены')
