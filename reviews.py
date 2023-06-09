
import gzip
import json
from toolz.itertoolz import first
import time
from mysql.connector import connect, Error


connection = connect(
    host='localhost',
    user='root',
    password='root',
    database='loco',
)

query = """
        INSERT INTO bravo_review
        (object_id,object_slug,object_model,title,content,rate_number,author_ip,
        status,publish_date,create_user,update_user,deleted_at,	lang,created_at,
        updated_at,vendor_id,is_ostrovok, author)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
invalid_json_counter = 0

print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' + 'Старт')

with connection.cursor() as cursor:
    cursor.execute("""DELETE FROM bravo_review WHERE is_ostrovok = 1""")
    connection.commit()

print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' +
      'Удалили старые отзывы, запуск процесса добавления новых')


def setReviewScore(slug: str, review_score) -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
        UPDATE bravo_spaces
        SET review_score=%s
        WHERE slug=%s
        """, (int(review_score) / 2, slug))
        connection.commit()
        with connection.cursor() as cursor:
            cursor.execute("""
        UPDATE bravo_hotels
        SET review_score=%s
        WHERE slug=%s
        """, (int(review_score) / 2, slug))
        connection.commit()
        return True
    except:
        return False


def save_to_db(data: dict) -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, data)
            connection.commit()
        return True
    except:
        return False


def prepareData(slug: str, reviews: dict, rating) -> bool:

    for review in reviews:
        try:
            if (review['review_plus'] and len(review['review_plus']) and len(review['author'])):
                save_to_db([(
                    None,
                    slug,
                    None,
                    None,
                    review['review_plus'],
                    int(review['rating']) / 2,
                    None,
                    'approved',
                    time.strftime(review['created'] + ' %H:%M:%S'),
                    None,
                    None,
                    None,
                    None,
                    time.strftime(review['created'] + ' %H:%M:%S'),
                    time.strftime(review['created'] + ' %H:%M:%S'),
                    1,
                    1,
                    review['author']
                )])
            else:
                continue
        except:
            continue

    setReviewScore(slug, rating)
    return True


with gzip.open('dumps/feed_ru.json.gz', 'r') as fin:
    for line in fin:
        try:
            raw_data = line.decode("utf-8", 'ignore')
            line = '{' + raw_data[:-2] + '}'

            data = json.loads(line)

            slug = first(data)

            if (data[slug]):
                prepareData(slug, data[slug]['reviews'], data[slug]['rating'])
            else:
                continue
        except:
            invalid_json_counter += 1
            continue

print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' + 'Стоп, отзывы добавлены')
