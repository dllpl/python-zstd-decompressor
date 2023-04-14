"""
Обработчик файлов формата zstd
Для распаковки используется библиотека zstandard
> pip install zstandard

Скрипт берет путь к файлу архива,
разбивает весь файл на куски по 16 МБ,
извлекает объекты построчно (каждая строка содержит один отель в формате JSON) в асинхронном режиме,
и преобразует их в словари Python, которые вы можете использовать в своей внутренней логике.

Обработка происходит в асинхронном режиме.
Основное различие между асинхронным и синхронным режимами заключается во времени обработки:
async быстрее, так как каждый фрагмент будет обрабатываться асинхронно.
"""

import asyncio
from zstandard import ZstdDecompressor
import json
from asyncio import Semaphore
from mysql.connector import connect, Error
import time
from slugify import slugify


class Decoder:
    def __init__(self, semaphore_value: int) -> None:
        self.sem = Semaphore(semaphore_value)
        self._raw = []
        self.connection = connect(
            host='localhost',
            user='root',
            password='root',
            database='loco',
        )
        print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' + 'Старт')

        self.delete_table_data()

    def delete_table_data(self) -> None:
        query = """DELETE FROM bravo_hotels WHERE is_ostrovok = 1"""
        self.connection.cursor().execute(query)
        self.connection.commit()
        query = """DELETE FROM bravo_spaces WHERE is_ostrovok = 1"""
        self.connection.cursor().execute(query)
        self.connection.commit()
        print('[' + time.strftime('%Y-%m-%d %H:%M:%S') + ']: ' +
              'Удалили старые объекты, запуск процесса добавления новых')

    def handler_request_to_db(self, data) -> None:
        query_hotel = ""
        query_space = ""
        if (data['kind'] != 'Apartment'):
            query_hotel = """
                    INSERT INTO bravo_hotels
                    (title, slug, content, image_id, banner_image_id, location_id,
                    address,map_lat,map_lng,map_zoom,is_featured,gallery,video,policy,star_rate,
                    price, check_in_time, check_out_time, allow_full_day, sale_price, status, create_user,
                    update_user, deleted_at, created_at, updated_at, review_score, ical_import_url, enable_extra_price,
                    extra_price, min_day_before_booking, min_day_stays, enable_service_fee, service_fee, surrounding, remark, is_ostrovok,serp_filters
                    )
                    VALUES ( %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
            data_hotel = [(
                data.get('name', None),  # title
                data.get('id', None),  # slug
                self._description_struct_handler(
                    data['description_struct']),  # content
                self.check_exist(0, data['images']),  # image_id
                self.check_exist(1, data['images']),  # banner_image_id
                data['region']['id'],  # location_id
                data['address'],  # address
                data['latitude'],  # map_lat
                data['longitude'],  # map_lng
                12,  # map_zoom
                None,  # is_featured
                self.check_if_not_empty(data['images']),  # gallery
                None,  # video
                None,  # policy
                data['star_rating'],  # star_rate
                None,  # price
                data['check_in_time'],  # check_in_time
                data['check_out_time'],  # check_out_time
                None,  # allow_full_day
                None,  # sale_price
                'publish',  # status
                1,  # create_user
                1,  # update_user
                None,  # deleted_at
                time.strftime('%Y-%m-%d %H:%M:%S'),  # created_at
                time.strftime('%Y-%m-%d %H:%M:%S'),  # updated_at
                # review_score, ical_import_url, enable_extra_price, extra_price, min_day_before_booking, min_day_stays, enable_service_fee, service_fee, surrounding, remark,
                None, None, None, None, None, None, None, None, None, None,
                1,  # is_ostrovok
                json.dumps(self.check_exist('serp_filters', data))
            )]
            with self.connection.cursor() as cursor:
                cursor.executemany(query_hotel, data_hotel)
                self.connection.commit()
        else:
            query_space = """
                INSERT INTO bravo_spaces
                (
                    title,
                    slug,
                    content,
                    image_id,
                    banner_image_id,
                    location_id,
                    address,
                    map_lat,
                    map_lng,
                    map_zoom,
                    is_featured,
                    gallery,
                    video,
                    faqs,
                    price,
                    sale_price,
                    is_instant,
                    allow_children,
                    allow_infant,
                    max_guests,
                    bed,
                    bathroom,
                    square,
                    enable_extra_price,
                    extra_price,
                    discount_by_days,
                    status,
                    default_state,
                    create_user,
                    update_user,
                    deleted_at,
                    created_at,
                    updated_at,
                    review_score,
                    ical_import_url,
                    kv_api_import_id,
                    min_day_before_booking,
                    min_day_stays,
                    enable_service_fee,
                    service_fee,
                    surrounding,
                    check_in,
                    check_out,
                    remark,
                    `reads`,
                    vrefid,
                    is_ostrovok,
                    serp_filters
                )
                VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            data_space = [(
                data.get('name', None),  # title
                data.get('id', None),  # slug
                self._description_struct_handler(
                    data['description_struct']),  # content
                self.check_exist(0, data['images']),  # image_id
                self.check_exist(1, data['images']),  # banner_image_id
                data['region']['id'],  # location_id
                data['address'],  # address
                data['latitude'],  # map_lat
                data['longitude'],  # map_lng
                12,  # map_zoom
                None,  # is_featured
                self.check_if_not_empty(data['images']),  # gallery
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                'publish',  # status
                None,  # default_state
                1,  # create_user
                1,  # update_user
                None,  # deleted_at
                time.strftime('%Y-%m-%d %H:%M:%S'),  # created_at
                time.strftime('%Y-%m-%d %H:%M:%S'),  # updated_at
                # review_score, #surrounding
                None, None, None, None, None, None, None, None,
                data['check_in_time'],  # check_in_time
                data['check_out_time'],  # check_out_time
                None,
                1,
                1,
                1,  # is_ostrovok
                json.dumps(self.check_exist('serp_filters', data))
            )]
            with self.connection.cursor() as cursor:
                cursor.executemany(query_space, data_space)
                self.connection.commit()

        query_location = """
                INSERT IGNORE INTO bravo_locations
                    (id, name,content,slug,image_id,map_lat,map_lng,map_zoom,
                    status,_lft,_rgt,parent_id,create_user,update_user,deleted_at,
                    origin_id,lang,created_at,updated_at,banner_image_id,trip_ideas,is_ostrovok)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
        try:
            data_location = [(
                data['region']['id'],
                data['region']['name'],
                None,
                slugify(data['region']['name'].lower()),
                None,
                None,
                None,
                13,
                'publish', '0', '0', None, 1, None, None,
                None, None,
                time.strftime('%Y-%m-%d %H:%M:%S'),
                time.strftime('%Y-%m-%d %H:%M:%S'),
                None, None, 1,
            )]
            with self.connection.cursor() as cursor:
                cursor.executemany(query_location, data_location)
                self.connection.commit()
        except:
            return False

    async def _process_raw_hotels(self) -> None:
        """
         Обрабатываем необработанные строки из архива.
         Обычно это первая и последняя строки чанков.
        """
        raw_hotels = self._raw[1:]
        raw_hotels = [self._raw[0]] + [
            "".join(t) for t in zip(raw_hotels[::2], raw_hotels[1::2])
        ]
        await self._process_hotel(*raw_hotels)

    def _description_struct_handler(self, arr):
        str = None
        try:
            if (len(arr[0]['paragraphs'][0]) > 0):
                str = arr[0]['paragraphs'][0] + '<br><br>'
            if (len(arr[1]['paragraphs'][1]) > 0):
                str += arr[1]['paragraphs'][1] + '<br><br>'
            if (len(arr[2]['paragraphs'][2]) > 0):
                str += arr[2]['paragraphs'][2] + '<br><br>'
            return str
        except:
            return str

    def check_exist(self, key, my_list):
        try:
            return my_list[key]
        except:
            return None

    def check_if_not_empty(self, my_list):
        if not my_list:
            return None
        else:
            return ','.join(my_list)

    async def _process_hotel(self, *raw_hotels: str) -> None:
        for h in raw_hotels:
            hotel_data = json.loads(h)
            # Тут можно применить свой код, в моем случае это вставка в БД построчно

            if (self.check_exist(0, hotel_data['images']) == None or self.check_exist(1, hotel_data['images']) == None):
                continue

            self.handler_request_to_db(hotel_data)

    async def _process_chunk(self, chunk: bytes) -> None:
        raw_data = chunk.decode("utf-8", 'ignore')
        # все файлы JSON разделены новой строкой char "\n"
        # пытаемся читать по одной
        lines = raw_data.split("\n")
        for i, line in enumerate(lines[1:-1]):
            if i == 0:
                # помещаем плохую строку в необработанный список
                self._raw.append(lines[0])
            await self._process_hotel(line)

        # поместите плохую строку в необработанный список
        self._raw.append(lines[-1])
        # увеличить значение семафора
        self.sem.release()

    async def parse_dump(self, filename: str) -> None:
        """
        Пример функции, которая может разобрать большой дамп zstd.
         :param имя_файла: путь к архиву zstd
        """
        with open(filename, "rb") as fh:
            # сделать декопрессию
            dctx = ZstdDecompressor()
            with dctx.stream_reader(fh) as reader:
                while True:
                    # будем читать файл порциями по 16мб
                    chunk = reader.read(2 ** 24)
                    if not chunk:
                        await self._process_raw_hotels()
                        break
                    # уменьшить значение семафора
                    # мы не можем запускать одновременно все чанки
                    await self.sem.acquire()
                    # запуск
                    asyncio.create_task(self._process_chunk(chunk))
                print('[' + time.strftime('%Y-%m-%d %H:%M:%S') +
                      ']: ' + 'Стоп, объекты добавлены')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    d = Decoder(semaphore_value=10)
    loop.run_until_complete(d.parse_dump("dumps/partner_feed_ru.json.zst"))
