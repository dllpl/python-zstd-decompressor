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


class Decoder:
    def __init__(self, semaphore_value: int) -> None:
        self.sem = Semaphore(semaphore_value)
        self._raw = []
        self.connection = connect(
            host='localhost',
            user='root',
            password='',
            database='loco',
        )

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

    async def _process_hotel(self, *raw_hotels: str) -> None:
        for h in raw_hotels:
            hotel_data = json.loads(h)
            # Тут можно применить свой код, в моем случае это вставка в БД построчно
            insert_reviewers_query = """
            INSERT INTO test
            (name, address)
            VALUES ( %s, %s)
            """
            reviewers_records = [
                (hotel_data['name'], hotel_data['address']),
            ]

            with self.connection.cursor() as cursor:
                cursor.executemany(insert_reviewers_query,
                                   reviewers_records)
                self.connection.commit()

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


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    d = Decoder(semaphore_value=10)
    loop.run_until_complete(d.parse_dump("partner_feed_ru.json.zst"))
