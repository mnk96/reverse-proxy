import asyncio
import json
import time
from asyncio.streams import StreamReader, StreamWriter

from config import settings
from logger import logger

metrics_list = {}


def backend_metrics_request(backend: str) -> None:
    if backend not in metrics_list:
        metrics_list[backend] = {
            'request': 1,
            'all_time': 0,
            'rps': 0,
        }
    else:
        metrics_list[backend]['request'] += 1
        if metrics_list[backend]['all_time'] == 0:
            metrics_list[backend]['rps'] = 0
        else:
            metrics_list[backend]['rps'] = (metrics_list[backend]['request'] /
                                            metrics_list[backend]['all_time'])


def backend_metrics_time(backend: str, time: time) -> None:
    metrics_list.setdefault(backend, {'request': 0, 'all_time': 0, 'rps': 0})
    if backend not in metrics_list:
        metrics_list[backend]['all_time'] = {
            'request': 1,
            'all_time': time,
            'rps': 1 / time
        }
    else:
        metrics_list[backend]['all_time'] += time
        metrics_list[backend]['rps'] = (metrics_list[backend]['request'] /
                                        metrics_list[backend]['all_time'])


async def handle_metrics(reader: StreamReader, writer: StreamWriter) -> None:
    """Обработчик запросов к получению метрик"""
    address = writer.get_extra_info('peername')
    logger.info('Запрос метрик от %s', address)
    try:
        data = await asyncio.wait_for(reader.read(settings.chunk_size), timeout=100)
        if not data:
            return
        metrics_json = json.dumps(metrics_list[address[0]])
        writer.write(metrics_json.encode('utf-8'))
        logger.info('Данные метрик Для %s: %s', address[0], metrics_json)
        await writer.drain()
    except Exception as e:  # noqa: BLE001
        logger.info('Ошибка получения метрик %s', e)
    finally:
        writer.close()
        await writer.wait_closed()


async def metrics_server(host: str, port: int) -> None:
    server = await asyncio.start_server(handle_metrics, host, port)
    logger.info('Сервер метрик запущен')

    async with server:
        await server.serve_forever()
