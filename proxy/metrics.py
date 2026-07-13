import asyncio
from logger import logger
import json

metrics_list = {}


def backend_metrics_request(backend):
    if backend not in metrics_list.keys():
        metrics_list[backend] = {
            'request': 1,
            'all_time': 0,
            'rps': 0,
        }
    else:
        metrics_list[backend]['request'] += 1
        metrics_list[backend]['rps'] = (metrics_list[backend]['request'] /
                                        metrics_list[backend]['all_time'])


def backend_metrics_time(backend, time):
    if backend not in metrics_list.keys():
        metrics_list[backend]['all_time'] = {
            'request': 1,
            'all_time': time,
            'rps': 1 / time
        }
    else:
        metrics_list[backend]['all_time'] += time
        metrics_list[backend]['rps'] = (metrics_list[backend]['request'] /
                                        metrics_list[backend]['all_time'])


async def handle_metrics(reader, writer):
    """Обработчик запросов к получению метрик"""
    address = writer.get_extra_info('peername')
    logger.info('Запрос метрик от %s', address)
    try:
        data = await asyncio.wait_for(reader.read(1024), timeout=100)
        if not data:
            return
        metrics_json = json.dumps(metrics_list[address[0]])
        writer.write(metrics_json.encode('utf-8'))
        logger.info('Данные метрик Для %s: %s', address[0], metrics_json)
        await writer.drain()
    except Exception as e:
        logger.info('Ошибка получения метрик %s', e)
    finally:
        writer.close()
        await writer.wait_closed()


async def metrics_server(host: str, port: int):
    server = await asyncio.start_server(handle_metrics, host, port)
    logger.info('Сервер метрик запущен')

    async with server:
        await server.serve_forever()
