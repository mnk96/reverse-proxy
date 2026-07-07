import asyncio
from asyncio.streams import StreamReader, StreamWriter
from request_parser import HttpRequestParser
from metrics import backend_metrics_request
from logger import logger
from proxy_server import proxy_server


parser = HttpRequestParser()

BACKENDS = [
    ('127.0.0.1', 9001),
    ('127.0.0.1', 9002)
]


backend_index = 0


def round_robin_balancer():
    global backend_index
    url = BACKENDS[backend_index]
    backend_index = (backend_index + 1) % len(BACKENDS)
    return url


async def client_connected(client_reader: StreamReader,
                           client_writer: StreamWriter):
    """Обработчик клиента"""
    address = client_writer.get_extra_info('peername')
    logger.info('Клиент подключен %s', address)
    try:
        logger.info("Подключение к апстриму")
        url = round_robin_balancer()
        stream_reader, stream_writer = await asyncio.open_connection(url[0],
                                                                     url[1])
        upstream_address = stream_writer.get_extra_info('peername')
        logger.info("Подключено к апстриму %s", upstream_address)
        backend_metrics_request(upstream_address[0])
        client_task = asyncio.create_task(proxy_server(client_reader,
                                                       stream_writer,
                                                       upstream_address[0],
                                                       'клиент'))
        upstrem_task = asyncio.create_task(proxy_server(stream_reader,
                                                        client_writer,
                                                        upstream_address[0],
                                                        'апстрим'))
        await asyncio.gather(upstrem_task, client_task)
    except Exception as e:
        logger.info('Ошибка %s', e)


async def main_server(host: str, port: int):
    srv = await asyncio.start_server(client_connected, host, port)
    logger.info('Сервер запущен')

    async with srv:
        await srv.serve_forever()
