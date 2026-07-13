import asyncio
import time
from asyncio.streams import StreamReader, StreamWriter
from asyncio import Semaphore
from request_parser import HttpRequestParser
from metrics import backend_metrics_request
from logger import logger
from proxy_server import proxy_server
from config import config


parser = HttpRequestParser()

SEMAPHORE = Semaphore(config['limits']['max_client_conns'])


BACKENDS = config['upstreams']
TIMEOUTS = config['timeouts']

backend_index = 0


class ConnectionPool:
    """Пул соединений"""
    def __init__(self, max_size, timeout, max_requests):
        self.max_size = max_size
        self.timeout = timeout
        self.max_requests = max_requests

        self.pool_list = {}
        self.active_list = {}

    async def start(self):
        """Создание начальных соединений"""
        for upstream in BACKENDS:
            host = upstream['host']
            port = upstream['port']
            key = f'{host}:{port}'
            for _ in range(2):
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port),
                        TIMEOUTS['connect_ms'])
                    self.pool_list[key] = {
                        'reader': reader,
                        'writer': writer,
                        'last_used': time.time(),
                        'created': time.time(),
                        'request_count': 0
                    }
                    logger.info('Создано начальное соединение %s', key)
                except Exception as e:
                    logger.info('Ошибка создания начального соединения для %s: %s', key, e)

    async def get_pool(self, host, port):
        """Получение соединения из пула или создание нового"""
        key = f'{host}:{port}'
        if key not in self.pool_list:
            logger.info('Неизвестное соединение %s', key)
            raise
        pool = self.pool_list[key]
        # Если есть существующее соединение
        while pool:
            writer = pool['writer']
            reader = pool['reader']
            pool['last_used'] = time.time()
            pool['request_count'] += 1
            self.active_list[writer] = pool
            logger.info('Переиспользовано соединение %s', key)
            return writer, reader
        # Создаем новое соединение
        try:
            reader, writer = await asyncio.open_connection(host, port)
            self.pool_list[key] = {
                'reader': reader,
                'writer': writer,
                'last_used': time.time(),
                'created': time.time(),
                'request_count': 0
            }
            logger.info('Создано новое соединение %s', key)
            return writer, reader
        except Exception as e:
            logger.info('Ошибка создания соединения %s', e)
            raise


def round_robin_balancer():
    global backend_index
    url = BACKENDS[backend_index]
    backend_index = (backend_index + 1) % len(BACKENDS)
    return url


connection_pool = None


async def init_pool(max_size, timeout, max_request):
    global connection_pool
    connection_pool = ConnectionPool(max_size, timeout, max_request)
    await connection_pool.start()
    return connection_pool


async def client_connected(client_reader: StreamReader,
                           client_writer: StreamWriter):
    """Обработчик клиента"""
    async with SEMAPHORE:
        logger.info(f"Семафор клиента захвачен (свободно: {SEMAPHORE._value})")
        address = client_writer.get_extra_info('peername')
        logger.info('Клиент подключен %s', address)
        try:
            async with asyncio.timeout(TIMEOUTS['total_ms']):
                logger.info("Подключение к апстриму")
                url = round_robin_balancer()
                stream_writer, stream_reader = await connection_pool.get_pool(
                    url[0], url[1]
                )
                upstream_address = stream_writer.get_extra_info('peername')
                logger.info("Подключено к апстриму %s", upstream_address)
                address = upstream_address[0]
                backend_metrics_request(address)
                client_task = asyncio.create_task(proxy_server(client_reader,
                                                               stream_writer,
                                                               address,
                                                               'клиент'))
                upstrem_task = asyncio.create_task(proxy_server(stream_reader,
                                                                client_writer,
                                                                address,
                                                                'апстрим'))
                await asyncio.gather(upstrem_task, client_task)
        except asyncio.TimeoutError:
            logger.info('Запрос превысил лимит времени')
        except Exception as e:
            logger.info('Ошибка %s', e)
    logger.info(f"Семафор клиента освобожден (свободно: {SEMAPHORE._value})")


async def main_server(host: str, port: int):
    await init_pool(10, 60, 100)
    srv = await asyncio.start_server(client_connected, host, port)
    logger.info('Сервер запущен')

    async with srv:
        await srv.serve_forever()
