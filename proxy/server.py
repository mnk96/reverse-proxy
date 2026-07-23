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


UPSTREAMS = config['upstreams']
TIMEOUTS = config['timeouts']
parser = HttpRequestParser()

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
        for upstream in UPSTREAMS:
            host = upstream['host']
            port = upstream['port']
            key = f'{host}:{port}'
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
            if self.check_connection_alive(writer):
                pool['last_used'] = time.time()
                pool['request_count'] += 1
                self.active_list[writer] = pool
                logger.info('Переиспользовано соединение %s', key)
                if pool['request_count'] >= config['limits']['max_requests_per_conns']:
                    logger.info('Соединение %s достигло лимита запросов', key)
                    writer.close()
                    await writer.wait_closed()
                    logger.info('Соединение закрыто %s', key)

                return writer, reader
            else:
                logger.info('Найдено мертвое соединение %s', key)
                try:
                    writer.close()
                    writer.wait_closed()
                except Exception as e:
                    logger.info('Ошибка закрытия мертвого соединения %s: %s',
                                key, e)
            # Создаем новое соединение
            logger.info('Создание нового соединения')
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

    def check_connection_alive(self, writer):
        """Проверка живо ли соединение"""
        try:
            sock = writer.get_extra_info('socket')
            if not sock:
                return False
            if sock.fileno() == -1:
                return False
            return True
        except:
            return False


def round_robin_balancer():
    global backend_index
    url = UPSTREAMS[backend_index]
    backend_index = (backend_index + 1) % len(UPSTREAMS)
    return url


connection_pool = None


async def init_pool(max_size, timeout, max_request):
    global connection_pool
    connection_pool = ConnectionPool(max_size, timeout, max_request)
    await connection_pool.start()
    return connection_pool


async def client_connected(client_reader: StreamReader,
                           client_writer: StreamWriter):
    """Обработчик клиентa с поддержкой keep-alive"""
    async with SEMAPHORE:
        logger.info(f"Семафор клиента захвачен (свободно: {SEMAPHORE._value})")
        address = client_writer.get_extra_info('peername')
        logger.info('Клиент подключен %s', address)
        try:
            async with asyncio.timeout(TIMEOUTS['total_ms']):
                logger.info("Подключение к апстриму")
                url = round_robin_balancer()
                upstream_writer, upstream_reader = await connection_pool.get_pool(
                    url['host'], url['port']
                )
                upstream_address = upstream_writer.get_extra_info('peername')
                logger.info("Подключено к апстриму %s", upstream_address)
                address = upstream_address[0]
                backend_metrics_request(address)
                keep_alive = True
                while keep_alive:
                    data = await asyncio.wait_for(client_reader.read(1024),
                                                  timeout=TIMEOUTS['read_ms'])
                    if not data:
                        logger.info('Клиент закрыл соединение')
                        break

                    request_info = parser.request_parser(data)
                    valid_request = request_info['is_valid']
                    if not valid_request:
                        logger.info('Невалидный запрос')
                        keep_alive = False
                        break
                    upstream_writer.write(data)
                    await asyncio.wait_for(upstream_writer.drain(),
                                           timeout=TIMEOUTS['write_ms'])
                    keep_alive = request_info['keep_alive']
                    client_task = asyncio.create_task(
                        proxy_server(client_reader, upstream_writer, address,
                                     'клиент'))
                    upstrem_task = asyncio.create_task(
                        proxy_server(upstream_reader, client_writer, address,
                                     'апстрим'))
                    await asyncio.gather(upstrem_task, client_task)
                    if not keep_alive:
                        break
        except asyncio.TimeoutError:
            logger.info('Запрос превысил лимит времени')
        except Exception as e:
            logger.info('Ошибка клиента %s', e)
        finally:
            client_writer.close()
            await client_writer.wait_closed()
            upstream_writer.close()
            await upstream_writer.wait_closed()
            logger.info('Клиент отключен %s', address)
    logger.info(f"Семафор клиента освобожден (свободно: {SEMAPHORE._value})")


async def main_server(host: str, port: int):
    await init_pool(10, 60, 100)
    srv = await asyncio.start_server(client_connected, host, port)
    logger.info('Сервер запущен')

    async with srv:
        await srv.serve_forever()
