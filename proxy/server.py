import asyncio
import time
from asyncio import Semaphore
from asyncio.streams import StreamReader, StreamWriter

from config import config
from logger import logger
from metrics import backend_metrics_request
from proxy_server import proxy_server
from request_parser import HttpRequestParser

UPSTREAMS = config['upstreams']
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
        self.semaphore = {}

    async def start(self):
        """Создание начальных соединений"""
        for upstream in UPSTREAMS:
            host = upstream['host']
            port = upstream['port']
            key = f'{host}:{port}'
            self.pool_list[key] = []
            self.semaphore[key] = Semaphore(config['limits']['max_client_conns'])
            for _ in range(self.max_size):
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port),
                        TIMEOUTS['connect_ms']/1000)
                    conn_data = {
                        'reader': reader,
                        'writer': writer,
                        'last_used': time.time(),
                        'created': time.time(),
                        'request_count': 0
                    }
                    self.pool_list[key].append(conn_data)
                    logger.info('Создано начальное соединение %s', key)
                except Exception as e:
                    logger.info('Ошибка создания начального соединения для %s: %s',
                                key, e)

    async def get_pool(self, host, port):
        """Получение соединения из пула или создание нового"""
        key = f'{host}:{port}'
        if key not in self.pool_list:
            logger.info('Неизвестное соединение %s', key)
            raise
        pool = self.pool_list[key]
        await self.semaphore[key].acquire()
        logger.info('Семафор захвачен %s(свободно %s)', key, self.semaphore[key]._value)
        # Если есть существующее соединение
        while pool:
            conn_data = pool[0]
            writer = conn_data['writer']
            reader = conn_data['reader']
            if self.check_connection_alive(writer):
                conn_data['last_used'] = time.time()
                conn_data['request_count'] += 1
                self.active_list[writer] = conn_data
                pool.pop(0)
                logger.info('Переиспользовано соединение %s', key)
                if conn_data['request_count'] >= config['limits']['max_requests_per_conns']:
                    logger.info('Соединение %s достигло лимита запросов', key)
                    writer.close()
                    await writer.wait_closed()
                    logger.info('Соединение закрыто %s', key)
                    continue
                self.semaphore[key].release()
                logger.info('Семафор освобожден %s(свободно %s)', key, self.semaphore[key]._value)
                return writer, reader
            else:
                logger.info('Найдено мертвое соединение %s', key)
                self.semaphore[key].release()
                logger.info('Семафор освобожден %s(свободно %s)', key, self.semaphore[key]._value)
                try:
                    writer.close()
                    await writer.wait_closed()
                    self.pool_list[key].pop(0)
                except Exception as e:
                    logger.info('Ошибка закрытия мертвого соединения %s: %s',
                                key, e)
            # Создаем новое соединение
            logger.info('Создание нового соединения')
            try:
                reader, writer = await asyncio.open_connection(host, port)
                conn_data = {
                    'reader': reader,
                    'writer': writer,
                    'last_used': time.time(),
                    'created': time.time(),
                    'request_count': 0
                }
                self.pool_list[key].append(conn_data)
                logger.info('Создано новое соединение %s', key)
                return writer, reader
            except Exception as e:
                logger.info('Ошибка создания соединения %s', e)
                self.semaphore[key].release()
                logger.info('Семафор освобожден %s(свободно %s)', key, self.semaphore[key]._value)
                raise

    def check_connection_alive(self, writer):
        """Проверка живо ли соединение"""
        try:
            sock = writer.get_extra_info('socket')
            if not sock:
                return False
            return sock.fileno() != -1
        except Exception:
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
    """Обработчик клиeнтa c поддержкой keep-alive"""
    address = client_writer.get_extra_info('peername')
    logger.info('Клиент подключен %s', address)
    try:
        async with asyncio.timeout(TIMEOUTS['total_ms']/1000):
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
                header_end = -1
                headers_data = b''
                parser = HttpRequestParser()
                while header_end == -1:
                    data = await asyncio.wait_for(client_reader.read(8192),
                                                    timeout=TIMEOUTS['read_ms']/1000)
                    if not data:
                        logger.info('Клиент закрыл соединение')
                        headers_data = b''
                        body_data = b''
                        keep_alive = False
                        break
                    headers_data += data
                    header_end = headers_data.find(b'\r\n\r\n')
                request_info = parser.request_parser(headers_data)
                content_length = request_info['content_length']
                body_data = headers_data[header_end+4:]
                while len(body_data) < content_length:
                    data = await asyncio.wait_for(client_reader.read(8192),
                                                    timeout=TIMEOUTS['read_ms']/1000)
                    if not data:
                        logger.info('Клиент закрыл соединение')
                        headers_data = b''
                        body_data = b''
                        keep_alive = False
                        break
                    body_data += data
                if not keep_alive:
                    upstream_writer.close()
                    await upstream_writer.wait_closed()
                    break
                parser = HttpRequestParser()
                request_info = parser.request_parser(headers_data+body_data)
                valid_request = request_info['is_valid']
                keep_alive = request_info['keep_alive']
                if not valid_request:
                    logger.info('Невалидный запрос')
                    keep_alive = False
                    upstream_writer.close()
                    await upstream_writer.wait_closed()
                    break
                upstream_writer.write(data+body_data)
                await asyncio.wait_for(upstream_writer.drain(),
                                        timeout=TIMEOUTS['write_ms']/1000)
                keep_alive = request_info['keep_alive']
                client_task = asyncio.create_task(
                    proxy_server(client_reader, upstream_writer, address,
                                    'клиент'))
                upstrem_task = asyncio.create_task(
                    proxy_server(upstream_reader, client_writer, address,
                                    'апстрим'))
                await asyncio.gather(upstrem_task, client_task)
                if not keep_alive:
                    upstream_writer.close()
                    await upstream_writer.wait_closed()
                    break
    except TimeoutError:
        logger.info('Запрос превысил лимит времени')
    except Exception as e:
        logger.info('Ошибка клиента %s', e)
    finally:
        client_writer.close()
        await client_writer.wait_closed()
        logger.info('Клиент отключен %s', address)


async def main_server(host: str, port: int):
    await init_pool(10, 60, 100)
    srv = await asyncio.start_server(client_connected, host, port)
    logger.info('Сервер запущен')

    async with srv:
        await srv.serve_forever()
