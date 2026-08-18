import asyncio
import time
from asyncio import Semaphore
from asyncio.streams import StreamReader, StreamWriter
from typing import TypedDict

from config import settings
from logger import logger
from metrics import backend_metrics_request
from proxy_server import proxy_server
from request_parser import HttpMessageParser


class ConnectionData(TypedDict):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    last_used: float
    created: float
    request_count: int
    address: str


class SemaphoreWithCounter:
    def __init__(self, value: int) -> None:
        self._semaphore = Semaphore(value)
        self._counter = value

    async def acquire(self) -> None:
        await self._semaphore.acquire()
        self._counter -= 1

    def release(self) -> None:
        self._semaphore.release()
        self._counter += 1

    @property
    def value(self) -> int:
        return self._counter


class ConnectionPool:
    """Пул соединений"""
    def __init__(
            self,
            max_size: int,
            max_requests: int
    ) -> None:
        self.max_size: int = max_size
        self.max_requests: int = max_requests

        self.pools: dict[str, dict] = {}
        self.active_conns: dict[StreamWriter, dict] = {}
        self.semaphore: dict[str, Semaphore] = {}
        self.locks: dict[str, asyncio.Lock] = {}

    def _make_conn_data(
            self,
            reader: StreamReader,
            writer: StreamWriter,
            address: str
    ) -> ConnectionData:
        return {
            'reader': reader,
            'writer': writer,
            'last_used': time.time(),
            'created': time.time(),
            'request_count': 0,
            'address': address
        }

    async def _safe_close(
            self,
            writer: StreamWriter
    ) -> None:
        try:
            writer.close()
            await writer.wait_closed()
            logger.info('Соединение закрыто')
        except Exception as e:  # noqa: BLE001
            logger.error('Ошибка закрытия соединения: %s', e)

    async def start(self) -> None:
        """Создание начальных соединений"""
        for upstream in settings.upstreams:
            host = upstream['host']
            port = upstream['port']
            address = f'{host}:{port}'
            self.pools[address] = []
            self.semaphore[address] = SemaphoreWithCounter(
                settings.limits.max_conns_per_upstream)
            self.locks[address] = asyncio.Lock()
            for _ in range(self.max_size):
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port),
                        settings.timeouts.connect_ms/1000)
                    conn_data = self._make_conn_data(reader, writer, address)
                    self.pools[address].append(conn_data)
                    logger.info('Создано начальное соединение %s', address)
                except Exception as e:  # noqa: BLE001
                    logger.error('Ошибка создания начального соединения для %s: %s',
                                address, e)

    async def get_pool(
            self,
            host: str,
            port: str
    ) -> tuple[StreamWriter, StreamReader]:
        """Получение соединения из пула или создание нового"""
        address = f'{host}:{port}'
        if address not in self.pools:
            logger.info('Неизвестное соединение %s', address)
            raise ValueError(f'Неизвестное соединение {address}')
        await self.semaphore[address].acquire()
        logger.info('Семафор захвачен %s(свободно %s)', address, self.semaphore[address].value)
        async with self.locks[address]:
            pool = self.pools[address]
            # Если есть существующее соединение
            while True:
                if pool:
                    conn_data = pool[0]
                    writer = conn_data['writer']
                    reader = conn_data['reader']
                    # Проверяем, что соединение живо и не используется
                    if self.check_connection_alive(writer) and writer not in self.active_conns:
                        conn_data['last_used'] = time.time()
                        conn_data['request_count'] += 1
                        self.active_conns[writer] = conn_data
                        pool.pop(0)
                        logger.info('Переиспользовано соединение %s', address)
                        # Проверка лимита запросов
                        if conn_data['request_count'] >= settings.limits.max_requests_per_conns:
                            logger.info('Соединение %s достигло лимита запросов', address)
                            await self._safe_close(writer)
                            del self.active_conns[writer]
                            continue
                        return writer, reader
                    else:
                        if writer in self.active_conns:
                            logger.info('Найдено активное соединение %s', address)
                            del self.active_conns[writer]
                        else:
                            logger.info('Найдено мертвое соединение %s', address)
                        await self._safe_close(writer)
                # Создаем новое соединение
                logger.info('Создание нового соединения')
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    conn_data = self._make_conn_data(reader, writer, address)
                    self.active_conns[writer] = conn_data
                    logger.info('Создано новое соединение %s', address)
                    return writer, reader
                except Exception as e:  # noqa: BLE001
                    logger.error('Ошибка создания соединения %s', e)
                    self.semaphore[address].release()
                    logger.error('Семафор освобожден %s(свободно %s)', address,
                                self.semaphore[address].value)
                    raise ValueError('Ошибка создания соединения')  # noqa: B904

    def check_connection_alive(
            self,
            writer: StreamReader
    ) -> bool:
        """Проверка живо ли соединение"""
        try:
            sock = writer.get_extra_info('socket')
            if not sock:
                return False
            return sock.fileno() != -1
        except Exception:  # noqa: BLE001
            return False

    async def put_back_pool(
            self,
            writer: StreamWriter,
            reader: StreamReader|None=None
    ) -> None:
        """Возвращает соединение в пулл после использования"""
        if writer not in self.active_conns:
            logger.info('Попытка вернуть неизвестное соединение')
            await self._safe_close(writer)
            return
        conn_data = self.active_conns[writer]
        address = conn_data['address']
        if address is None or address not in self.pools:
            logger.info('Неизвестный бекенд')
            await self._safe_close(writer)
            del self.active_conns[writer]
            return
        # Проверка можно ли вернуть соедение
        async with self.locks[address]:
            if self.check_connection_alive(writer) and len(self.pools[address]) < self.max_size:
                conn_data['last_used'] = time.time()
                if reader:
                    conn_data['reader'] = reader
                self.pools[address].append(conn_data)
                logger.info('Соединение возвращено')
            else:
                logger.info('Закрытие соединения')
                await self._safe_close(writer)
        del self.active_conns[writer]
        self.semaphore[address].release()
        logger.info('Семафор освобожден %s(свободно %s)', address,
                                            self.semaphore[address].value)

class RoundRobin:

    def __init__(self) -> None:
        self.backend_index = 0

    def round_robin_balancer(self) -> tuple[str, int]:
        url = settings.upstreams[self.backend_index]
        self.backend_index = (self.backend_index + 1) % len(settings.upstreams)
        return url

round_robin = RoundRobin()


async def client_connected(
        client_reader: StreamReader,
        client_writer: StreamWriter,
        connection_pool: ConnectionPool
) -> None:
    """Обработчик клиeнтa c поддержкой keep-alive"""
    address = client_writer.get_extra_info('peername')
    upstream_writer, upstream_reader = None, None
    logger.info('Клиент подключен %s', address)
    try:
        async with asyncio.timeout(settings.timeouts.total_ms/1000):
            logger.info("Подключение к апстриму")
            url = round_robin.round_robin_balancer()
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
                parser = HttpMessageParser()
                while header_end == -1:
                    data = await asyncio.wait_for(client_reader.read(settings.chunk_size),
                                                    timeout=settings.timeouts.read_ms/1000)
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
                    data = await asyncio.wait_for(client_reader.read(settings.chunk_size),
                                                    timeout=settings.timeouts.read_ms/1000)
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
                parser = HttpMessageParser()
                request_info = parser.request_parser(headers_data[:header_end+4] + body_data)
                valid_request = request_info['is_valid']
                keep_alive = request_info['keep_alive']
                if not valid_request:
                    logger.info('Невалидный запрос')
                    keep_alive = False
                    upstream_writer.close()
                    await upstream_writer.wait_closed()
                    break
                upstream_writer.write(headers_data[:header_end+4] + body_data)
                await asyncio.wait_for(upstream_writer.drain(),
                                        timeout=settings.timeouts.write_ms/1000)
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
        logger.error('Запрос превысил лимит времени')
    except Exception as e:  # noqa: BLE001
        logger.error('Ошибка клиента %s', e)
    finally:
        # Возвращаем соединение в пул
        if upstream_writer:
            try:
                await connection_pool.put_back_pool(upstream_writer, upstream_reader)
                logger.info('Соединение возвращено в пул')
            except Exception as e:  # noqa: BLE001
                logger.error('Ошибка возврата соединения в пул: %s', e)
        client_writer.close()
        await client_writer.wait_closed()
        logger.info('Клиент отключен %s', address)


async def main_server(
        host: str,
        port: int
) -> None:
    connection_pool = ConnectionPool(settings.max_size_conn,
                                     settings.limits.max_conns_per_upstream)
    await connection_pool.start()
    srv = await asyncio.start_server(lambda reader, writer: client_connected(reader, writer,
                                                                             connection_pool),
                                                                             host, port)
    logger.info('Сервер запущен')

    async with srv:
        await srv.serve_forever()
