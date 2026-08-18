import socket
import threading
import time
from threading import Semaphore
from typing import TypedDict

from config import settings
from logger import logger
from metrics import backend_metrics_request
from proxy_server import proxy_server
from request_parser import HttpMessageParser


class ConnectionData(TypedDict):
    sock: socket.socket
    # writer: asyncio.StreamWriter
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
        self.active_conns: dict[socket.socket, dict] = {}
        self.semaphore: dict[str, Semaphore] = {}
        self.locks: dict[str, threading.Lock] = {}

    def _make_conn_data(
            self,
            sock: socket.socket,
            # writer: StreamWriter,
            address: str
    ) -> ConnectionData:
        return {
            'sock': socket.socket,
            'last_used': time.time(),
            'created': time.time(),
            'request_count': 0,
            'address': address
        }

    def _safe_close(
            self,
            sock: socket.socket
    ) -> None:
        try:
            sock.close()
            # await writer.wait_closed()
            logger.info('Соединение закрыто')
        except Exception as e:  # noqa: BLE001
            logger.error('Ошибка закрытия соединения: %s', e)

    def start(self) -> None:
        """Создание начальных соединений"""
        for upstream in settings.upstreams:
            host = upstream['host']
            port = upstream['port']
            address = f'{host}:{port}'
            self.pools[address] = []
            self.semaphore[address] = Semaphore(
                settings.limits.max_conns_per_upstream)
            self.locks[address] = threading.Lock()
            for _ in range(self.max_size):
                try:
                    sock = socket.create_connection((host, port),
                        settings.timeouts.connect_ms/1000)
                    conn_data = self._make_conn_data(sock, address)
                    self.pools[address].append(conn_data)
                    logger.info('Создано начальное соединение %s', address)
                except Exception as e:  # noqa: BLE001
                    logger.error('Ошибка создания начального соединения для %s: %s',
                                address, e)

    def get_pool(
            self,
            host: str,
            port: str
    ) -> socket.socket:
        """Получение соединения из пула или создание нового"""
        address = f'{host}:{port}'
        if address not in self.pools:
            logger.info('Неизвестное соединение %s', address)
            raise ValueError(f'Неизвестное соединение {address}')
        self.semaphore[address].acquire()
        # logger.info('Семафор захвачен %s(свободно %s)', address, self.semaphore[address].value)
        logger.info('Семафор захвачен %s(свободно)', address)
        with self.locks[address]:
            pool = self.pools[address]
            # Если есть существующее соединение
            while True:
                if pool:
                    conn_data = pool[0]
                    sock = conn_data['sock']
                    # Проверяем, что соединение живо и не используется
                    if self.check_connection_alive(sock) and sock not in self.active_conns:
                        conn_data['last_used'] = time.time()
                        conn_data['request_count'] += 1
                        self.active_conns[sock] = conn_data
                        pool.pop(0)
                        logger.info('Переиспользовано соединение %s', address)
                        # Проверка лимита запросов
                        if conn_data['request_count'] >= settings.limits.max_requests_per_conns:
                            logger.info('Соединение %s достигло лимита запросов', address)
                            self._safe_close(sock)
                            del self.active_conns[sock]
                            continue
                        return sock
                    else:
                        if sock in self.active_conns:
                            logger.info('Найдено активное соединение %s', address)
                            del self.active_conns[sock]
                        else:
                            logger.info('Найдено мертвое соединение %s', address)
                        self._safe_close(sock)
                # Создаем новое соединение
                logger.info('Создание нового соединения')
                try:
                    sock = socket.create_connection((host, port), settings.timeouts.connect_ms/1000)
                    conn_data = self._make_conn_data(sock, address)
                    self.active_conns[sock] = conn_data
                    logger.info('Создано новое соединение %s', address)
                    return sock
                except Exception as e:  # noqa: BLE001
                    logger.error('Ошибка создания соединения %s', e)
                    self.semaphore[address].release()
                    # logger.error('Семафор освобожден %s(свободно %s)', address,
                    #             self.semaphore[address].value)
                    logger.error('Семафор освобожден %s(свободно)', address)
                    raise ValueError('Ошибка создания соединения')  # noqa: B904

    def check_connection_alive(
            self,
            sock: socket.socket
    ) -> bool:
        """Проверка живо ли соединение"""
        try:
            return sock.fileno() != -1
        except Exception:  # noqa: BLE001
            return False

    def put_back_pool(
            self,
            sock: socket.socket
    ) -> None:
        """Возвращает соединение в пулл после использования"""
        if sock not in self.active_conns:
            logger.info('Попытка вернуть неизвестное соединение')
            self._safe_close(sock)
            return
        conn_data = self.active_conns[sock]
        address = conn_data['address']
        if address is None or address not in self.pools:
            logger.info('Неизвестный бекенд')
            self._safe_close(sock)
            del self.active_conns[sock]
            return
        # Проверка можно ли вернуть соедение
        with self.locks[address]:
            if self.check_connection_alive(sock) and len(self.pools[address]) < self.max_size:
                conn_data['last_used'] = time.time()
                self.pools[address].append(conn_data)
                logger.info('Соединение возвращено')
            else:
                logger.info('Закрытие соединения')
                self._safe_close(sock)
        del self.active_conns[sock]
        self.semaphore[address].release()
        # logger.info('Семафор освобожден %s(свободно %s)', address,
        #                                     self.semaphore[address].value)
        logger.info('Семафор освобожден %s(свободно)', address)

class RoundRobin:

    def __init__(self) -> None:
        self.backend_index = 0

    def round_robin_balancer(self) -> tuple[str, int]:
        url = settings.upstreams[self.backend_index]
        self.backend_index = (self.backend_index + 1) % len(settings.upstreams)
        return url

round_robin = RoundRobin()


def client_connected(
        client_sock: socket.socket,
        connection_pool: ConnectionPool
) -> None:
    """Обработчик клиeнтa c поддержкой keep-alive"""
    address = client_sock.getpeername()
    upstream_socket = None
    logger.info('Клиент подключен %s', address)
    try:
        client_sock.settimeout(settings.timeouts.total_ms/1000)
        logger.info("Подключение к апстриму")
        url = round_robin.round_robin_balancer()
        upstream_socket = connection_pool.get_pool(url['host'], url['port'])
        upstream_address = upstream_socket.getpeername()
        logger.info("Подключено к апстриму %s", upstream_address)
        address = upstream_address[0]
        backend_metrics_request(address)
        keep_alive = True
        while keep_alive:
            header_end = -1
            headers_data = b''
            parser = HttpMessageParser()
            while header_end == -1:
                data = client_sock.recv(settings.chunk_size)
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
                data = client_sock.recv(settings.chunk_size)
                if not data:
                    logger.info('Клиент закрыл соединение')
                    headers_data = b''
                    body_data = b''
                    keep_alive = False
                    break
                body_data += data
            if not keep_alive:
                break
            parser = HttpMessageParser()
            request_info = parser.request_parser(headers_data[:header_end+4] + body_data)
            valid_request = request_info['is_valid']
            keep_alive = request_info['keep_alive']
            if not valid_request:
                logger.info('Невалидный запрос')
                keep_alive = False
                break
            upstream_socket.sendall(headers_data[:header_end+4] + body_data)
            keep_alive = request_info['keep_alive']
            client_task = threading.Thread(target=proxy_server, args=(client_sock, address, 'клиент'))
            upstrem_task = threading.Thread(target=proxy_server, args=(upstream_socket, address, 'апстрим'))
            client_task.start()
            upstrem_task.start()
            client_task.join()
            upstrem_task.join()
            if not keep_alive:
                break
    except TimeoutError:
        logger.error('Запрос превысил лимит времени')
    except Exception as e:  # noqa: BLE001
        logger.error('Ошибка клиента %s', e)
    finally:
        # Возвращаем соединение в пул
        if upstream_socket:
            try:
                connection_pool.put_back_pool(upstream_socket)
                logger.info('Соединение возвращено в пул')
            except Exception as e:  # noqa: BLE001
                logger.error('Ошибка возврата соединения в пул: %s', e)
        # upstream_socket.close()
        logger.info('Клиент отключен %s', address)


def main_server(
        host: str,
        port: int
) -> None:
    connection_pool = ConnectionPool(settings.max_size_conn,
                                     settings.limits.max_conns_per_upstream)
    connection_pool.start()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(5)
    logger.info('Сервер запущен')
    while True:
        client_socket, address = server_socket.accept()
        message = client_socket.recv(1024).decode('utf-8')
        print(f"Получено сообщение: {message}")
        print(client_socket, address)
        client_thread = threading.Thread(target=client_connected, args=(client_socket, connection_pool))
        client_thread.daemon = True
        client_thread.start()
