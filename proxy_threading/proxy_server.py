# import asyncio
import threading
import socket
from threading import Semaphore
import time
# from asyncio import Semaphore
# from asyncio.streams import StreamReader, StreamWriter

from config import settings
from logger import logger
from metrics import backend_metrics_time
from request_parser import HttpMessageParser

SEMAPHORE = Semaphore(settings.limits.max_conns_per_upstream)


def proxy_server(
        sock: socket.socket,
        backend: str,
        description: str
) -> None:
    with SEMAPHORE:
        start_time = time.time()
        logger.info("Семафор апстрима захвачен (свободно: %s)", SEMAPHORE._value)
        try:
            while True:
                sock.settimeout(settings.timeouts.read_ms/1000)
                data = sock.recv(settings.chunk_size)
                if not data:
                    break
                if description == 'апстрим':
                    HttpMessageParser.parser_status_code(data)
                sock.sendall(data)
                if data:
                    logger.info('%s: Отправлено %s байт', description,
                                len(data))
                    logger.info('Получен ответ %s', description)
                backend_metrics_time(backend, time.time() - start_time)
        except Exception as e:  # noqa: BLE001
            logger.error('Ошибка прокси %s', e)
    logger.info("Семафор апстрима освобожден (свободно: %s)", SEMAPHORE._value)
