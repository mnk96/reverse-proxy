import json
import socket
import threading
import time

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


def backend_metrics_time(
        backend: str,
        time: time
) -> None:
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


def handle_metrics(
        sock: socket.socket
) -> None:
    """Обработчик запросов к получению метрик"""
    address = sock.getpeername()
    logger.info('Запрос метрик от %s', address)
    try:
        data = sock.recv(settings.chunk_size)
        if not data:
            return
        metrics_json = json.dumps(metrics_list[address[0]])
        sock.sendall(metrics_json.encode('utf-8'))
    except Exception as e:  # noqa: BLE001
        logger.info('Ошибка получения метрик %s', e)
    finally:
        sock.close()


def metrics_server(
        host: str,
        port: int
) -> None:
    # server = await asyncio.start_server(handle_metrics, host, port)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(5)
    logger.info('Сервер метрик запущен')
    while True:
        client_socket, address = server_socket.accept()
        message = client_socket.recv(1024).decode('utf-8')
        print(f"Получено сообщение: {message}")
        print(client_socket, address)
        client_thread = threading.Thread(target=handle_metrics, args=(client_socket))
        client_thread.daemon = True
        client_thread.start()
    # logger.info('Сервер метрик запущен')

    # async with server:
    #     await server.serve_forever()
