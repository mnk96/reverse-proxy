from multiprocessing import Process

from config import settings
from metrics import metrics_server
from server import main_server


def main() -> None:
    proxy_task = Process(target=main_server, args = (settings.listen.host, settings.listen.port))
    metrics_task = Process(target=metrics_server, args = (settings.metrics.host, settings.metrics.port))
    proxy_task.start()
    metrics_task.start()
    proxy_task.join()
    metrics_task.join()


if __name__ == '__main__':
    main()
