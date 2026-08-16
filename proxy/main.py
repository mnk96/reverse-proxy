import asyncio

from config import settings
from metrics import metrics_server
from server import main_server


async def main() -> None:
    proxy_task = asyncio.create_task(main_server(settings.listen.host, settings.listen.port))
    metrics_task = asyncio.create_task(metrics_server(settings.metrics.host, settings.metrics.port))

    await asyncio.gather(proxy_task, metrics_task)


if __name__ == '__main__':
    asyncio.run(main())
