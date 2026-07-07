import asyncio
from server import main_server
from metrics import metrics_server


async def main():
    proxy_task = asyncio.create_task(main_server('127.0.0.1', 8080))
    metrics_task = asyncio.create_task(metrics_server('127.0.0.1', 9090))

    await asyncio.gather(proxy_task, metrics_task)


if __name__ == '__main__':
    asyncio.run(main())
