import asyncio
from server import main_server
from metrics import metrics_server
from config import config


async def main():
    main_address = config['listen'].split(':')
    metrics_address = config['metrics'].split(':')
    proxy_task = asyncio.create_task(main_server(main_address[0],
                                                 int(main_address[1])))
    metrics_task = asyncio.create_task(metrics_server(metrics_address[0],
                                                      int(metrics_address[1])))

    await asyncio.gather(proxy_task, metrics_task)


if __name__ == '__main__':
    asyncio.run(main())
