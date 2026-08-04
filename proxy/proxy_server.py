import asyncio
import time
from asyncio import Semaphore

from config import config
from logger import logger
from metrics import backend_metrics_time
from request_parser import HttpRequestParser

SEMAPHORE = Semaphore(config['limits']['max_conns_per_upstream'])
TIMEOUTS = config['timeouts']


async def proxy_server(reader, writer, backend, description):
    async with SEMAPHORE:
        start_time = time.time()
        logger.info(f"Семафор апстрима захвачен (свободно:{SEMAPHORE._value})")
        try:
            while True:
                data = await asyncio.wait_for(reader.read(1024),
                                                timeout=TIMEOUTS['read_ms']/1000)
                if not data:
                    break
                if description == 'апстрим':
                    parser = HttpRequestParser()
                    parser.parser_status_code(data)
                writer.write(data)
                await asyncio.wait_for(writer.drain(),
                                        timeout=TIMEOUTS['write_ms']/1000)
                if data:
                    logger.info('%s: Отправлено %s байт', description,
                                len(data))
                    logger.info('Получен ответ %s: %s', description,
                                data.decode('utf-8'))
                backend_metrics_time(backend, time.time() - start_time)
        except Exception as e:
            logger.info('Ошибка прокси %s', e)
    logger.info(f"Семафор апстрима освобожден (свободно: {SEMAPHORE._value})")
