import asyncio
from asyncio import Semaphore
from request_parser import HttpRequestParser
from metrics import backend_metrics_time
from logger import logger
from config import config
import time

SEMAPHORE = Semaphore(config['limits']['max_conns_per_upstream'])
TIMEOUTS = config['timeouts']
parser = HttpRequestParser()


async def proxy_server(reader, writer, backend, description):
    async with SEMAPHORE:
        start_time = time.time()
        logger.info(f"Семафор апстрима захвачен (свободно:{SEMAPHORE._value})")
        try:
            while True:
                data = await asyncio.wait_for(reader.read(1024),
                                              timeout=TIMEOUTS['read_ms'])
                if not data:
                    break
                parser.request_parser(data)
                writer.write(data)
                await asyncio.wait_for(writer.drain(),
                                       timeout=TIMEOUTS['write_ms'])
                logger.info('%s: Отправлено %s байт', description, len(data))
                logger.info('Получен ответ %s: %s', description,
                            data.decode('utf-8'))
                backend_metrics_time(backend, time.time() - start_time)
        except Exception as e:
            logger.info('Ошибка %s', e)
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info('%s: Соединение закрыто', description)
    logger.info(f"Семафор апстрима освобожден (свободно: {SEMAPHORE._value})")
