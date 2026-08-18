import asyncio
import time
from asyncio import Semaphore
from asyncio.streams import StreamReader, StreamWriter

from config import settings
from logger import logger
from metrics import backend_metrics_time
from request_parser import HttpMessageParser

SEMAPHORE = Semaphore(settings.limits.max_conns_per_upstream)


async def proxy_server(
        reader: StreamReader,
        writer: StreamWriter,
        backend: str,
        description: str
) -> None:
    async with SEMAPHORE:
        start_time = time.time()
        logger.info("Семафор апстрима захвачен (свободно: %s)", SEMAPHORE._value)
        try:
            while True:
                data = await asyncio.wait_for(reader.read(settings.chunk_size),
                                                timeout=settings.timeouts.read_ms/1000)
                if not data:
                    break
                if description == 'апстрим':
                    HttpMessageParser.parser_status_code(data)
                writer.write(data)
                await asyncio.wait_for(writer.drain(),
                                        timeout=settings.timeouts.write_ms/1000)
                if data:
                    logger.info('%s: Отправлено %s байт', description,
                                len(data))
                    logger.info('Получен ответ %s', description)
                backend_metrics_time(backend, time.time() - start_time)
        except Exception as e:  # noqa: BLE001
            logger.error('Ошибка прокси %s', e)
    logger.info("Семафор апстрима освобожден (свободно: %s)", SEMAPHORE._value)
