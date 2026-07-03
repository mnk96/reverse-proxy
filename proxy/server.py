import logging
import sys
import asyncio
from asyncio.streams import StreamReader, StreamWriter
from request_parser import HttpRequestParser
# from client_handler import main_client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

parser = HttpRequestParser()

async def transfer_data(reader, writer, description):
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            try:
                parser.request_parser(data)
            except Exception as e:
                print('error')

            writer.write(data)
            await writer.drain()
            logger.info('%s: Отправлено %s байт', description, len(data))
    except Exception as e:
        logger.info('Ошибка %s', e)
    finally:
        writer.close()
        await writer.wait_closed()

async def client_connected(client_reader: StreamReader, client_writer: StreamWriter):
    """Обработчик клиента"""
    address = client_writer.get_extra_info('peername')
    logger.info('Клиент подключен %s', address)

    stream_reader = None
    stream_writer = None
    try:
        logger.info("Подключение к апстриму")
        stream_reader, stream_writer = await asyncio.open_connection('127.0.0.1', 8000)
        upstream_address = stream_writer.get_extra_info('peername')
        logger.info("Подключено к апстриму %s", upstream_address)
        upstrem_task = asyncio.create_task(transfer_data(stream_reader, client_writer, 'апстрим-клиент'))
        client_task = asyncio.create_task(transfer_data(client_reader, stream_writer, 'клиент-апстрим'))
        await asyncio.gather(upstrem_task, client_task)
    except Exception as e:
        logger.info('Ошибка %s', e)
    finally:
        # if stream_writer:
        stream_writer.close()
        await stream_writer.wait_closed()
        logger.info('Соединение с апстримом закрыто')
        client_writer.close()
        await client_writer.wait_closed()
        logger.info('Соединение с клиентом закрыто')


async def main_server(host: str, port: int):
    srv = await asyncio.start_server(
        client_connected, host, port)
    print('Сервер запущен')

    async with srv:
        await srv.serve_forever()


if __name__ == '__main__':
    asyncio.run(main_server('127.0.0.1', 8000))