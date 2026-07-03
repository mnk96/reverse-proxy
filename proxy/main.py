import asyncio
from server import main_server
# from client_handler import main_client

if __name__ == '__main__':
    asyncio.run(main_server('127.0.0.1', 8000))