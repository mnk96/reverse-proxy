from typing import TypedDict

from logger import logger


class RequestInfo(TypedDict):
    method: str
    path: str
    version: str
    headers: dict[str, str]
    body: bytes
    keep_alive: bool
    is_valid: bool
    content_length: int
    end_request: bool


class HttpMessageParser:
    """Минимальный парсер http запросов"""
    ALLOWED_METHODS = ('GET', 'POST', 'DELETE') # методы, поддерживаемые апстримами

    def __init__(self) -> None:
        self.method: str = ''
        self.path: str = ''
        self.version: str = ''
        self.headers: dict[str, str] = {}
        self.body: bytes = b''
        self.keep_alive: bool = True
        self.is_valid: bool = True
        self.content_length: int = 0
        self.end_request: bool = False

    def parse_start_line(
            self,
            line: bytes
    ) -> None:
        """Парсит первую строку для получения method, path, version"""
        try:
            parts = line.decode("utf-8").splitlines()[0].split(' ')
            self.method = parts[0]
            self.path = parts[1]
            if (self.method.upper() not in self.ALLOWED_METHODS or
                not self.path or not self.path.startswith('/')):
                self.is_valid = False
            self.version = parts[2]
            if self.version != 'HTTP/1.1':
                self.keep_alive = False
        except Exception as e:  # noqa: BLE001
            logger.info('Ошибка получения данных из стартовой строки: %s', e)

    def parse_headers(
            self,
            data: bytes
    ) -> None:
        try:
            for line in data:
                if b':' in line:
                    header_info = line.decode("utf-8").split(': ')
                    self.headers[header_info[0]] = header_info[1]
                    if header_info[0].lower() == 'content-length':
                        self.content_length = int(header_info[1])
                    if header_info[0].lower() == 'connection':
                        if header_info[1].lower() == 'close':
                            self.keep_alive = False
                        elif header_info[1].lower() == 'keep-alive':
                            self.keep_alive = True
        except Exception as e:  # noqa: BLE001
            logger.info('Ошибка получения заголовков: %s', e)

    @staticmethod
    def parser_status_code(data: bytes) -> None:
        try:
            lines = data.split(b'\r\n')
            if not lines:
                return
            first_line = lines[0].decode('utf-8')
            string = first_line.split(' ')
            if 'HTTP' in string[0]:
                logger.info('Cтaтyca ответа: %s', string[1])
            else:
                return
        except Exception as e:  # noqa: BLE001
            logger.info('Ошибка получения статуса ответа: %s', e)

    def request_parser(
            self,
            data: bytes
    ) -> RequestInfo:
        """Принимает данные и запускает обработку"""
        header_end = data.find(b'\r\n\r\n')
        if header_end != -1:
            lines = data.split(b'\r\n')
            if lines:
                self.parse_start_line(lines[0])
                self.parse_headers(lines[1:header_end])
                self.body = data[header_end+4:]
            else:
                self.is_valid = False
        else:
            self.is_valid = False
        return {
            'method': self.method,
            'path': self.path,
            'version': self.version,
            'header': self.headers,
            'body': self.body,
            'keep_alive': self.keep_alive,
            'is_valid': self.is_valid,
            'end_request': self.end_request,
            'content_length': self.content_length
            }
