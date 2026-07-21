from logger import logger


class HttpRequestParser:
    """Минимальный парсер http запросов"""
    ALLOWED_METHODS = {'GET', 'POST', 'PUT', 'DELETE'}

    def __init__(self):
        self.method = ''
        self.path = ''
        self.version = ''
        self.headers = {}
        self.body = ''
        self.keep_alive = True
        self.is_valid = True

    def parse_start_line(self, line):
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
        except Exception as e:
            logger.info('Ошибка получения данных из стартовой строки: %s', e)

    def parse_headers(self, data):
        try:
            for line in data:
                if not line:
                    break
                if b':' in line:
                    header_info = line.decode("utf-8").split(': ')
                    self.headers[header_info[0]] = header_info[1]

                    if header_info[0].lower() == 'connection':
                        if header_info[1].lower() == 'close':
                            self.keep_alive = False
                        elif header_info[1].lower() == 'keep-alive':
                            self.keep_alive = True
                else:
                    self.is_valid = False
        except Exception as e:
            logger.info('Ошибка получения заголовков: %s', e)

    def request_parser(self, data):
        """Принимает данные и запускает обработку"""
        self.method = ''
        self.path = ''
        self.version = ''
        self.headers = {}
        self.body = ''
        self.keep_alive = True
        self.is_valid = True
        header_end = data.find(b'\r\n\r\n')
        if header_end != -1:
            lines = data.split(b'\r\n')
            self.parse_start_line(lines[0])
            self.parse_headers(lines[1:])
            self.body = data[header_end+4:]
        return {
            'method': self.method,
            'path': self.path,
            'version': self.version,
            'header': self.headers,
            'body': self.body,
            'keep_alive': self.keep_alive,
            'is_valid': self.is_valid
            }
