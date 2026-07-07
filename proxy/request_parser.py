from logger import logger


class HttpRequestParser:
    """Минимальный парсер http запросов"""
    def __init__(self):
        self.method = ''
        self.path = ''
        self.version = ''
        self.headers = {}
        self.body = ''

    def parse_start_line(self, line):
        """Парсит первую строку для получения method, path, version"""
        try:
            parts = line.decode("utf-8").splitlines()[0].split(' ')
            self.method = parts[0]
            self.path = parts[1]
            self.version = parts[2]
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
        except Exception as e:
            logger.info('Ошибка получения заголовков: %s', e)

    def request_parser(self, data):
        """Принимает данные и запускает обработку"""
        header_end = data.find(b'\r\n\r\n')
        if header_end != -1:
            lines = data.split(b'\r\n')
            self.parse_start_line(lines[0])
            self.parse_headers(lines[1:])
            self.body = data[header_end+4:]
        return
