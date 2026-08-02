# Мини‑Nginx на asyncio (reverse proxy)

Асинхронный reverse proxy‑сервер, который принимает HTTP‑запросы от клиента и проксирует их к одному или нескольким upstream‑сервисам.

### Функционал:
- Приём входящих соединений по TCP и обработка HTTP/1.1 запросов (минимальный парсер стартовой строки и заголовков).
- Поддержка одного или нескольких апстримов.
- Простейшая балансировка: round‑robin.
- Потоковая передача тела запроса к апстриму.
- Потоковая передача ответа от апстрима к клиенту.
- Backpressure через await writer.drain().
- Keep‑Alive: повторное использование соединений с клиентом.

## Запуск проекта

Создать и активировать виртуальное окружение
```python
python -m venv venv
source venv/Scripts/activate
```

Установить зависимости
```python
pip install -r requirements.txt
```

## Конфигурация
Пример конфигурации (config.yaml)
```yaml
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
  - host: "127.0.0.1"
    port: 9002
timeouts:
  connect_ms: 1000
  read_ms: 15000
  write_ms: 15000
  total_ms: 30000
limits:
  max_client_conns: 1000
  max_conns_per_upstream: 100
logging:
  level: "info"
```

- Поддержка **жирного**, *курсива*, ~~зачёркнутого~~
- [Ссылки](https://calcal.ru) и изображения
- Списки, цитаты, таблицы
- Блоки кода с подсветкой

## Запуск локальных апстримов

```python
uvicorn tests.echo_app:app --host 127.0.0.1 --port 9001 --workers 1
uvicorn tests.echo_app:app --host 127.0.0.1 --port 9002 --workers 1
```

## Проверка запросов

```python
curl -v http://127.0.0.1:8080/
curl -v -X POST http://127.0.0.1:8080/echo -d 'hello world'
```

## Структура проекта

| Название файла| Функциональность|
| --- | --- |
| proxy/main.py | Запуск сервера, запуск сервера метрик |
| proxy/metrics.py| Функции подсчета метрик |
| proxy/proxy_server.py| Основной функционал proxy server|
| proxy/request_perser.py| Парсер http запросов|
| proxy/server.py| Функционал сервера, обработчик клиeнтa c поддержкой keep-alive|
| proxy/logger.py| Настройка logger|
| proxy/config.py| Чтение конфигурации|
| tests/echo_app.py| Создание echo-сервера|