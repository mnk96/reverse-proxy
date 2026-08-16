import json

from fastapi import FastAPI, Request
from fastapi.responses import Response

app = FastAPI()


@app.get('/')
async def home(request: Request) -> Response:
    response_data = {
            'method': request.method,
            'path': request.url.path,
            'query_params': dict(request.query_params),
            'headers': dict(request.headers)
        }
    json_str = json.dumps(response_data)
    json_bytes = json_str.encode('utf-8')
    http_response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(json_bytes)).encode() + b"\r\n"
        b"Connection: close\r\n"
        b"\r\n\r\n"
        + json_bytes
    )
    
    return Response(
        content=http_response,
        media_type="application/json"
    )


@app.get('/{parametr}')
async def get_parametr(parametr: str, request: Request) -> Response:
    response_data = {
                'method': request.method,
                'path': request.url.path,
                'parametr': parametr,
                'query_params': dict(request.query_params),
                'headers': dict(request.headers)
            }
    json_str = json.dumps(response_data)
    json_bytes = json_str.encode('utf-8')
    http_response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(json_bytes)).encode() + b"\r\n"
        b"Connection: close\r\n"
        b"\r\n\r\n"
        + json_bytes
    )
    
    return Response(
        content=http_response,
        media_type="application/json"
    )

@app.post('/echo')
async def get_post_info(request: Request) -> Response:
    # Получаем тело
    body = await request.body()
    
    # Формируем ответ с полной информацией о запросе
    response_data = {
        'method': request.method,
        'path': request.url.path,
        'query_params': dict(request.query_params),
        'headers': dict(request.headers),
        'body': body.decode('utf-8', errors='ignore')
    }
    
    json_str = json.dumps(response_data, indent=2, ensure_ascii=False)
    json_bytes = json_str.encode('utf-8')

    http_response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(json_bytes)).encode() + b"\r\n"
        b"Connection: close\r\n"
        b"\r\n\r\n"
        + json_bytes
    )
    
    return Response(
        content=http_response,
        media_type="application/json",
        status_code=200
    )


@app.delete('/echo')
async def get_delete_info(request: Request) -> Response:
    # Получаем тело
    body = await request.body()
    
    # Формируем ответ с полной информацией о запросе
    response_data = {
        'method': request.method,
        'path': request.url.path,
        'query_params': dict(request.query_params),
        'headers': dict(request.headers),
        'body': body.decode('utf-8', errors='ignore')
    }
    
    json_str = json.dumps(response_data, indent=2, ensure_ascii=False)
    json_bytes = json_str.encode('utf-8')

    http_response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(json_bytes)).encode() + b"\r\n"
        b"Connection: close\r\n"
        b"\r\n\r\n"
        + json_bytes
    )
    
    return Response(
        content=http_response,
        media_type="application/json",
        status_code=200
    )