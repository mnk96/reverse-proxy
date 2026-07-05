from fastapi import FastAPI, Request

app = FastAPI()

@app.get('/')
async def home():
    return {'GET': 'GET request'}

@app.get('/{parametr}')
async def get_parametr(parametr):
    return {'GET': f'GET request {parametr}'}

@app.post('/echo')
async def get_post_info(request: Request):
    body = await request.body()
    info = body.decode('utf-8')
    return {'POST': info}