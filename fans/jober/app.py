from fastapi import FastAPI

from .router import router


app = FastAPI(
    title='fans.jober',
    docs_url='/api',
    redoc_url='/api/redoc',
    openapi_url='/api/openapi.json',
)
app.include_router(router, prefix='/api')
