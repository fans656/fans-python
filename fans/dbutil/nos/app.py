from fastapi import FastAPI

from .router import app as router


app = FastAPI()
app.include_router(router)
