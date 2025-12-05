from fastapi import APIRouter


app = APIRouter()


@app.get('/api/nos/info')
def info():
    pass
