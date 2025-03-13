from fastapi import FastAPI


app = FastAPI()


@app.post('/api/list-jobs')
def api_list_jobs():
    pass
