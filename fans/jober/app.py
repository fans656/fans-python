from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel, Field

from .jober import Jober


app = FastAPI()


@app.get('/api/jober/info')
def api_info(
        job_id: str = None,
        run_id: str = None,
):
    jober = Jober.get_instance()
    if run_id:
        pass
    elif job_id:
        job = jober.get_job(job_id)
        if not job:
            raise HTTPException(404, f'no job with id {job_id}')
        return job.as_dict()
    else:
        return jober.info


@app.get('/api/jober/listen')
async def api_listen(request: Request):
    async def gen():
        async with Jober.get_instance().pubsub.subscribe().async_events as events:
            while not await request.is_disconnected():
                event = await events.get()
                yield {'data': json.dumps(event)}
    return EventSourceResponse(gen())


@app.get('/api/jober/list')
def api_list():
    return [job.as_dict() for job in Jober.get_instance().jobs]


@app.post('/api/jober/prune')
def api_prune():
    return [job.as_dict() for job in Jober.get_instance().prune_jobs()]


class RunJobRequest(BaseModel):
    
    job_id: str = Field()


@app.post('/api/jober/run')
def api_run(req: RunJobRequest):
    jober = Jober.get_instance()
    job = jober.get_job(req.job_id)
    jober.run_job(job)


class StopJobRequest(BaseModel):
    
    job_id: str = Field()


@app.post('/api/jober/stop')
def api_stop(req: StopJobRequest):
    jober = Jober.get_instance()
    job = jober.get_job(req.job_id)
    # TODO
