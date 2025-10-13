from typing import Optional

from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel, Field, create_model

from .jober import Jober


app = FastAPI(title='fans.jober')


def paginated_response(item_model):
    return create_model(
        'List',
        data=(list[item_model], Field()),
    )


@app.get('/jobs', response_model=paginated_response(create_model('Job', **{
    'id': (str, Field()),
    'name': (Optional[str], Field(default=None)),
    'extra': (Optional[str], Field(default=None)),
})))
async def jobs_():
    """List existing jobs"""
    data = [job.as_dict() for job in Jober.get_instance().jobs]
    return {
        'data': data,
    }


@app.get('/job')
async def job_(
        job_id: str = None,
        run_id: str = None,
):
    """Get job info"""
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


@app.get('/events')
async def events_(request: Request):
    """Subscribe to events"""
    async def gen():
        async with Jober.get_instance().pubsub.subscribe().async_events as events:
            while not await request.is_disconnected():
                event = await events.get()
                yield {'data': json.dumps(event)}
    return EventSourceResponse(gen())


@app.get('/info')
async def info_():
    """Get jober info"""
    jober = Jober.get_instance()
    return jober.info


class RunJobRequest(BaseModel):
    
    job_id: str = Field()


@app.post('/run')
async def run_(req: RunJobRequest):
    """Run a job"""
    jober = Jober.get_instance()
    job = jober.get_job(req.job_id)
    jober.run_job(job)


class StopJobRequest(BaseModel):
    
    job_id: str = Field()


@app.post('/stop')
async def stop_(req: StopJobRequest):
    """Stop a job"""
    jober = Jober.get_instance()
    job = jober.get_job(req.job_id)
    # TODO


@app.post('/prune')
async def prune_():
    """Prune volatile jobs"""
    return [job.as_dict() for job in Jober.get_instance().prune_jobs()]


root_app = FastAPI(title='fans.jober')
root_app.mount('/api', app)
