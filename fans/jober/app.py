from typing import Optional, Any

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


@app.get('/list-jobs', response_model=paginated_response(create_model('Job', **{
    'id': (str, Field()),
    'name': (Optional[str], Field(default=None)),
    'extra': (Optional[Any], Field(default=None)),
})))
async def list_jobs_():
    """List existing jobs"""
    data = [job.as_dict() for job in Jober.get_instance().jobs]
    return {'data': data}


@app.get('/list-runs')
async def list_runs_(job_id: str):
    """List runs of given job"""
    job = _get_job(job_id)
    data = [run.as_dict() for run in job.runs]
    return {'data': data}


@app.get('/get-job')
async def get_job(job_id: str):
    """Get job info"""
    return _get_job(job_id).as_dict()


@app.get('/get-run')
async def get_run(run_id: str):
    """Get run info"""
    pass


@app.get('/get-jober')
async def get_jober_():
    """Get jober info"""
    return Jober.get_instance().as_dict()


@app.get('/logs')
async def logs_(request: Request):
    """Subscribe to run logs"""
    pass


@app.get('/events')
async def events_(request: Request):
    """Subscribe to events"""
    async def gen():
        async with Jober.get_instance().pubsub.subscribe().async_events as events:
            while not await request.is_disconnected():
                event = await events.get()
                yield {'data': json.dumps(event)}
    return EventSourceResponse(gen())


class RunJobRequest(BaseModel):
    
    job_id: str = Field()


@app.post('/run-job')
async def run_job_(req: RunJobRequest):
    """Run a job"""
    jober = Jober.get_instance()
    job = jober.get_job(req.job_id)
    jober.run_job(job)


class StopJobRequest(BaseModel):
    
    job_id: str = Field()


@app.post('/stop-job')
async def stop_job_(req: StopJobRequest):
    """Stop a job"""
    jober = Jober.get_instance()
    job = jober.get_job(req.job_id)
    # TODO


@app.post('/prune-jobs')
async def prune_jobs_():
    """Prune volatile jobs"""
    return [job.as_dict() for job in Jober.get_instance().prune_jobs()]


def _get_job(job_id: str):
    job = Jober.get_instance().get_job(job_id)
    if not job:
        raise HTTPException(404, f'no job with id {job_id}')
    return job


root_app = FastAPI(title='fans.jober')
root_app.mount('/api', app)
