import json
import asyncio
from collections import deque
from typing import Optional, Any

from fastapi import FastAPI, Request, Response, HTTPException
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel, Field, create_model

from .jober import Jober


app = FastAPI(title='fans.jober')


def paginated_response(item_model):
    return create_model(
        'List',
        data=(list[item_model], Field()),
    )


@app.get('/jobs')
async def list_jobs_():
    """List existing jobs"""
    data = [job.as_dict() for job in Jober.get_instance().jobs]
    return {'data': data}


@app.get('/runs')
async def list_runs_(job_id: str):
    """List runs of given job"""
    job = _get_job(job_id)
    data = sorted([run.as_dict() for run in job.runs], key=lambda d: d['run_id'])
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
async def logs_(
    job_id: str,
    run_id: str = None,
    head: int = None,
    tail: int = 30,
    follow: bool = False,
    stderr: bool = False,
    until_run: bool = True,
    request: Request = ...,
):
    """Subscribe to run logs
    
    Params:
        job_id - Job's ID
        run_id - Run's ID, defaults to last run
        head - If specified, only show lines at head
        tail - Number of lines to get at tail
        follow - Whether to follow in real time
        stderr - Show stderr instead of stdout
    """
    job = _get_job(job_id)
    get_run = lambda: job.get_run(run_id) if run_id else job.last_run
    run = await _until(get_run) if until_run else get_run()
    if not run:
        raise HTTPException(404, 'no run')

    out = run.capture.err if stderr else run.capture.out

    if follow:
        async def gen():
            async with out.open_async() as f:
                if tail:
                    lines = deque([], tail)
                    async for line in out.iter_async(f, nowait=True):
                        lines.append(line)
                    for line in lines:
                        yield {'data': json.dumps({'line': line})}

                async for line in out.iter_async(f):
                    yield {'data': json.dumps({'line': line})}

        return EventSourceResponse(gen())
    else:
        if head:
            text = out.read(head=head)
        elif tail:
            text = out.read(tail=tail)
        else:
            raise HTTPException(500, 'to be implemented')
        return Response(text, media_type='text/plain')


@app.get('/events')
async def events_(request: Request):
    """Subscribe to events"""
    async def gen():
        async with Jober.get_instance().pubsub.subscribe().async_events as events:
            while not await request.is_disconnected():
                event = await events.get()
                yield {'data': json.dumps(event)}
    return EventSourceResponse(gen())


@app.post('/run-job')
async def run_job_(req: dict):
    """Run a job"""
    jober = Jober.get_instance()
    if req.get('job_id'):
        job = jober.get_job(req['job_id'])
        jober.run_job(job)
    else:
        jober.run_job(**req)


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


async def _until(pred, *, interval=0.01):
    while True:
        ret = pred()
        if ret:
            return ret
        await asyncio.sleep(interval)


root_app = FastAPI(title='fans.jober')
root_app.mount('/api', app)
