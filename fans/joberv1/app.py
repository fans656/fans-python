"""
If you are mounting app as a sub app, you should execute `startup/shutdown` in root app event handlers:

    root_app.mount('/', app)


    @root_app.on_event('startup')
    def on_startup():
        app.state.startup()


    @root_app.on_event('shutdown')
    def on_shutdown():
        app.state.shutdown()

You can use `app.state.setup` to assign jober spec, like:

    root_app.mount('/', app.state.setup(
        spec = '/home/fans656/enos/.fme/jober/conf.yaml',
    ))
"""
import json
import asyncio

import aiofiles
from fastapi import FastAPI, HTTPException, Request, Body
from starlette.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
from fans.pubsub import pubsub
from fans.logger import get_logger

from . import errors
from .jober import Jober


logger = get_logger(__name__)
app = FastAPI()


def setup(spec):
    Jober.spec = spec
    return app
app.state.setup = setup


@app.exception_handler(errors.Error)
def handle_exception(request: Request, exc: errors.Error):
    return JSONResponse({
        'reason': exc.reason,
        'data': exc.data,
    }, status_code = exc.status_code)


@app.on_event('startup')
def on_startup():
    Jober.get_instance().start()
app.state.startup = on_startup


@app.on_event('shutdown')
def on_shutdown():
    Jober.get_instance().stop()
app.state.shutdown = on_shutdown


@app.get('/api/job/jobs')
def api_get_jobs():
    """
    Get existing jobs info.
    """
    return {
        'jobs': [
            job.info() for job in Jober.get_instance().jobs
        ],
    }


@app.get('/api/job/info')
def api_get_info(id: str = None):
    job = Jober.get_instance().get_job_by_id(id)
    if not job:
        raise errors.NotFound(f'{id} not found')
    return job.info()


@app.post('/api/job/run')
async def api_run_job(req: dict = Body(...)):
    """
    Run a job.

    Request: {
        id: str,
        args: (str|tuple)?,
    }
    """
    Jober.get_instance().run_job(
        id = req.get('id'),
        args = req.get('args'),
    )


@app.post('/api/job/stop')
async def api_stop_job(req: dict = Body(...)):
    """
    Stop a job.

    Request: {
        id: str,
        force: bool = False,
    }
    """
    Jober.get_instance().stop_job(
        id = req.get('id'),
        force = req.get('force'),
    )


@app.post('/api/job/make')
async def job_make(spec: dict = Body(...)):
    """
    Make a new job.
    """
    Jober.get_instance().make_and_add_job(spec)


@app.get('/api/job/runs')
def api_get_runs(id: str):
    job = Jober.get_instance().get_job_by_id(id)
    if not job:
        raise errors.NotFound(f'{id} not found')
    return {
        'runs': [run.info() for run in job.runs],
    }


@app.get('/api/job/logs')
async def job_logs(
    id: str,
    run_id: str = None,
    #filename: str = None,
    #head: int = None,
    limit: int = 128,
    #show_all: bool = None,
    request: Request = None,
):
    """
    Get job logs.
    """
    async def gen():
        job = Jober.get_instance().get_job_by_id(id)
        if not job:
            raise errors.NotFound(f'{id} job not found')
        if run_id:
            run = job.get_run_by_id(run_id)
        else:
            run = job.latest_run
        if not run:
            raise errors.NotFound(f'{run_id} run not found')
        out_path = run.out_path
        async with aiofiles.open(
            out_path,
            mode = 'r',
            encoding = job.encoding,
            errors = 'backslashreplace',
        ) as f:
            try:
                lines = await f.readlines()
                initial_lines = lines[-limit:]
                for line in initial_lines:
                    yield {
                        'data': line[:-1],
                    }
                while not await request.is_disconnected():
                    line = await f.readline()
                    if not line:
                        await asyncio.sleep(0.1)
                        continue
                    yield {
                        'data': line[:-1],
                    }
            except UnicodeDecodeError:
                logger.warning(f'bad log encoding')
    return EventSourceResponse(gen())


@app.get('/api/job/events')
async def api_get_events(request: Request):
    async def gen():
        async with Jober.get_instance().pubsub.subscribe().async_events as events:
            while not await request.is_disconnected():
                event = await events.get()
                yield {'data': json.dumps(event)}
    return EventSourceResponse(gen())
