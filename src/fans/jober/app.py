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
        spec = '/home/fans656/.fme/jober/conf.yaml',
    ))
"""
import json

from fastapi import FastAPI, HTTPException, Request, Body
from starlette.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
from fans.pubsub import pubsub

from . import errors
from .jober import Jober


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
def api_get_jobs(latest_run: bool = True):
    """
    Get existing jobs info.
    """
    return {
        'jobs': [
            job.info(
                latest_run = latest_run,
            ) for job in Jober.get_instance().jobs
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


@app.post('/api/job/make')
async def job_make(spec: dict = Body(...)):
    """
    Make a new job.
    """
    Jober.get_instance().make_and_add_job(spec)


@app.get('/api/job/logs')
async def job_logs(
    job_name: str,
    run_name: str = None,
    filename: str = None,
    head: int = None,
    tail: int = 10,
    show_all: bool = None,
    request: Request = None,
):
    """
    Get job logs.
    """
    pass


@app.get('/api/job/stop')
async def job_stop(job_name: str):
    """
    Stop a running job.
    """
    pass


@app.get('/api/job/prune')
async def job_prune(job_name: str = None, prune_all: bool = False):
    """
    Prune job's historical runs.
    """
    pass


@app.get('/api/job/events')
async def api_get_events(request: Request):
    async def gen():
        with await Jober.get_instance().pubsub.subscribe_async() as events:
            while not await request.is_disconnected():
                event = await events.get_async()
                yield {'data': json.dumps(event)}
    return EventSourceResponse(gen())
