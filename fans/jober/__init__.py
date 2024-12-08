"""
Instantiate <Jober>:

    jober = Jober()

Create <Job>:

    jober.run_job() -> Job
    jober.add_job() -> Job
    jober.make_job() -> Job

Schedule <Job>:

    job.schedule()

Query <Jober>:

    .get_jobs() -> List[Job] - get a list of known jobs
    .get_job(id) -> Job - get job by id

Query <Job>:

    .last_run: Run - Get last run instance of this job
    .runs: List[Run] - Get a list of known runs of this job

    # following attributes delegate to job's last run
    .status
    .finished
    .async_iter_output -> ...

Query <Run>:

    .status: str - Current status of job's one run
        'ready'     - not run yet
        'running'   - is running
        'done'      - finished with success
        'error'     - finished with error

    .finished: bool - Whether job run finished (with success or error)

    .async_iter_output -> ... - Get an async generator to iter job's output

    .get_output() -> str - Get job's output

Example jobs:

    - ocal (quick-console, switcha, stome)
    - fme (quantix.pricer, enos.backup)
    - stome (thumbnail.generate:ffmpeg, search)
"""
from .jober import Jober
