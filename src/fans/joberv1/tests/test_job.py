from fans.jober.job import Job


def test_job():
    job = Job({})


def test_executable_job():
    job = Job(cmd = 'ls -lh')
    job()


def test_job_status():
    job = Job(cmd = 'ls')
    assert job.status == 'ready'
    job()
    assert job.status == 'done'


def test_clear_old_runs(tmp_path):
    # with limit
    job = Job(cmd = 'ls', config = {
        'limit.archived.runs': 1,
        'runs_dir': str(tmp_path),
    })
    job()
    assert len(list(job.root_dir.iterdir())) == 1
    job()
    assert len(list(job.root_dir.iterdir())) == 1

    # without limit
    job = Job(cmd = 'ls', config = {
        'runs_dir': str(tmp_path),
    })
    for count in range(1, 10):
        job()
        assert len(list(job.root_dir.iterdir())) == count
