from fans.jober.job import Job


def test_job():
    job = Job({})


def test_executable_job():
    job = Job({'cmd': 'ls -lh'})
    job()
    assert False
