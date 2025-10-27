from fans.jober.job import Job


def test_id_and_name():
    # can have auto id and name
    job = Job()
    assert job.id and job.name
    assert job.name == job.id
    
    # can specify id
    job = Job(id='foo')
    assert job.id == job.name == 'foo'
    
    # can specify name
    job = Job(name='foo')
    assert job.id == job.name == 'foo'
    
    # can specify id and name
    job = Job(id='foo', name='bar')
    assert job.id == 'foo'
    assert job.name == 'bar'
