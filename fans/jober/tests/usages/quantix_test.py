from pathlib import Path

import yaml
from fans.jober import Jober


def test_load_jobs_from_conf(tmp_path, empty_instance):
    proj_path = Path(__file__).parent.parent / 'samples/foo'
    conf_text = f'''
    jobs:
      - name: foo
        cmd: uv run -m foo "hello world"
        cwd: {proj_path}
    '''  # <--------------------- setup jobs using config file

    conf_path = Path(tmp_path / 'conf.yaml')
    with conf_path.open('w') as f:
        f.write(conf_text)

    Jober.conf['conf_path'] = conf_path  # <--- set conf path
    jober = Jober.get_instance()  # <--- get jober instance

    job = jober.get_job('foo')
    jober.run_job(job)
    job.wait()
    assert job.output == 'hello world\n'
    
    jober.stop()


def test_cron_schedule():
    pass


def test_real_time_logs():
    pass


def test_log_persistence():
    pass


def test_stop_job():
    pass


def test_single_instance_only():
    pass
