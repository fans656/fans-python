import time


class Test_interval:

    def test_interval_run(self, mocker, jober):
        func = mocker.Mock()
        job = jober.add_job(func, sched=0.01)
        time.sleep(0.1)
        print(func.call_count)
