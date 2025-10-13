Intro
================================================================================

`fans.jober` is for managing executables as jobs.

You simply create a `Jober` instance and run job in it:

    from fans.jober import Jober

    Jober(capture=False).run_job('date').wait()

or run a callable periodically:
    
    import time
    
    def func():
        print(time.time())

    jober = Jober(capture=False)
    jober.add_job(func, when=1)  # run per second
    jober.wait()

or run a command daily (cron like):

    jober = Jober()
    jober.add_job('rsync ~/enos/ /mnt/d/backup/enos/', when='0 22 * * *')

Executables
================================================================================

Executable is job execution target, having following supported types:
- Shell command
- Python function
- Python module
- Python script

## Shell command

Use a string to represent shell command, specifying `shell=True` to run through shell:

    jober.run_job('sleep 0.01 && date', shell=True)

if not specifying `shell=True`, the string will be `shlex` split and pass to `subprocess.Popen`:

    jober.run_job('ls -lh')

equivalent to:

    jober.run_job(['ls', '-lh'])

## Python callable

Use a function or any callable:

    def func():
        print('hello')

    jober.run_job(func)

## Python module

Use module name string:

    jober.run_job('fans.jober.tests.samples.echo', args=('hello',))

Or choose a function in the module:

    jober.run_job('fans.jober.tests.samples.echo:say')

## Python script

Use script path string:

    jober.run_job('./samples/echo.py', args=('hello',))

Or choose a function in the module:

    jober.run_job('./samples/echo.py:say')

Execution environment
================================================================================

## Thread or Process

By default, jobs with callable executable will run in thread:

    jober.run_job(func)  # callable
    jober.run_job('fans.jober.tests.samples.echo:say')  # module callable
    jober.run_job('./samples/echo.py:say')  # script callable

and shell commands, python module/script will run in process:

    jober.run_job('ls -lh')  # command
    jober.run_job('fans.jober.tests.samples.echo')  # module
    jober.run_job('./samples/echo.py')  # script

You can force callable to be run in process:

    jober.run_job(func, process=True)  # callable
    jober.run_job('fans.jober.tests.samples.echo:say', process=True)  # module callable
    jober.run_job('./samples/echo.py:say', process=True)  # script callable

## Current working directory

You can set current working directory of the job if it's run in process:

    jober.run_job('ls', cwd='/tmp')

Schedule
================================================================================

There are multiple types of schedule supported:
- single shot
- interval
- cron

## Single shot

Run executable now and only once:

    jober.run_job('ls')

## Interval

Run every `<interval>` seconds:

    jober.run_job('date', when=0.5)  # run every 500ms

## Cron

    jober.run_job('date', when='0 22 * * *')  # run at 10:00 PM everyday

--------------------------------------------------------------------------------

usages
- fme
  - interval jobs
  - named
- quantix
  - module as process
  - cron jobs
  - named
  - stop
  - timezone
  - next run time
  - persisted status
- stome
  - singleshot & prunable jobs
- ocal
  - long running process job
  - keep alive
  - named
- common
  - get/list
  - remove
  - head/tail/iter output

---

interval job: no new run if previous one isn't finished?

output collection:
- collect to where?
  - no collection
  - collect into memory
  - collect into file
- separate stdout/stderr?
- note: sub-threads in run thread
- binary output?
- output encoding

schedule types:
- singleshot
- interval
- cron
- date

run mode:
- in thread pool
- separate thread
- in process pool
- separate process

subscription:
- sub to job events (start, finish, error)
- sub to run output
- head/tail run output

run operations:
- stop/kill run
- re-run
- run with different args (append? replace?)

supported source:
- callable
- py module (callable)
- py script (callable)
- command line (`shell=True/False`)

ensure no orphant processes
