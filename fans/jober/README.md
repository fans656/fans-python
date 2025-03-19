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
