import { useState, useEffect } from 'react'

import * as api from 'src/api';
import { useInitializeGlobals } from 'src/globals';
import { useQuery, setQuery } from 'src/utils';
import List from 'src/comps/list';

export default function App() {
  useInitializeGlobals();

  const [currentJob, set_currentJob] = useState(null);
  const [currentRun, set_currentRun] = useState(null);

  const jobs = useJobs();
  const runs = useRuns(currentJob);

  return (
    <div className="padding horz space flex-1">
      <JobsList jobs={jobs} set_currentJob={set_currentJob}/>
      <div className="flex-1" style={{marginLeft: '1em'}}>
        {currentJob ? (
          <JobDetail
            job={currentJob}
            run={currentRun}
            runs={runs}
            set_currentRun={set_currentRun}
          />
        ) : (
          <div>dashboard</div>
        )}
      </div>
    </div>
  );
}

function JobsList({jobs, currentJob, set_currentJob}) {
  return (
    <List
      domain="job"
      style={{minWidth: '30em'}}
      items={jobs}
      render={job => <JobListItem job={job}/>}
      onSelected={set_currentJob}
    />
  );
}

function JobListItem({job}) {
  return (
    <div>
      <div>{job.name}</div>
      <div className="gray small mono">{job.id}</div>
    </div>
  );
}

function getJobListItemActions(job) {
  return [
    <a>Run</a>,
    <a>Stop</a>,
  ];
}

function JobDetail({job, run, runs, set_currentRun}) {
  return (
    <div className="flex-1 vert margin" style={{minHeight: '100%'}}>
      <div className="horz margin center">
        <h3>{job.name}</h3>
        <div className="horz xs-margin small">
          <div>ID: {job.id}</div>
        </div>
      </div>
      <div className="small mono">
        <pre><code>{JSON.stringify(job, null, 2)}</code></pre>
      </div>
      <div className="horz margin flex-1">
        <RunOutput job={job} run={run || runs[0]}/>
        <RunsList
          runs={runs}
          set_currentRun={set_currentRun}
        />
      </div>
    </div>
  );
}

function RunOutput({job, run}) {
  const [text, set_text] = useState('');
  useEffect(() => {
    if (job && run) {
      (async () => {
        const text = await api.get('/api/logs', {
          job_id: job.id,
          run_id: run.run_id,
        }, {raw: true});
        set_text(text);
      })();
    }
  }, [run && run.run_id]);
  return (
    <div
      className="flex-1 small mono"
      style={{
        border: '1px solid #ccc',
        padding: '.5em',
      }}
    >
      <pre style={{margin: 0, padding: 0}}>{text}</pre>
    </div>
  );
}

function RunsList({runs=[], set_currentRun}) {
  return (
    <List
      domain="run"
      style={{minWidth: '20em'}}
      items={runs}
      render={run => <RunListItem run={run}/>}
      onSelected={set_currentRun}
      getItemId={d => d.run_id}
    />
  );
}

function RunListItem({run}) {
  return (
    <div>
      <div>{run.run_id}</div>
    </div>
  );
}

function useJobs() {
  const [jobs, set_jobs] = useState([]);
  useEffect(() => {
    (async () => {
      const res = await api.get('/api/jobs');
      set_jobs(res.data);
    })();
  }, []);
  return jobs;
}

function useRuns(job) {
  const [runs, set_runs] = useState([]);
  useEffect(() => {
    if (job && job.id) {
      (async () => {
        const res = await api.get('/api/runs', {job_id: job.id});
        console.log('runs', res);
        set_runs(res.data.reverse());
      })();
    }
  }, [job && job.id]);
  return runs;
}
