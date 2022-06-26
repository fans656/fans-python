import json
from pathlib import Path

from fans.jober import Jober


def test_jober(tmpdir):
    # make default
    jober = Jober()
    assert jober.jobs == []

    # make from spec dict
    spec = {
        'jobs': [
            {'name': 'foo'},
        ],
    }
    jober = Jober(spec)
    assert len(jober.jobs) == len(spec['jobs'])

    # make from spec file
    fpath = tmpdir / 'spec.json'
    with fpath.open('w') as f:
        json.dump(spec, f)
    jober = Jober(Path(fpath))
    assert len(jober.jobs) == len(spec['jobs'])
