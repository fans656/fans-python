import json
import pathlib

import pytest

from fans.jober.utils import load_spec


def test_load_spec(tmp_path):
    # default spec
    assert isinstance(load_spec(), dict)

    # load from dict
    spec = {'foo': 3}
    assert load_spec(spec) == spec

    # load from pathlib.Path
    fpath = tmp_path / 'foo.json'
    spec = {'foo': 5}
    with fpath.open('w') as f:
        json.dump(spec, f)
    loaded = load_spec(pathlib.Path(fpath))
    assert loaded == spec

    # load from str
    loaded = load_spec(str(fpath))
    assert loaded == spec

    # invalid spec
    with pytest.raises(RuntimeError):
        load_spec([])
