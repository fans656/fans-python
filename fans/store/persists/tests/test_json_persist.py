import json
from pathlib import Path

import pytest

from ..json_persist import Persist


class Test_json_persist:

    def test_atomic_write(self, mocker, tmpdir):
        tmpdir = Path(tmpdir)
        persist = Persist()
        path = tmpdir / 'foo.json'
        hint = {'tmpdir': tmpdir}

        persist.save(path, {'foo': 3}, hint = hint)
        assert json.load(path.open()) == {'foo': 3}

        def mocked_json_dump(data, f, **_):
            f.write('!')
            raise Exception('oops')
        orig_json_dump = json.dump
        json.dump = mocked_json_dump

        with pytest.raises(Exception):
            persist.save(path, {'foo': 5}, hint = hint)
        assert json.load(path.open()) == {'foo': 3}

        json.dump = orig_json_dump
        persist.save(path, {'foo': 5}, hint = hint)
        assert json.load(path.open()) == {'foo': 5}
