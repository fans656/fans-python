import json

import yaml
from fans.path import Path
from fans.path.meta import Meta


class Test_Meta:

    def test_load(self, tmp_path):
        fpath = Path(tmp_path / 'meta.json')
        with fpath.open('w') as f:
            json.dump({'foo': 3, 'bar': 5}, f)

        assert Meta(fpath) == {'foo': 3, 'bar': 5}

    def test_default(self, tmp_path):
        path = Path(tmp_path / 'meta.json')

        # no default
        assert path.as_meta() == {}

        # value default
        assert path.as_meta({'foo': 3}) == {'foo': 3}

        # lambda default
        assert path.as_meta(lambda: {'foo': 3}) == {'foo': 3}

    def test_save(self, tmp_path):
        path = Path(tmp_path / 'meta.json')
        meta = path.as_meta({'foo': 3})

        assert not path.exists()

        meta.save()
        assert path.exists()
        assert json.load(path.open()) == {'foo': 3}

    def test_yaml(self, tmp_path):
        fpath = Path(tmp_path / 'meta.yaml')
        with fpath.open('w') as f:
            yaml.dump({'foo': 3, 'bar': 5}, f)

        assert Meta(fpath) == {'foo': 3, 'bar': 5}
