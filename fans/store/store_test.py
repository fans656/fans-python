from pathlib import Path

import pytest

from .store import Store, eval_lambda_field


class Test_ensure_conf:

    def test_write_default_value(self, tmp_path):
        conf_path = Path(tmp_path / 'conf.yaml')
        store = Store(conf_path)
        conf = store.ensure_conf({'foo': 3})
        assert conf == {'foo': 3}
        assert store.load() == {'foo': 3}

    def test_load_existed_value(self, tmp_path):
        conf_path = Path(tmp_path / 'conf.yaml')
        store = Store(conf_path)
        store.save({'foo': 3})
        conf = store.ensure_conf({'foo': 5})
        assert conf == {'foo': 3}
        assert store.load() == {'foo': 3}

    def test_merge_with_default_value(self, tmp_path):
        conf_path = Path(tmp_path / 'conf.yaml')
        store = Store(conf_path)
        store.save({'foo': 3})
        conf = store.ensure_conf({'bar': 5})
        assert conf == {'foo': 3, 'bar': 5}
        assert store.load() == {'foo': 3, 'bar': 5}

    def test_lambda_value(self, tmp_path):
        conf_path = Path(tmp_path / 'conf.yaml')
        store = Store(conf_path)
        store.save({'foo': 3})
        conf = store.ensure_conf({'bar': lambda: 656})
        assert conf == {'foo': 3, 'bar': 656}
        assert store.load() == {'foo': 3, 'bar': 656}

    def test_json(self, tmp_path):
        conf_path = Path(tmp_path / 'conf.json')
        store = Store(conf_path)
        store.save({'foo': 3})
        conf = store.ensure_conf({'bar': lambda: 656})
        assert conf == {'foo': 3, 'bar': 656}
        assert store.load() == {'foo': 3, 'bar': 656}

    def test_exception(self, tmp_path):
        conf_path = Path(tmp_path / 'conf.yaml')
        with conf_path.open('wb') as f:
            f.write(b'\x01')
        store = Store(conf_path)
        with pytest.raises(Exception):
            conf = store.ensure_conf()


def test_eval_lambda_field():
    assert eval_lambda_field({'foo': 3}) == {'foo': 3}

    # can convert
    data = {'foo': lambda: 3}
    ret = eval_lambda_field(data)
    assert ret == {'foo': 3}  # return value converted
    assert callable(data['foo'])  # original not changed

    # can convert nested
    data = {'foo': 3, 'bar': {'baz': lambda: 5}}
    ret = eval_lambda_field(data)
    assert ret == {'foo': 3, 'bar': {'baz': 5}}
    assert callable(data['bar']['baz'])
