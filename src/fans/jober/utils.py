import json

from fans.path import Path


def load_spec(spec):
    if spec is None:
        return make_empty_spec()
    if isinstance(spec, dict):
        return spec
    if isinstance(spec, Path):
        return load_spec_from_file_path(spec)
    if isinstance(spec, str):
        return load_spec_from_file_path(Path(spec))
    raise RuntimeError(f'invalid spec: {spec}')


def load_spec_from_file_path(path):
    try:
        return path.load()
    except Exception as e:
        raise RuntimeError(f'error loading spec from {spec_path}: {e}')


def make_empty_spec():
    return {
        'jobs': [],
    }
