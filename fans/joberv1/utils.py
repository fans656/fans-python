import json
import pathlib
from typing import Optional, Union

from fans.path import Path


def load_spec(spec: Optional[Union[dict, str, pathlib.Path]] = None):
    """
    Load specification from file/dict.
    """
    if spec is None:
        return make_empty_spec()
    if isinstance(spec, dict):
        return spec
    if isinstance(spec, pathlib.Path):
        return load_spec_from_file_path(spec)
    if isinstance(spec, str):
        return load_spec_from_file_path(Path(spec))
    raise RuntimeError(f'invalid spec: {spec}')


def load_spec_from_file_path(path: Union[pathlib.Path, Path]):
    try:
        return Path(path).load()
    except Exception as e:
        raise RuntimeError(f'error loading spec from {path}: {e}')


def make_empty_spec():
    return {
        'jobs': [],
    }
