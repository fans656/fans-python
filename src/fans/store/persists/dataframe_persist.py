import fastparquet

from .utils import atomic_write


class Persist:

    @classmethod
    def save(cls, path, df, hint, **kwargs):
        kwargs.setdefault('file_scheme', 'simple')
        with atomic_write(path) as tmp_path:
            fastparquet.write(str(tmp_path), df, **kwargs)

    @classmethod
    def load(cls, path, hint, **kwargs):
        return fastparquet.ParquetFile(str(path)).to_pandas()

