from fans.progress import progress


class Test_file_progress:

    def test_default(self, tmp_path):
        fpath = tmp_path / 'data'
        with fpath.open('wb') as f:
            f.write(b'0' * 1024)

        # usage
        with fpath.open('rb') as f, progress(fpath) as prog:
            while chunk := f.read(100):
                p = prog(len(chunk))
                print(f'{p:6.2f}%')
                assert p > 0.0
            assert p == 100.0
