import re
import argparse
import subprocess
from pathlib import Path
from collections import defaultdict


KB = 1024
MB = 1024 * KB
GB = 1024 * MB


def list_files(root, suffix: str, recursive: bool = False):
    ret = []
    for path in root.iterdir():
        if path.is_dir():
            if recursive:
                ret.extend(list_files(path, suffix=suffix, recursive=recursive))
        elif path.suffix == suffix:
            ret.append(path)
    return ret


def compress_dir(root, suffix: str, recursive: bool):
    for path in root.iterdir():
        if path.is_dir():
            if recursive:
                compress_dir(path)
            else:
                print(f'skipping dir {path}')
        elif path.suffix == suffix:
            compress_file(path)
        else:
            print(f'skipping file {path} with wrong suffix')


def compress_file(path):
    if path.suffix == '.mp4':
        return
    compressed_path = path.with_suffix('.mp4')
    if compressed_path.exists():
        print(f'skip {path} -> {compressed_path}')
        return

    print(f'compressing {path}')
    proc = subprocess.Popen(
        [
            'C:/ProgramData/chocolatey/lib/ffmpeg/tools/ffmpeg/bin/ffmpeg.exe',
            '-i', path,
            '-vcodec', 'libx265',
            '-crf', '28',
            compressed_path,
        ],
        #stdout=subprocess.PIPE,
        #stderr=subprocess.PIPE,
    )
    proc.wait()


def merge_parts(root, suffix: str):
    date_to_paths = defaultdict(lambda: [])
    for path in root.glob(f'*{suffix}'):
        m = re.match(r'(?P<date>\d{8})_.*', path.name)
        if m:
            date = m.group('date')
            date_to_paths[date].append(path)

    concated_dir = root / 'concated'
    if not concated_dir.exists():
        concated_dir.mkdir()

    for date, paths in date_to_paths.items():
        dst_path = (concated_dir / date).with_suffix(suffix)
        if dst_path.exists():
            print(f'skip {dst_path} as already exists')
            continue
        try:
            files = Path('files.txt')
            with files.open('w') as f:
                for path in paths:
                    f.write(f"file '{path}'\n")
            'ffmpeg -safe 0 -f concat -i list.txt -c copy output.mp4'
            cmd = [
                'C:/ProgramData/chocolatey/lib/ffmpeg/tools/ffmpeg/bin/ffmpeg.exe',
                '-safe', '0',
                '-f', 'concat',
                '-i', f'{files}',
                '-c', 'copy',
                dst_path,
            ]
            proc = subprocess.Popen(cmd)
            proc.wait()
        finally:
            files.unlink()


def move_concated(root, suffix: str):
    concated_dir = root / 'concated'
    if not concated_dir.exists():
        concated_dir.mkdir()
    for path in root.glob(f'*{suffix}'):
        if re.match(r'\d{8}\.mp4', path.name):
            dst_path = concated_dir / path.name
            path.rename(dst_path)
            print(f'move {path} to {dst_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', nargs='?', help='Sub command (info)')
    parser.add_argument('-d', '--directory', default='./', help='Directory containing videos')
    parser.add_argument('-s', '--suffix', default='.mp', help='Suffix of source video files')
    parser.add_argument('--compress-suffix', default='.mp4', help='Suffix of compressed video files')
    parser.add_argument('-r', '--recursive', action='store_true', default=False, help='Recursively process sub-directories')
    args = parser.parse_args()

    root = Path(args.directory)
    if not root.exists():
        print(f'[ERROR] {root} does not exists')
        exit(1)

    cmd = args.cmd
    if not cmd:
        print('-' * 80, 'compress_dir')
        compress_dir(root, suffix=args.suffix, recursive=args.recursive)
        print('-' * 80, 'merge_parts')
        merge_parts(root, suffix=args.compress_suffix)
        print('-' * 80, 'move_concated')
        move_concated(root, suffix=args.compress_suffix)

    elif cmd in ('info',):
        source_paths = list_files(root, suffix=args.suffix)
        source_size = sum(Path(d).stat().st_size for d in source_paths)

        compressed_paths = list_files(root, suffix=args.compress_suffix)
        compressed_size = sum(Path(d).stat().st_size for d in compressed_paths)

        print('Source files:')
        for path in source_paths:
            print(f'    {path}')

        print(
            f'Source size: {source_size / GB:.2f}GB '
            f'| Compressed size: {compressed_size / GB:.2f}GB'
            f'| Compression ratio: {compressed_size / source_size * 100:.2f}%'
        )
        print(f'Progress: {len(compressed_paths) / len(source_paths) * 100:.2f}%')
