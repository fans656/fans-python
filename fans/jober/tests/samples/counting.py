import time
import argparse
import itertools
from pathlib import Path


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=float, default=1)
    parser.add_argument('--stop-file')
    args = parser.parse_args()

    for i in itertools.count():
        if args.stop_file and Path(args.stop_file).exists():
            break
        print(i)
        time.sleep(args.interval)
