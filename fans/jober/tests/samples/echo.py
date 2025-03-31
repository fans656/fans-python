import os
import argparse


def say(what: str = 'hi'):
    print(what)


def echo(
        message: str = '',
        count: int = 1,
        file: str = None,
        show_pid: bool = False,
):
    if file:
        file = open(file, 'w')
    
    if show_pid:
        print(os.getpid(), file=file)

    for _ in range(count):
        print(message, file=file)
    
    if file:
        file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--count', type=int, default=1)
    parser.add_argument('-f', '--file', default=None)
    parser.add_argument('message')
    args = parser.parse_args()

    echo(args.message, count=args.count, file=args.file)
