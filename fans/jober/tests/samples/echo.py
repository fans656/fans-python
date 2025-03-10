import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--count', type=int, default=1)
    parser.add_argument('message', nargs='*')
    args = parser.parse_args()
    for _ in range(args.count):
        print(' '.join(args.message))
