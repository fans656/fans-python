#! /usr/bin/env python3
print('1')
import os
import sys
import json

import requests
from sseclient import SSEClient
print('2')


def get_cwd():
    cwd = os.getcwd()
    if cwd.startswith(c_prefix):
        return f'C:/{cwd[len(c_prefix):]}'
    elif cwd.startswith(d_prefix):
        return f'D:/{cwd[len(c_prefix):]}'
    else:
        return cwd


c_prefix = '/mnt/c/'
d_prefix = '/mnt/d/'


if __name__ == '__main__':
    try:
        host_ip = os.environ.get('HOST')
        host_port = 6570
        host_prefix = f'http://{host_ip}:{host_port}'

        args = sys.argv[1:]
        encoding = 'utf-8'
        if args:
            match args[0]:
                case '-u':
                    encoding = 'utf-8'
                    args = args[1:]
                case '-g':
                    encoding = 'gbk'
                    args = args[1:]

        events = SSEClient(f'{host_prefix}/api/run-command', json = {
            'command': args,
            'cwd': get_cwd(),
            'encoding': encoding,
        })
        for event in events:
            if event.event == 'ping':
                continue
            try:
                data = json.loads(event.data)
            except:
                print(f'ERROR parse event data: {event.event} {repr(event.data)}')
            else:
                event_type = data.get('event')
                if event_type == 'output':
                    print(data['content'].rstrip())
                elif event_type == 'done':
                    break
                elif event_type == 'trace':
                    print(data['content'].rstrip())
                    break
                else:
                    print(f'unknown event: {data}')
    except KeyboardInterrupt:
        pass
