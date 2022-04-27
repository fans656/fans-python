#!/usr/bin/env python3
import os
import shutil
from pathlib import Path


if __name__ == '__main__':
    if Path('./dist').exists():
        shutil.rmtree('./dist')
    os.system('python3 -m build')
    os.system('python3 -m twine upload dist/*')
