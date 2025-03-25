import subprocess


proc = subprocess.Popen(
    'ls -lh',
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    encoding='utf-8',
    bufsize=1,  # line buffered
    errors='replace',
    shell=True,
)
while (line := proc.stdout.readline()):
    print(repr(line))
