import asyncio
import fcntl
import os
import sys

async def input(prompt=None):
    if prompt:
        sys.stdout.write(prompt)
        sys.stdout.flush()

    readline = asyncio.get_event_loop().create_future()
    def input_ready():
        line = sys.stdin.readline()
        if line:
            readline.set_result(line)

    try:
        loop = asyncio.get_event_loop()
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

        loop.add_reader(sys.stdin.fileno(), input_ready)
        return await readline
    finally:
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl)
        loop.remove_reader(sys.stdin.fileno())