import asyncio
from behave.api.async_step import AsyncContext
import logging
import os, os.path
import sys

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_base = os.path.join(base, 'src')
if src_base not in sys.path:
    sys.path.append(src_base)

def after_scenario(context, scenario):
    loop = asyncio.get_event_loop()
    if hasattr(context, 'servers'):
        for S in context.servers:
            S.shutdown()

    try:
        # run_forever() returns after calling loop.stop()
        tasks = asyncio.all_tasks()
        for t in tasks:
            if not t.done():
                t.cancel()
            if t.cancelled():
                # give canceled tasks the last chance to run
                loop.run_until_complete(t)
    except RuntimeError:
        pass

import uvloop
uvloop.install()