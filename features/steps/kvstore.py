import os, os.path
import sys

base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_base = os.path.join(base, 'src')
if src_base not in sys.path:
    sys.path.append(src_base)

import asyncio
from behave import given, when, then
from behave.api.async_step import async_run_until_complete as async_step

# Direct database components
from store import KeyValueStore

loop = asyncio.get_event_loop()

@given('an empty kv database')
def step_impl(context):
    context.kv = KeyValueStore()

@when("{key}={value} is added")
@async_step
async def step_impl(context, key, value):
    await context.kv.set(key, value)

@when('{key} is deleted from the database')
@async_step
async def step_impl(context, key):
    await context.kv.delete(key)

@then("{key} is in the database")
@async_step
async def step_impl(context, key):
    try:
        assert key in context.kv, \
            'Expected key %s is not in database' % (key,)
    except TypeError:
        assert type(context.kv) is StoreClient
        assert await context.kv.get(key) is not None, \
            'Expected key %s is not in database' % (key,)

@then("{key} is not in the database")
@async_step
async def step_impl(context, key):
    try:
        assert key not in context.kv, \
            'Key %s is unexpectedly in database' % (key,)
    except TypeError:
        assert type(context.kv) is StoreClient
        assert await context.kv.get(key) is None, \
            'Expected key %s is not in database' % (key,)

@then("the value of {key} is {value}")
def step_impl(context, key, value):
    stored = context.kv.get(key)
    assert stored == value, \
        'Database contains unexpected value %s for key %s' % (stored, key)

# Server API components
from server import kvserver
from client import StoreClient
import threading

@given('a kv server')
@async_step
async def step_impl(context):
    task = asyncio.create_task(kvserver('localhost', 10000, None),)
    context.add_cleanup(lambda: task.cancel())

@given('a kv client')
@async_step
async def step_impl(context):
    reader, writer = await asyncio.open_connection('localhost', 10000)
    context.kv = StoreClient((reader, writer))