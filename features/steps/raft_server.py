import asyncio
from behave import given, then, when
from behave.api.async_step import async_run_until_complete as async_step

from raft.server import LocalServer

import logging
log = logging.getLogger('behave.steps')

@given('a raft server')
@given('a raft server with id {id}')
def step_impl(context, id=0, port=10000):
    context.server = LocalServer(int(id), ('localhost', port), cluster=None)

@when('voting for node {id} for term {term}')
@then('voting for node {id} for term {term} will {succeed_fail}')
def step_impl(context, id, term, succeed_fail='succeed'):
    try:
        context.server.voteFor(int(term), int(id))
        succeeded = True
    except AssertionError:
        log.exception("Got an error")
        succeeded = False

    assert (succeed_fail == 'succeed') == succeeded