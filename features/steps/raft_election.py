import asyncio
from behave import given, then, when
from behave.api.async_step import async_run_until_complete as async_step

from raft.role import CandidateRole

@when('node {n} becomes a candidate for term {term}')
@async_step
async def step_impl(context, n, term):
    n = int(n)
    context.servers[n].config.currentTerm = int(term) - 1
    await context.servers[n].switch_role(CandidateRole)


@then('node {n} is_leader == {bool}')
def step_impl(context, n, bool):
    n = int(n)
    assert context.servers[n].is_leader() == (bool == 'True')