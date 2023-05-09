import asyncio
from behave import given, then, when
from behave.api.async_step import async_run_until_complete as async_step

from raft.role import CandidateRole

@when('node {n} becomes a candidate for term {term}')
@when('node {n} becomes a candidate')
@async_step
async def step_impl(context, n, term=None):
    n = int(n)
    if term is not None:
        context.servers[n].config.currentTerm = int(term)
    await context.servers[n].switch_role(CandidateRole)


@then('node {n} is_leader == {is_leader}')
def is_maybe_leader(context, n, is_leader):
    n = int(n)
    if type(is_leader) is str:
        is_leader = is_leader.upper() == 'TRUE'
    assert context.servers[n].is_leader() == is_leader

@then('node {n} is the leader')
def step_impl(context, n):
    return is_maybe_leader(context, n, True)

@then('node {n} is not the leader')
def step_impl(context, n):
    return is_maybe_leader(context, n, False)

@then('the cluster will have a leader')
@async_step
async def step_impl(context):
    for _ in range(40):
        await asyncio.wait([
            wait_condition(server.role_changed)
            for server in context.servers
        ], return_when=asyncio.FIRST_COMPLETED, timeout=0.05)

        for server in context.servers:
            if server.is_leader():
                return

    assert False, "Cluster did not elect a leader"

async def wait_condition(c):
    async with c:
        return await c.wait()

@then('the cluster will have {n} followers')
@async_step
async def step_impl(context, n):
    for _ in range(40):
        await asyncio.wait([
            wait_condition(server.role_changed)
            for server in context.servers
        ], return_when=asyncio.FIRST_COMPLETED, timeout=0.05)

        n_followers = 0
        for server in context.servers:
            if server.is_follower():
                n_followers += 1

        if n_followers == int(n):
            return

    assert False, f"Cluster does not have {n} followers: has {n_followers}"
