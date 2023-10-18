import asyncio
from behave import given, then, when
from behave.api.async_step import async_run_until_complete as async_step
from behave.api.async_step import use_or_create_async_context
from itertools import count
import random

from raft.cluster import Cluster
from raft.config import ClusterConfig
from raft.log import LogEntry

@given('a cluster of {n} nodes')
@async_step
async def step_impl(context, n):
    id = count(random.randint(1, 10000000))
    port = count(10000)
    nodes = [
        {
            "id": next(id),
            "port": next(port),
            "listen": "::1",
            "hostname": "::1"
        }
        for _ in range(int(n))
    ]
    config = {
        "election_timeout": 0.4,
        "broadcast_timeout": 0.06,
        "nodes": nodes,
    }

    # The same configuration from the perspective of each of the nodes
    context.configs = [
        ClusterConfig.from_json(x['id'], config)
        for x in nodes
    ]

    # The same cluster from the perspective of each of the nodes
    context.clusters = [
        Cluster(config)
        for config in context.configs
    ]

    # Start all the servers
    context.servers = [
        cluster.local_server
        for cluster in context.clusters
    ]

    for server in context.servers:
        await server.start()

def shutdown_clusters(context):
    if hasattr(context, 'servers'):
        for S in context.servers:
            S.shutdown()

@given('a leader')
@async_step
async def step_impl(context):
    await context.servers[0].become_leader()
    assert context.servers[0].is_leader()
    context.leader = context.servers[0]

@given('the following log states')
@async_step
async def step_impl(context):
    for node, log in enumerate(context.text.split("\n")):
        for idx, term in enumerate(log.split(",")):
            context.servers[node].log.append(LogEntry(int(term), f'junk{idx}'))

        assert len(context.servers[node].log) == len(list(log.split(",")))

@when('node {n} becomes the leader for term {term}')
@async_step
async def step_impl(context, n, term):
    server = context.servers[int(n)]
    await server.become_leader()
    assert server.is_leader()
    server.config.currentTerm = int(term)
    context.leader = server

@when('the leader has replicated its logs to all nodes')
@async_step
async def step_impl(context):
    bt = context.clusters[0].config.broadcast_timeout
    for _ in range(20):
        await asyncio.sleep(bt)
        log_len = [len(server.log) for server in context.servers]
        if min(log_len) == max(log_len):
            break
    else:
        assert False

@when('node {n} is restarted')
@async_step
async def step_impl(context, n):
    node = context.servers[int(n)]
    node.shutdown()
    node.log.purge()
    await node.start()

@then('the last entry in all nodes is ({term}, {index})')
@async_step
async def step_impl(context, term, index):
    await asyncio.sleep(0.06)
    for server in context.servers:
        entry = server.log.lastEntry
        assert entry
        print(term, server.log)
        assert entry.term == int(term)
        assert server.log.lastIndex == int(index)

@then('all nodes will have log with terms "{terms}"')
def step_impl(context, terms):
    terms = [int(n) for n in terms.split(",")]
    for server in context.servers:
        print(server.log, terms)
        for term, entry in zip(terms, server.log):
            assert term == entry.term
        assert len(server.log) == len(terms) 
