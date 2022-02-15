import os, os.path
import sys

base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_base = os.path.join(base, 'src')
if src_base not in sys.path:
    sys.path.append(src_base)

import asyncio
from behave import given, when, then
from behave.api.async_step import async_run_until_complete as async_step

from raft.log import LogEntry, TransactionLog

@given(u'an empty raft log')
def step_impl(context):
    context.log = TransactionLog(None)

@when('adding {n} random entry with term={term} and prev_index={prev_index}')
def add_entry_with_term_and_index(context, n, term, prev_index):
    prev_term = context.log.previousTerm
    if type(prev_index) is str:
        prev_index = int(prev_index)

    context.last_append = context.log.append_entries(
        [LogEntry(int(term), ('ignored', 'command')) for _ in range(1, int(n) + 1)],
        prev_index, prev_term
    )

@when('adding {n} random entry with term={term}')
def step_impl(context, n, term):
    prev_index = context.log.previousIndex
    return add_entry_with_term_and_index(context, n, term, prev_index)

@then('lastApplied == {state}')
def step_impl(context, state):
    print(context.log.lastApplied)
    assert context.log.lastApplied == int(state)

@then('commitIndex == {state}')
def step_impl(context, state):
    assert context.log.commitIndex == int(state)

@then('appendResult == {result}')
def step_impl(context, result):
    print(context.last_append, result)
    assert context.last_append == (result == 'True')

@then('there are {n} items in the log')
def step_impl(context, n):
    assert len(context.log) == int(n)