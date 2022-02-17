from behave import given, when, then

from raft.log import LogEntry, TransactionLog

@given('an empty raft log')
def step_impl(context):
    context.log = TransactionLog(None)

@given('a raft log with terms {term_list}')
def step_impl(context, term_list):
    context.log = TransactionLog(None)
    context.log.append_entries([
        LogEntry(term=int(t), value="initial")
        for t in term_list.split(',')
        ], 0, 0
    )

@when('adding {n} random entry with term={term} and prev_index={prev_index}')
@when('adding {n} random entries with term={term} and prev_index={prev_index}')
@when('adding {n} entry with term={term} at index={prev_index}')
def add_entry_with_term_and_index(context, n, term, prev_index):
    if type(prev_index) is str:
        prev_index = int(prev_index)

    if prev_index == 0:
        prev_term = 0
    elif prev_index > len(context.log):
        # Gap
        prev_term = context.log.previousTerm
    else:
        prev_term = context.log.get(prev_index).term

    context.last_append = context.log.append_entries(
        [LogEntry(int(term), ('ignored', 'command')) for _ in range(1, int(n) + 1)],
        prev_index, prev_term
    )

@when('adding {n} random entry with term={term}')
@when('adding {n} random entries with term={term}')
def step_impl(context, n, term):
    prev_index = context.log.lastIndex
    return add_entry_with_term_and_index(context, n, term, prev_index)

@then('appendResult == {result}')
def step_impl(context, result):
    print(context.last_append, result)
    assert context.last_append == (result == 'True')

@then('there are {n} items in the log')
def step_impl(context, n):
    print(len(context.log))
    assert len(context.log) == int(n)

@then('the log will have terms {terms}')
def step_impl(context, terms):
    for E, t in zip(context.log, terms.split(",")):
        assert int(t) == E.term

@when(u'an log entry with "{content}" is added to the cluster log')
def step_impl(context, content):
    local_server = context.leader
    assert local_server.is_leader()

    local_server.append_entry(LogEntry(
        term=local_server.currentTerm,
        value=content
    ))