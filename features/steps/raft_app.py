from behave import then

@then('the leaderCommitId will be {n}')
def step_impl(context, n):
    leader = context.leader
    assert leader.is_leader()
    print(leader.commitIndex)
    assert leader.commitIndex == int(n)