# Project 9 - System Simulation

One question about a distributed systems project is the issue of
proving that it's correct. The overall state space is gigantic and
there are many failure modes. How do you know that there isn't some
obscure corner case that has been overlooked?

One strategy for addressing this problem is to write a more
mathematically influenced simulation of the system.  This project
briefly explores that idea.

## A More Mathematical Foundation

Consider the traffic light problem.  Suppose that the state of the
system is stored in a Python dictionary like this:

```
init = {
    'ew' : 'G',
    'ns' : 'R',
    'clock' : 0,
    'ew_button' : False,
    'ns_button' : False,
}
```

Here the `ns` and `ew` fields are colors of the lights shown in
the north-south and east-west directions respectively. `clock` is the
value of the internal clock.  `ew_button` and `ns_button` track
information about the pedestian buttons.

The traffic light has certain operational rules:

1.  The East-West light (ew) stays green for 30 seconds.

2.  The North-South light (ns) stays green for 60 seconds.

3.  Yellow lights always last 5 seconds.

4.  The pedestrian buttons cause the light the change immediately as
long as 15 seconds have elapsed.  If less than 15 seconds have elapsed
the light will change once 15 seconds have elapsed.

Encode these rules into a single function called `next_state()`.  As
input, this function should accept a state dictionary and an event name. It
should return the next state (a dictionary) as a result. For example:

```
def next_state(state, event):
    ...
    return new_state
```

Here's what it might look like to use the function:

```
>>> next_state(init, 'clock')
{'ew': 'G', 'ns': 'R', 'clock': 1, 'ew_button': False, 'ns_button': False }
>>> next_state(_, 'clock')
{'ew': 'G', 'ns': 'R', 'clock': 2, 'ew_button': False, 'ns_button': False }
>>> next_state(_, 'clock')
{'ew': 'G', 'ns': 'R', 'clock': 3, 'ew_button': False, 'ns_button': False }
>>>
```

What happens if you do this?

```
>>> state = init
>>> while True:
...      print(state)
...      state = next_state(state, 'clock')
...
```

## Writing a Simulator

Using the `next_state()` function, can you write an exhaustive
simulation that explores every possible operational state of the
traffic light system?

To do this, you start at an initial state.  You then simulate every
possible event that could occur (i.e., clock tick, button press) to
get a new set of states.  You then repeat this process for each of
those states to get new states.  You keep repeating this process to
get more and more states.  Think of it as implementing an exhaustive
search of the state space.  If you were playing a game of chess, it's
like exploring every possible game configuration that you could reach
from a starting point.

In implementing your simulation, you need to keep track of the states
that you've already encountered.  Each state should only be processed
once. If you don't do this, the simulation could get stuck in an
infinite loop as it repeatedly checks states that have already been
checked.  Hint: use a set or a dict to track previous states.

## Safety

In writing such a simulator, what are you actually looking for?  For
one, a simulation could act as a kind of giant unit test that executes
every possible configuration of the system--if there were fatal flaws
in your implementation, they'd be found.

A simulation could also identify deadlock.  Deadlock is a condition
where the `next_state()` function is unable to return a new
state--meaning that you're simply stuck. For example, with a traffic
light, reaching deadlock might mean the light just freezes in some
configuration and never changes ever again.

You could also use a simulator to verify certain invariants or safety
conditions.  As an example, making sure the traffic light didn't
turn the lights green in all directions at the same time. In theory,
if you explored the entire state space, you'd find such situations.
