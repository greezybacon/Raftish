# Project 4 - The Log

The most important part of Raft is the transaction log.  In fact, the
whole point of the algorithm is to implement a distributed replicated
transaction log!  Everything is ultimately about the log.  No log, no
Raft.

In this project, you are going to implement the log as a completely
stand-alone function or object.  The goal is to have no dependencies
on the network, the system, or any other part of Raft.  The primary
reason for doing this is testing and understanding.  It is absolutely
essential that the log gets implemented correctly.  If there are any
bugs in it, you will be chasing them through the 9th inner circle of
debugging hell if you're trying to figure out what's wrong when it's
combined with all of the networking and concurrency code later.

## Background Reading

The behavior of the log is described in section 5.3 and 5.4 of the
[Raft Paper](https://raft.github.io/raft.pdf). At first read, it's not
going to entirely make sense, but give it another read and proceed.

## The Project

In a nutshell, we're going to make a function `append_entries()` that
implements the most central operation of Raft.  The function has the
following signature:

```
# Each log entry consists of a term number and an item
class LogEntry:
    def __init__(self, term, item):
        self.term = term
        self.item = item

# Implementation of "AppendEntries"
def append_entries(log, prev_index, prev_term, entries):
    ...
    return success     # A boolean
```

The `append_entries()` function adds one or more entries to the log
and returns a True/False value to indicate success.  The `log` is a
list-like object that holds instances of `LogEntry`.
The `prev_index` argument specifies the position in
the log after which the new entries go (e.g., specifying
`prev_index=8` means that entries are being appended starting at index
9). The `prev_term` argument specifies the `term` value of the log
entry at position `prev_index`. `entries` is a list of zero or more
`LogEntry` instances that are being added.

There are a number of very tricky edge cases in the log implementation
that need to be accounted for:

1. The log is never allowed to have holes in it.  For example, if
there are currently 5 entries in the log, and `append_entries()` tries
to add new data at index 9, then the operation fails (return `False`).

2. There is a log-continuity condition where every append operation
must verify that the term number of any previous entry matches an
expected value. For example, if appending at `prev_index` 8, the
`prev_term` value must match the value of `log[prev_index].term`. If
there is a mismatch, the operation fails (return `False`).

3. Special case: Appending log entries at index 0 has to work. That's
the start of the log and there are no prior entries.  Note: the
Raft paper uses 1-based indexing.  If you are going to follow that
convention, then adjust accordingly.

4. `append_entries()` is "idempotent."  That means that
`append_entries()` can be called repeatedly with the same arguments
and the end result is always the same.  For example, if you called
`append_entries()` twice in a row to add the same entry at index 10,
it just puts the entry at index 10 and does not result in any data
duplication or corruption.

5. Calling `append_entries()` with an empty list of entries is
allowed.  In this case, it should report `True` or `False` to indicate
if it would have been legal to add new entries at the specified
position.
 
6. If there are already existing entries at the specified log position,
but those entries are from an earlier term, the existing entries and
everything that follows are deleted.  The new entries are then
added in their place.  Ponder: What happens if there are existing
entries from the current term?  What happens if there are existing
entries from a later term?

## Testing

Of particular interest to this project is the problem of testing.
You should write unit tests that test the various edge cases
of log behavior.

Figure 7 of the [Raft Paper](https://raft.github.io/raft.pdf) might
also be of interest.  This figure shows different possible log
configurations in relation to the leader.  You might use this
as the basis of the following test:

```
# Append entry from term=8 at prev_index=10, prev_term=6
# Note: This assumes 1-based indexing like in the paper.
append_entries(log, 10, 6, [ LogEntry(8, "x") ])
```

The result of doing this for Figure 7 is as follows:

(a) False. Missing entry at index 10.
(b) False. Many missing entries.
(c) True. Entry already in position 11 is replaced.
(d) True. Entries at position 11,12 are replaced.
(e) False. Missing entries.
(f) False. Previous term mismatch.

## Commentary

A major goal of this first step is to keep things simple. Use
simple data structures.  Use a simple function to perform
the core append operation.   You want something that you can
look at, debug, and test.

## Persistence

Technically, the Raft log is supposed to be stored in a persistent
data structure that can survive server crashes.  One design problem
to think about is how persistence might get implemented and how this
implementation might related to your stand-alone `append_entries()`
operation.   It's not necessary to solve this problem right away, but
tuck it away in the back of your mind to think about for later.



