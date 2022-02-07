# Project 6 - Consensus

In Project 5, you worked on log replication.  This project extends that
to determine "consensus" and to think about the possible interaction
with an application.

## Committed Entries

A core idea in Raft is that of a "committed" log entry.  In short, log
entries are "committed" if they have been replicated across a majority
of servers in a cluster. For example, if a log entry is replicated
on 3 of 5 servers (i.e., "consensus").  

Each server maintains a "commit_index" variable that tracks the last known
log entry to be committed.

```
class RaftState:
    def __init__(self, ...):
        ...
	self.commit_index = -1
	...
```

Initially, `commit_index` is set to -1 (meaning that nothing is known).
Advancement of the index is determined solely by the leader.  This happens
in two ways.  When the leader receives an `AppendEntriesResponse` message
from a follower, it learns how much of the log is replicated on that follower.
From that, the leader can advance the commit index as applicable (note: this
requires monitoring the state of all followers).

A follower only advances its `commit_index` value when it receives a
message from the leader telling it to do so.  Each `AppendEntries`
message encodes the leader's `commit_index` value.  If this value is
greater than that on the follower, the follower can update its
`commit_index` value accordingly.

## Applied Entries

Log entries are "committed" if they are replicated across a majority.
However, there is a separate concept of "applied entries."  Raft, by
itself, doesn't do anything beyond log replication. However, in a
larger application, the log represents transactions that are actually
supposed to be carried out.  For example, if implementing a key-value
store, you need to know when you can actually modify the key-value
store values.  This is the idea of an "applied entry."  Not only has
the log entry been committed, but it has been applied to the
application code.

To track this, each server additionally includes an "last_applied"
index.

```
class RaftState:
    def __init__(self, ...):
        ...
	self.last_applied = -1
	...
```

The management of the `last_applied` value exists somewhat outside of
Raft itself--the Raft algorithm only provides the value.  It doesn't
contain any logic to update the value--that's up to the application.
Basically, the application program (i.e., the key-value store) can
watch this value and know that it's safe to process log entries
anytime that it lags behind the value of `commit_index`.  The only
real requirement is that `last_applied` should never be greater than
`commit_index`.

## Your Task

Your task is to extend the log-replication code with logic that tracks
consensus.   To do this, you'll need to start tracking information about
the Raft cluster itself.  For instance, the leader will need to know
how many nodes are in the cluster and use that information to know
if consensus has been reached.  In addition, the leader will need
to communicate its commit index to followers.

Testing is going to become more difficult.  In order to test consensus,
you'll need to set up a scenario where there are multiple Raft servers,
the leader communicates with all of its followers, and their responses
are collectively used to advance the commit index.

## Thinking about Raft-Application Interaction

Eventually, you'll need to add Raft to your key-value server.
How does that interaction integrate with Raft log replication
and consensus? Specifically, how are transactions put on the log?
How do committed transactions later get turned into operations on the
key-value store class?

The Raft-Application interface is messy and complicated.  Partly
this is an artifact of everything being delayed.  Operations
on the KV store (`get()`, `set()`, and `delete()`) don't happen
right away--they get delayed until certain things happen in the
underlying Raft module.   Coordinating the time sequencing of
this is problematic and you'll need to think about the software
architecture for it.
  

 

