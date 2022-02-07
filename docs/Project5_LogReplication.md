# Project 5 - Log Replication

In Project 4, you implemented the basic `append_entries()` operation that's
at the core of Raft. In this project, you're going to extend this work to
implement log replication.   

Warning: This part of the project should not involve a huge amount of
code, but there are a lot of moving parts which make testing
and debugging more difficult. Take it slow.

## The Scenario

The ultimate goal for this project is to make one server (designated
in advance as the "leader") replicate its log on all of the other
servers. It will do this by sending messages and processing their
replies.  You will be able to append new log entries onto the leader
log and those entries will appear on all of the followers. The leader
will be able to bring any follower up to date if its log is missing
many entries.

## Server State

Each Raft server maintains its own copy of the log.  

```
class RaftState
    def __init__(self):
        self.log = [ ]
	self.current_term = 1
	...
	# More attributes added as needed
	...
```

Each server maintains its own copy of this state.

## Thinking in Events

Log replication can be broken down into four separate "events":

1. ClientLogAppend.  A client of Raft makes a request to add
   a new log entry to the leader.   The leader should take the
   new entry and use `append_entries()` to add it to its own log.
   This is how new log entries get added to a Raft cluster. 

2. AppendEntries. A message sent by the Raft leader to a follower.
   This message contains log entries that should be added to the
   follower log.  When received by a follower, it uses
   `append_entries()` to carry out the operation and responds with an
   AppendEntriesResponse message to indicate success or failure.

3. AppendEntriesResponse. A message sent by a follower back to the
   Raft leader to indicate success/failure of an earlier AppendEntries
   message.  A failure tells the leader to retry the AppendEntries
   with earlier log entries.

4. LeaderHeartbeat.  Expiration of a heartbeat timer on the leader.  When
   received, the leader sends an AppendEntries message to all of 
   the followers.  This message will include all newly added
   log entries since the last heartbeat (note: there might be none).


To handle these events, you might define a function or method corresponding
to each event.  This functions should update the Raft server state as
appropriate:

```
def handle_client_log_append(state, msg):
    # Client adds a log entry (received by leader)
    ...

def handle_append_entries(state, msg):
    # Update to the log (received by a follower)
    ...

def handle_append_entries_response(state, msg):
    # Follower response (received by leader)
    ...

def handle_leader_heartbeat(state, msg):
    # Leader heartbeat. Send AppendEntries to all followers
    ...
    
```

In response to the above events, a Raft server carries out various actions.
For example, a follower might send a message back to the leader. You
need to figure out how you want to express this concept.  

## Testing

To test your log replication, you might be able to write isolated unit
tests that test certain functionality within a carefully controlled
environment.   However, at some point, you probably need to
do something more advanced.  You might try to devise some system-level
tests around Figures 6 and 7 in the Raft paper.   For example,
you could set up a cluster of nodes in the configuration of Figure 6,
manually designate Server 0 as the leader, and have it update all of the other
servers.  If it's working, the leader will eventually make the logs
of all followers match itself.

## Comments

Getting log replication to work might be one of the most difficult
parts of the entire Raft project.  It's not necessarily a lot of code,
but it integrates everything that you've been working on so far.
Testing and debugging is extremely challenging because you've suddenly
got multiple servers and it's hard to wrap your brain around
everything that's happening.

You will likely feel that you are at some kind of impasse where
everything is broken or hacked together in some horrible way that
should just be thrown out and rewritten.  This is normal.  Expect that
certain parts might need to be refactored or improved later.

  

 

