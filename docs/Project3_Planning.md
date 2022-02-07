# Project 3 - Starting Out

Part of the problem with implementing Raft is knowing precisely where
to begin.  In this project, the goal is to sketch out an overall
framework for building the project.  In particular, you should
carefully read over sections 5 and 8 of the [Raft
Paper](https://raft.github.io/raft.pdf).  Think about the different
things you're going to need in the code.  

I realize that this is probably a bit neubulous, but here are some
concrete examples.

Raft requires a certain amount of configuration and setup.  For example,
each server needs to have a network address. Thus, perhaps you
have a `raftconfig.py` file containing some of that information:

```
# raftconfig.py

# Mappimng of server numbers to network addresses
SERVERS = {
    0: ('localhost', 15000),
    1: ('localhost', 16000),
    2: ('localhost', 17000),
    3: ('localhost', 18000),
    4: ('localhost', 19000),
    }
```

Raft involves sending messages between servers.  Many of these messages
are described in Figure 2 of the Raft paper.  Perhaps you define classes
for these messages:

```
# raftmessage.py

class Message:
    ...
      
class AppendEntries(Message):
    ...

class AppendEntriesResponse(Message):
    ...
```

Raft servers store a certain amount of state.  This includes a log,
current term, state, voting information, and so forth.  Perhaps this
information should be put in some kind of class definition.

Raft is used to serve an application.  In our case, this is the key-value store
from Project 1.   How does the application interact with Raft?  How do you WANT the
application to interact with Raft?

This is the basic gist of the idea.  Think about what you are going to need based
on your understanding of the paper.  Plan out the files, the data structures,
and other details.   We're not so focused on actually making anything work--this
is all just planning and thinking.   Implementation details will be filled in later.



