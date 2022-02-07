# Project 1 - Key-Value Server

In the [Warmup](Warmup.doc) exercise, you wrote code for sending and
receiving messages over a socket.  In this project, you are going to
use that code to build a networked key-value store. A key value store
is basically a dictionary that lives on the network.  For example:

```
data = { }

def get(key):
    return data.get(key)

def set(key, value):
    data[key] = value

def delete(key):
    if key in data:
        del data[key]
```

Write a program `kvserver.py` that contains the key-value store data
and responds to client messages.  You'll run the server as a
standalone program like this:

```
bash % python kvserver.py
KV Server listening on port 12345
```

Next, write a `kvclient.py` program that allows remote access to the
server using a command-line interface like this:

```
bash % python kvclient.py set foo hello
ok
bash % python kvclient.py get foo
hello
bash % python kvclient.py delete foo
ok
bash %
```

Here, the `kvclient.py` program accepts three commands `set`, `get`,
and `delete` that interact with the server.

## How to Proceed

To make this work, you need to send messages back and forth between the client and the server.
These messages need to contain the method (i.e., "get", "set", "delete") as well as additional
information about the keys and values.   In addition, the server needs to send a response
message back. To encode these messages, you might use a format such as JSON.

## Big Picture

This key-value client/server is eventually going to serve as the
"application" for Raft.  It doesn't have to be super-fancy, but spend
some time to make sure that it works correctly.







