# Introduction to Messages and Concurrency

To implement Raft, you minimally need to be able to write programs
that exchange network messages.  In addition, you will need to write
programs that involve concurrency (i.e., being able to do more than
one thing at once).  This exercise guides you through a few primitive
elements that may be useful in the project.

## Part 1:  Network Programming with Sockets

The most low-level way to communiate on the network is to write code
that uses sockets. A socket represents an end-point for communicating
on the network.

To create a socket, use the `socket` library.  For example:

```
from socket import socket, AF_INET, SOCK_STREAM
sock = socket(AF_INET, SOCK_STREAM)
```

The `AF_INET` option specifies that you are using the internet
(version 4) and the `SOCK_STREAM` specifies that you want a reliable
streaming connection.  Technically, this is a TCP/IPv4 connection.
TCP/IPv4 communication.  Change the `AF_INET` to `AF_INET6` if you
want to use TCP/IPv6.

### Making an Outgoing Connection

If a program makes an outgoing connection to a remote machine, it is
usually known as a "client." Here's an example of using a socket to
make an outgoing HTTP request and reading the response:

```
from socket import socket, AF_INET, SOCK_STREAM

sock = socket(AF_INET, SOCK_STREAM)
sock.connect(('www.python.org', 80))
sock.send(b'GET /index.html HTTP/1.0\r\n\r\n')
parts = []
while True:
    part = sock.recv(1000)     # Receive up to 1000 bytes (might be less)
    if part == b'':
        break                  # Connection closed
    parts.append(part)
# Form the complete response
response = b''.join(parts)
print("Response:", response.decode('ascii'))
```

Try running the above program.  You'll probably get a response
indicating some kind of error.  That is fine. Our goal is not to
implement HTTP, but simply to see some communication in action.

Now, a few important details.

1. Network addresses are specified as a tuple `(hostname, port)` where
`hostname` is a name like `'www.python.org'` `port` is a number in the range 0-65535.

2. The port number must be known in advance.  This is usually dictated
by a standard. For example, port 25 is used for email, port 80 is used
for HTTP, and port 443 is used for HTTPS.  See
https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml

3. Data is sent using `sock.send()`.  Data is received `sock.recv()`.
Both of these operations only work with byte-strings.  If you are
working with text (Unicode), you will need to make sure it's properly
encoded/decoded from bytes.

4. Data is received and transmitted in fragments.  The `sock.recv()`
accepts a maximum number of bytes, but that it is only a maximum.  The
actual number of bytes returned might be much less than this. It is your
responsibility to reassemble data from fragments into a complete
response.  Thus, you might have to collect parts and put them back
together as shown.  A closed connection or "end of file" is indicated
by `sock.recv()` returning an empty byte string.

### Receiving Incoming Connections

A program that receives incoming connections is usually known as a
"server."  Recall that clients (above) need to know the address and
port number in order to make a connection.  To receive a connection, a
program first needs to bind a socket to a port.  Here's how you do
that:

```
sock = socket(AF_INET, SOCK_STREAM)
sock.bind(('', 12345))         # Bind to port 12345 on this machine
sock.listen()                  # Enable incoming connections
```

To accept a connection, use the `sock.accept()` method:

```
client, addr = sock.accept()     # Wait for a connection
```

`accept()` returns two values.  The first is a new socket
object that represents the connection back to the client.  The second
is the remote address `(host, port)` of the client.  You use the
`client` socket for further communication.  Use the address `addr` for
diagnostics and to do things like reject connections from unknown
locations.

One confusion with servers concerns the initial socket that you create
(`sock`).  The initial socket only serves as a connection point
for clients. No actual communication takes place using this socket.
All further communication actually uses the `client` socket
returned by `sock.accept()`.

Here is an example of a server that reports the current time back to
clients:

```
# timeserver.py
import time
from socket import socket, AF_INET, SOCK_STREAM

sock = socket(AF_INET, SOCK_STREAM)
sock.bind(('',12345))
sock.listen()
while True:
    client, addr = sock.accept()
    print('Connection from', addr)
    msg = time.ctime()    # Get current time
    client.sendall(msg.encode('utf-8'))
    client.close()
```

Try running this program on your machine.  While it is running, try
connecting to it from a separate program.

```
# timeclient.py
from socket import socket, AF_INET, SOCK_STREAM

sock = socket(AF_INET, SOCK_STREAM)
sock.connect(('localhost', 12345))
msg = sock.recv(1000)              # Get the time
print('Current time is:', msg.decode('utf-8'))
```

Run this program (separately from the server).  You should see it
print the current time.

## Part 2: Message Passing

A problem with sockets is that they are too low-level.  Data is often
fragmented in a manner that's hard to predict (for example, you don't
know how much data will be returned by a `recv()` call).

To make communication over a socket more sane, it is sometimes common
to build a message-passing abstraction layer where messages are
packaged into discreet units that are delivered and received in their
entirety.  One way to do this is to use size-prefixed messages.  This
is a technique where every message is prepended with a byte-count to
indicate how large the message is.  Here is some sample code that
sends a size-prefixed message:

```
def send_message(sock, msg):
    size = b'%10d' % len(msg)    # Make a 10-byte length field
    sock.sendall(size)
    sock.sendall(msg)
```

Here is some sample code that receives a size-prefixed message:

```
def recv_exactly(sock, nbytes):
    chunks = []
    while nbytes > 0:
        chunk = sock.recv(nbytes)
        if chunk == b'':
            raise IOError("Incomplete message")
        chunks.append(chunk)
        nbytes -= len(chunk)
    return b''.join(chunks)

def recv_message(sock):
    size = int(recv_exactly(sock, 10))
    return recv_exactly(sock, size)
```

Put these functions into a file called `message.py`.

### Echo Server

To test your `send_message()` and `recv_message()` functions, write a simple
echo server.  An echo server reads a message from the client and echos it
back. For example, here's a client:

```
# echoclient.py
from socket import socket, AF_INET, SOCK_STREAM
from message import send_message, recv_message

def main(addr):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(addr)
    while True:
        msg = input("Say >")
        if not msg:
            break
        send_message(sock, msg.encode('utf-8'))       
        response = recv_message(sock)
        print("Received >", response.decode('utf-8'))
    sock.close()
     
main(('localhost', 12345))
```

Here's the corresponding server

```
# echoserver.py
from socket import socket, AF_INET, SOCK_STREAM
from message import send_message, recv_message

def echo_messages(sock):
    try:
        while True:
            msg = recv_message(sock)
            send_message(sock, msg)
    except IOError:
        sock.close()
        
def main(addr):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    while True:
        client, addr = sock.accept()
        print('Connection from:', addr)
        echo_messages(client)

main(('localhost', 12345))
```

Make sure you know how to run the above two programs.  You need to run
the `echoserver.py` program first and leave it running.  The
`echoclient.py` programs needs to run separately.  You should be able
to type messages into the client and see them echoed back.

## Part 3 - Concurrency with Threads

Run the `echoserver.py` and `echoclient.py` programs from the last part.
While they are running, start a second `echoclient.py` program in a
separate terminal window.   Does this second program work?  That is, if
you type messages, are they echoed back?   The answer should be no.

What happens if you kill the first `echoclient.py` program?  (Hint:
you should sending see the second program start working).

A common problem in network programming is that of handling concurrent
connections.  One way to to address this is to use thread programming.
A thread a function that runs concurrently and independently within
your program.  Here is a simple Python example:

```
import time
import threading

def countdown(n):
    while n > 0:
        print('T-minus', n)
        time.sleep(1)
        n -= 1

def countup(stop):
    x = 0
    while x < stop:
        print('Up we go', x)
        time.sleep(1)
        x += 1

def main():
    t1 = threading.Thread(target=countdown, args=[10])
    t2 = threading.Thread(target=countup, args=[5])
    t1.start()
    t2.start()
    print('Waiting')
    t1.join()
    t2.join()
    print('Goodbye')

main()
```

Run this program and watch what it does.  You should see the `countdown()`
and `countup()` functions running at the same time.

There's not much you can do with threads once created.  The `join()`
method is used if you want to wait for a thread to terminate.  There is
no way to kill a thread manually.

### A Threaded Echo Server

Modify the `echoserver.py` program to handle each client connection
in a thread.  This is a minor change:

```
# echoserver.py
from socket import socket, AF_INET, SOCK_STREAM
from message import send_message, recv_message
from threading import Thread

def echo_messages(sock):
    try:
        while True:
            msg = recv_message(sock)
            send_message(sock, msg)
    except IOError:
        sock.close()
        
def main(addr):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    while True:
        client, addr = sock.accept()
        print('Connection from:', addr)
        Thread(target=echo_messages, args=[client]).start()  # <<<

main(('localhost', 12345))
```

Run this new version of `echoserver.py`.  Make sure that it can talk
to multiple clients at once.

## Part 4 - Communicating Tasks

A particular problem with threads is that of coordination and
communication.  One approach for solving this is to create a
channel in the form of a queue.   Here is a simple example
for you to try:

```
import threading
import queue
import time

def producer(q):
    for i in range(10):
        print('Producing', i)
        q.put(i)
        time.sleep(1)
    q.put(None)
    print('Producer done')

def consumer(q):
    while True:
        item = q.get()
        if item is None:
            break
        print('Consuming', item)
    print('Consumer goodbye')


def main():
    q = queue.Queue()
    t1 = threading.Thread(target=producer, args=[q])
    t2 = threading.Thread(target=consumer, args=[q])
    t1.start()
    t2.start()
    print('Waiting')
    t1.join()
    t2.join()
    print('Done')

main()
```

In this program, there are two different threads, `producer()` and
`consumer()`.   The producer puts items onto a queue which the
consumer reads them.   Try running this program and observe its
behavior.  Make sure you understand what's happening.

### Echo Server Revisited

A common architecture for concurrency and distributed computing is to
focus entirely on communicating tasks.  This could involve independent
programs sending messages over sockets as well as threads
communicating via queues.

Try this slightly different version of an echo server.

```
# echoserver.py
from socket import socket, AF_INET, SOCK_STREAM
from message import send_message, recv_message
from threading import Thread
from queue import Queue

def send_responses(q):
    while True:
        sock, msg = q.get()
        send_message(sock, msg)

def read_requests(q, sock):
    try:
        while True:
            msg = recv_message(sock)
            q.put((sock, msg))
    except IOError:
        sock.close()
        
def main(addr):
    q = Queue()
    Thread(target=send_responses, args=[q]).start()
    
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    while True:
        client, addr = sock.accept()
        print('Connection from:', addr)
        Thread(target=read_requests, args=[q, client]).start() 

main(('localhost', 12345))
```

In this version of code, messages are read from a socket, dropped into
a queue, and then forgotten about.  A separate thread is responsible
for reading from the queue and sending responses back.

With this architecture, you can make more complicated kinds of things.
For example, here is a task that performs case conversions.

```
def case_converter(in_q, out_q):
    while True:
        sock, msg = in_q.get()
	out_q.put(sock, msg.upper())
```

Try wiring this into the echo server:

```
# echoserver.py
from socket import socket, AF_INET, SOCK_STREAM
from message import send_message, recv_message
from threading import Thread
from queue import Queue

def send_responses(q):
    while True:
        sock, msg = q.get()
        send_message(sock, msg)

def read_requests(q, sock):
    try:
        while True:
            msg = recv_message(sock)
            q.put((sock, msg))
    except IOError:
        sock.close()
	
def case_converter(in_q, out_q):
    while True:
        sock, msg = in_q.get()
        out_q.put((sock, msg.upper()))

def main(addr):
    sender_q = Queue()
    case_q = Queue()
    Thread(target=send_responses, args=[sender_q]).start()
    Thread(target=case_converter, args=[case_q, sender_q]).start()
    
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    while True:
        client, addr = sock.accept()
        print('Connection from:', addr)
        Thread(target=read_requests, args=[case_q, client]).start() 

main(('localhost', 12345))
```

Try running this code. Make sure you understand what is happening and
how data is flowing in the system.

## Summary

The general programming techniques used in this warmup are the main
components you will need to implement Raft.  Specifically:

1. You need to send network messages.
2. You need to have concurrent tasks (threads, etc.)
3. You need to manage communication between tasks (queues, channels, etc.).

If you are planning to implement the project in a different language than
Python, you should try to re-do the examples in that language.  All
of these concepts should be translatable to other environments.  For
example, if you were using Go, you might use goroutines and channels in
place of threads and queues.







 
