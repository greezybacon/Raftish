import asyncio
from itertools import count
from dataclasses import dataclass
import pickle
import struct
import time
from typing import Type

from .exception import NewTermError
from .log import LogEntry

import logging
log = logging.getLogger('raft.message')

id_sequence = count(1)

@dataclass
class Message:

    @classmethod
    async def from_socket(self, socket):
        header = struct.calcsize("!II")
        size, id = struct.unpack("!II", await socket.readexactly(header))
        message = await socket.readexactly(size)
        message = pickle.loads(message)
        message.id = id
        return message

    @classmethod
    def from_bytes(self, bytes):
        view = memoryview(bytes)
        header = struct.calcsize("!II")
        size, id = struct.unpack("!II", view[:header])
        message = pickle.loads(view[header:header+size])
        message.id = id
        return message

    def ensure_id(self):
        if not hasattr(self, 'id'):
            self.id = next(id_sequence)

    async def send(self, socket, destination):
        self.ensure_id()
        await self._send(self.id, socket, destination)

    async def respond_with(self, response: Type['Message'], socket, destination):
        await response._send(self.id, socket, destination)

    async def _send(self, id, socket, destination):
        message = pickle.dumps(self)
        await socket.send(
            struct.pack("!II", len(message), id) + message,
            destination)

    async def handle(self, server, sender):
        pass

class Response(Message): pass

@dataclass
class RequestVote(Message):
    term: int
    candidateId: str
    lastLogIndex: int
    lastLogTerm: int

    @dataclass
    class Response(Response):
        term: int
        voteGranted: bool

    async def handle(self, server, sender) -> Response:
        # Record the vote locally and only vote once per term
        if self.term > server.currentTerm:
            raise NewTermError(self.term)

        should_vote = self.should_vote_for_candidate(server)
        if should_vote:
            server.voteFor(self.term, self.candidateId)

        return self.Response(
            term=server.currentTerm,
            voteGranted=should_vote
        )

    def should_vote_for_candidate(self, server):
        # Respond NO if sender term is less than local
        if self.term < server.currentTerm:
            return False
        # Respond NO if sender log is shorter than local
        elif self.lastLogIndex < server.log.lastIndex:
            return False
        elif server.log.lastEntry is not None \
                and self.lastLogTerm < server.log.lastEntry.term:
            return False
        # Server can only vote once per term
        elif server.config.hasVoted:
            return False

        # else respond YES
        return True

@dataclass
class AppendEntries(Message):
    term: int
    leaderId: str
    prevLogIndex: int
    prevLogTerm: int
    entries: tuple[LogEntry]
    leaderCommit: int

    @dataclass
    class Response(Response):
        term: int
        success: bool
        matchIndex: int

    async def handle(self, server, sender) -> Response:
        if self.term > server.currentTerm:
            raise NewTermError(self.term)

        # It's important to call ::append_entries here, even if entries is
        # empty, because the leader needs to know if new entries *could* be
        # appended from the referenced prevLogIndex location.
        success = server.log.append_entries(self.entries, self.prevLogIndex,
            self.prevLogTerm)

        if success:
            await server.advanceCommitIndex(self.leaderCommit)

        # Update the cluster leader-id
        server.cluster.leaderId = self.leaderId

        return self.Response(term=server.currentTerm, success=success,
            matchIndex=server.log.lastIndex)

class WaitList(dict):
    max_lifetime = 10

    @dataclass
    class Item:
        expires: float
        message: Message

        @classmethod
        def for_message(self, message: Message, lifetime):
            message = self(message=message, expires=time.monotonic() + lifetime)
            message.waiter = asyncio.get_event_loop().create_future()
            return message

    async def wait_for(self, message: Message, timeout=None):
        log.debug(f"Starting wait for message {message.id}")
        item = self[message.id] = self.Item.for_message(message, timeout or self.max_lifetime)
        return await asyncio.wait_for(item.waiter, timeout=timeout)

    def set_response(self, response):
        log.debug(f"Resolving wait for message {response.id}")

        id = response.id
        try:
            if id in self:
                self[id].waiter.set_result(response)
        except asyncio.InvalidStateError:
            # The waiter was probably cancelled
            pass

        self.cleanup()

    def cleanup(self):
        now = time.monotonic()
        to_remove = {id for id, x in self.items() if x.expires < now}
        for id in to_remove:
            # Notify the waiter that a response isn't coming
            if not self[id].waiter.done():
                self[id].waiter.set_exception(asyncio.TimeoutError())
            del self[id]