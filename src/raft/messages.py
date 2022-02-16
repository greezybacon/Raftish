from itertools import count
from dataclasses import dataclass, field
import pickle
import random
import struct

from .exception import NewTermError
from .log import LogEntry

import logging
log = logging.getLogger('raft.message')

class Message:
    sequence = count(random.randrange(1, int(1e9)))

    @classmethod
    async def from_socket(self, socket):
        header = struct.calcsize("!IQ")
        size, id = struct.unpack("!IQ", await socket.readexactly(header))
        message = await socket.readexactly(size)
        return pickle.loads(message)

    @classmethod
    def from_bytes(self, bytes):
        view = memoryview(bytes)
        header = struct.calcsize("!IQ")
        try:
            size, id = struct.unpack("!IQ", view[:header])
            return pickle.loads(view[header:])
        except:
            raise

    async def send(self, socket, destination):
        message = pickle.dumps(self)
        await socket.send(
            struct.pack("!IQ", len(message), next(self.sequence))
                + message,
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
        voteCast: bool

    async def handle(self, server, sender) -> Response:
        response = server.role.handle_request_vote()
        self.respond(writer, term=server.currentTerm)

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

    async def handle(self, server, sender) -> Response:
        if self.term > server.term:
            raise NewTermError(self.term)

        success = server.log.append_entries(self.entries, self.prevLogIndex,
            self.prevLogTerm)

        if success:
            server.commitIndex = self.leaderCommit

        return self.Response(term=server.term, success=success)

