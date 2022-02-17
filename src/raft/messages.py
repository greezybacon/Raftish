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
        voteGranted: bool

    async def handle(self, server, sender) -> Response:
        # Record the vote locally and only vote once per term
        log.info(f"Received vote request from {server}")
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

        # Apply "committed" entries
        await server.log.apply_up_to(self.leaderCommit)

        return self.Response(term=server.currentTerm, success=success,
            matchIndex=len(server.log) - 1)

