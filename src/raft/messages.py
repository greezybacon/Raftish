import asyncio
from itertools import count
from dataclasses import dataclass
import pickle
import struct
import time

from .exception import NewTermError, TheresAnotherLeader
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
        id, message = pickle.loads(view)
        message.id = id
        return message

    def ensure_id(self, extra: int=None):
        if not hasattr(self, 'id'):
            id = next(id_sequence)
            if extra is not None:
                id = (extra, id)
            self.id = id

    async def send(self, socket, destination):
        self.ensure_id()
        await self._send(self.id, socket, destination)

    async def respond_with(self, response: 'Response', socket, destination):
        await response._send(self.id, socket, destination)

    async def _send(self, id, socket, destination):
        message = pickle.dumps((id, self), 3)
        await socket.send(message, destination)

    async def handle(self, server, sender):
        raise NotImplementedError

class Response(Message):
    def set_id(self, message: Message):
        self.id = message.id

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

    def should_vote_for_candidate(self, server: 'LocalServer'):
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
        elif self.term == server.currentTerm and server.config.hasVoted:
            return False
        # Server should not vote if it is a leader?
        elif server.is_leader():
            return False
        # Remote server must be in local systems
        elif self.candidateId not in server.cluster.remote_server_ids:
            log.warn("Got vote request from foreign server")
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
        matchIndex: int = 0

    async def handle(self, server, sender) -> Response:
        if self.leaderId not in server.cluster.remote_server_ids:
            return

        if self.term > server.currentTerm:
            raise NewTermError(self.term)

        if self.term < server.currentTerm:
            # Reject the message
            return self.Response(term=server.currentTerm, success=False)

        if not server.is_follower():
            raise TheresAnotherLeader()

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
    """
    Simple object to be used by the RemoteServer instances to await a response
    to a message. Using Message::ensure_id(), an ID# is generated and sent with
    the message. If the receiver uses Message::respond_with() to send a
    response, the same ID# is returned in the response. This allows a
    transactional model to be used for messaging and allows the sender to block
    and wait for the response. It also allows keeping the response out of the
    local servers receive queue and allows the sender to be freed from blocking
    directly.
    """
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
        """
        Await a response to the message. The LocalServer will receive the
        response and use ::set_response to actually deliver the response and
        free the waiter.

        Parameters:
        message: Message
            The message which will be responded to in the near future. Must
            already be sent with Message::send().
        timeout: Optional[float] = None
            The time to wait for response. If unspecified, the ::max_lifetime
            property of the WaitList is used as the timeout.

        Raises:
        asyncio.TimeoutError
            If the response in not received within the timeout specified. If the
            timeout is not specified, the WaitList imposes a maximum lifetime of
            the wait item in the list. It will inject TimeoutError into the
            waiter task when the wait is abandoned.

        Returns: Response
        The response the the given message.
        """
        log.debug(f"Starting wait for message {message.id}")
        item = self[message.id] = self.Item.for_message(message, timeout or self.max_lifetime)
        try:
            return await item.waiter
        finally:
            if message.id in self:
                del self[message.id]

    def set_response(self, response):
        log.debug(f"Resolving wait for message {response.id}")

        id = response.id
        try:
            if id in self:
                assert isinstance(response, Response)
                self[id].waiter.set_result(response)
            else:
                log.warning(f"No waiter for response id {id}")
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
                self[id].waiter.set_exception(asyncio.TimeoutError)
            del self[id]