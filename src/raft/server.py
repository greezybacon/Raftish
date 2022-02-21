import asyncio
import time
from typing import Type
import asyncio_dgram
from dataclasses import dataclass
import os, os.path
import pickle

from .exception import LocalAppendFailed, NewTermError, DeadlockError, NotALeader
from .log import TransactionLog
from .messages import Message, Response, WaitList
from .util import wait_for

import logging
log = logging.getLogger('raft.server')

class LocalServer:
    def __init__(self, id: str, listen_address: tuple[str, int], cluster):
        self._id = id
        self._cluster = cluster
        self.storage_path = cluster.config.get_storage_path()
        
        # Messaging
        self.socket_lock = asyncio.Lock()
        self.out_queue = asyncio.Queue()
        self.in_queue = asyncio.Queue()
        self.listen_address = listen_address
        self.wait_list = WaitList()

        # Persistent state
        self.config = None
        self.load_config(self.storage_path)
        
        # Volatile state
        self._commitIndex = 0
        self.log = TransactionLog(self.storage_path)

        # Coordination
        self.role = None
        self.append_event = asyncio.Event()
        self.role_changed = asyncio.Event()

    @property
    def id(self):
        return self._id
    
    @property
    def cluster(self):
        return self._cluster

    def load_config(self, path):
        self.config = LocalState(path)

    async def start(self):
        # Load the log from disk
        await self.log.load()

        # Open a socket and handle messages
        self.comm_tasks = [
            asyncio.create_task(self.receive_messages()),
            asyncio.create_task(self.send_messages()),
            asyncio.create_task(self.handle_messages())
        ]

        # Start as a follower
        await self.become_follower()

        # Get ready to communicate with remote servers
        for server in self.cluster.remote_servers:
            await server.start(self)

        await self.ensure_dgram_endpoint()

    def shutdown(self):
        self.role_task.cancel()
        for task in self.comm_tasks:
            task.cancel()
        self.socket.close()

    # Follower Protocol ------
        
    async def become_follower(self):
        await self.switch_role(FollowerRole)

    def is_follower(self):
        return type(self.role) is FollowerRole
        
    async def switch_role(self, new_role):
        if type(self.role) is new_role:
            return

        if hasattr(self, 'role_task'):
            self.role_task.cancel()

        self.role_task = asyncio.create_task(self.do_role(new_role))
        while type(self.role) is not new_role:
            await asyncio.sleep(0)

    async def handle_new_term_error(self, error: NewTermError, must_switch=False):
        self.config.currentTerm = error.term
        if must_switch or not self.is_follower():
            self.role = None
            await self.switch_role(FollowerRole)

    async def do_role(self, next_role: Type["Role"]):
        try:
            while True:
                log.info(f"{self.id}: Switching to {next_role} role")
                self.role = next_role(self)
                self.role_changed.set()
                self.role_changed.clear()
                next_role = await self.role.initiate()
                if next_role is None:
                    raise DeadlockError("Role task returned but did not specify next role")
        except NewTermError as e:
            await asyncio.shield(
                self.handle_new_term_error(e, must_switch=True))
        except asyncio.CancelledError:
            # Happens when switching roles
            pass

        self.role = None

    @property
    def commitIndex(self):
        return self._commitIndex

    async def advanceCommitIndex(self, index):
        # Ensure the commit index doesn't advance past the end of the log
        index = min(self.log.lastIndex, index)
        self._commitIndex = index
        await self.log.apply_up_to(index)

    @property
    def hasVoted(self):
        return self.config.hasVoted

    def voteFor(self, term, candidate):
        assert not self.config.hasVoted
        self.config.voteFor(term, candidate)

    # Candidate Protocol ------

    def is_candidate(self):
        return type(self.role) is CandidateRole

    @property
    def currentTerm(self):
        return self.config.currentTerm

    def incrementTerm(self):
        return self.config.incrementCurrentTerm()

    # Leader Protocol ------

    def is_leader(self):
        return type(self.role) is LeaderRole

    async def become_leader(self):
        await self.switch_role(LeaderRole)
    
    def append_entry(self, entry):
        if not self.is_leader():
            raise NotALeader

        # It's assumed that the local server is the leader
        lastId = self.log.append(entry)
        if lastId is False:
            raise LocalAppendFailed

        # Tickle the append_event for anyone listening
        self.append_event.set()
        self.append_event.clear()

        return lastId

    # Networking ------

    async def ensure_dgram_endpoint(self):
        async with self.socket_lock:
            if not hasattr(self, 'socket'):
                self.socket = await asyncio_dgram.bind(self.listen_address)
                log.info(f"Listening on {self.listen_address}")

    async def send_messages(self):
        await self.ensure_dgram_endpoint()
        while True:
            message, dest_addr = await self.out_queue.get()
            await message.send(self.socket, dest_addr)
        
    async def receive_messages(self):
        await self.ensure_dgram_endpoint()
        try:
            while True:
                message, sender = await self.socket.recv()
                message = Message.from_bytes(message)
                if isinstance(message, Response):
                    # Looks like a response. See if a message is waiting on it.
                    self.wait_list.set_response(message)
                else:
                    # Message is a request for the local server node
                    await self.in_queue.put((message, sender))
        except asyncio.CancelledError:
            # Server is shutting down
            pass
            
    async def handle_messages(self):
        # Messages will be handled here when the server is a follower. In
        # Candidate and Leader roles, the RemoteServer objects will be used to
        # send and receive messages.
        try:
            while True:
                response = False
                try:
                    message, sender = await self.in_queue.get()
                    response = await self.role.handle_message(message, sender)
                except NewTermError as e:
                    # The incoming message indicates a new sherrif in town. Demote
                    # to follower
                    await self.handle_new_term_error(e)

                    # Try the message again with the new term
                    response = await self.role.handle_message(message, sender)

                if response:
                    await message.respond_with(response, self.socket, sender)
        except asyncio.CancelledError:
            # Server is shutting down
            pass
        except:
            log.exception("oops")

class LocalState:
    """
    Represents the disk-backed properties of the local system as required by Raft.
    """

    @dataclass
    class PersistentState:
        term:       int = 1
        votedFor:   str = None

    def __init__(self, disk_path):
        self.disk_path = os.path.join(disk_path, "local_state")
        self.load()

    def load(self):
        try:
            with open(self.disk_path, 'rb') as config_file:
                log.info("Recovering saved state from disk")
                self._config = pickle.load(config_file)
        except (FileNotFoundError, EOFError):
            self._config = self.PersistentState()

    # Disk-backed state ------

    @property
    def currentTerm(self):
        return self._config.term

    @currentTerm.setter
    def currentTerm(self, value):
        assert value >= self._config.term
        self._config.term = value
        self._config.votedFor = None
        self._sync()

    def incrementCurrentTerm(self):
        self.currentTerm = self.currentTerm + 1

    def voteFor(self, term, serverId):
        assert term >= self._config.term
        assert not self.hasVoted
        self._config.term = term
        self._config.votedFor = serverId
        self._sync()

    @property
    def hasVoted(self):
        return self._config.votedFor is not None

    def _sync(self):
        # Commit state to disk
        with open(self.disk_path, 'wb') as config_file:
            pickle.dump(self._config, config_file)


class RemoteServer:
    """
    Represents all the other servers in the cluster which are not this one/the
    local one.
    """
    @dataclass
    class State:
        nextIndex: int  = 0
        matchIndex: int = 0 
        lostMessages: int = -1

    def __init__(self, id, hostname, port):
        self.id = id
        self.address = (hostname, port)
        self.state = self.State()

    async def start(self, server: LocalServer):
        self.out_queue = server.out_queue
        self.wait_list = server.wait_list

    async def transceive(self, message, timeout=None):
        message.ensure_id()
        await self.out_queue.put((message, self.address))
        return await self.wait_list.wait_for(message, timeout=timeout)

    @property
    def is_online(self):
        return self.state.lostMessages == 0

    def __repr__(self):
        return f"<Server: ({self.id}) @ {self.address}>"

# Circular imports
from .role import CandidateRole, FollowerRole, LeaderRole, Role