import asyncio
from re import X
from typing import Type
import asyncio_dgram
from dataclasses import dataclass
from enum import Enum
import os, os.path
import pickle

from .exception import NewTermError
from .log import TransactionLog
from .messages import Message, Response

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

        # Persistent state
        self.config = None
        self.load_config(self.storage_path)
        
        # Volatile state
        self._commitIndex = 0
        self.log = TransactionLog(self.storage_path)

        # Coordination
        self.role = None
        self.append_event = asyncio.Event()

    @property
    def id(self):
        return self._id
    
    @property
    def cluster(self):
        return self._cluster

    def load_config(self, path):
        self.config = LocalState(path)

    async def start(self):
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
        self.socket.close()
        for task in self.comm_tasks:
            task.cancel()

    # Follower Protocol ------
        
    async def become_follower(self):
        await self.switch_role(FollowerRole)
        # TODO: Start ::handle_messages and Add done callback to cancel
        # ::handle_messages when changing from Follower role.
        
    async def switch_role(self, new_role):
        if hasattr(self, 'role_task'):
            self.role_task.cancel()

        self.role_task = asyncio.create_task(self.do_role(new_role))
        while type(self.role) is not new_role:
            await asyncio.sleep(0)

    async def do_role(self, next_role: Type["Role"]):
        try:
            while True:
                log.info(f"Switching to {next_role} role")
                self.role = next_role(self)
                next_role = await self.role.initiate()
        except asyncio.CancelledError:
            self.role = None

    @property
    def commitIndex(self):
        return self._commitIndex

    @commitIndex.setter
    def commitIndex(self, value):
        assert value >= self._commitIndex
        self._commitIndex = value

    # Candidate Protocol ------

    def is_candidate(self):
        return type(self.role) is CandidateRole

    @property
    def term(self):
        return self.config.currentTerm
    currentTerm = term

    def incrementTerm(self):
        return self.config.incrementTerm()

    # Leader Protocol ------

    def is_leader(self):
        return type(self.role) is LeaderRole

    async def become_leader(self):
        await self.switch_role(LeaderRole)
    
    def append_entry(self, entry):
        assert self.is_leader()

        # It's assumed that the local server is the leader
        lastId = self.log.append(entry)
        if lastId is not False:
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
        remote_servers = {
            x.address: x
            for x in self.cluster.remote_servers
        }

        await self.ensure_dgram_endpoint()
        try:
            while True:
                message, sender = await self.socket.recv()
                message = Message.from_bytes(message)
                if isinstance(message, Response) and sender[:2] in remote_servers:
                    # Looks like a response. Put it in the in-bound message queue
                    # for the remote server instance.
                    await remote_servers[sender[:2]].in_queue.put(message)
                else:
                    # Message is a request for the local server node
                    await self.in_queue.put((message, sender))
        except asyncio.CancelledError:
            # Server is shutting down
            pass
        except asyncio_dgram.aio.TransportClosed:
            pass
            
    async def handle_messages(self):
        # Messages will be handled here when the server is a follower. In
        # Candidate and Leader roles, the RemoteServer objects will be used to
        # send and receive messages.
        try:
            while True:
                try:
                    message, sender = await self.in_queue.get()
                    response = await self.role.handle_message(message, sender)
                    if response:
                        await response.send(self.socket, sender)
                except NewTermError as e:
                    # The incoming message indicates a new sherrif in town. Demote
                    # to follower
                    self.config.currentTerm = e.term
                    await self.switch_role(FollowerRole)
        except asyncio.CancelledError:
            # Server is shutting down
            pass
        except:
            log.exception("oops")

@dataclass
class PersistentState:
    term:       int
    votedFor:   str

class LocalState:
    """
    Represents the disk-backed properties of the local system as required by Raft.
    """
    default_config = PersistentState(
        term = 1,
        votedFor = None,
    )

    def __init__(self, disk_path):
        self.disk_path = os.path.join(disk_path, "local_state")
        self.load()

    def load(self):
        try:
            with open(self.disk_path, 'rb') as config_file:
                self._config = pickle.load(config_file)
        except (FileNotFoundError, EOFError):
            self._config = self.default_config

    # Disk-backed state

    @property
    def currentTerm(self):
        return self._config.term

    @currentTerm.setter
    def currentTerm(self, value):
        self._config.term = value
        self._sync()

    def incrementCurrentTerm(self):
        self._config.term += 1
        self._sync()

    @property
    def votedFor(self):
        return self._config.votedFor

    @votedFor.setter
    def votedFor(self, serverId):
        self._config.votedFor = serverId
        self._sync()

    def _sync(self):
        # Commit state to disk
        with open(self.disk_path, 'wb') as config_file:
            pickle.dump(self._config, config_file)

@dataclass
class RemoteState:
    nextIndex: int
    matchIndex: int

class RemoteServer:
    """
    Represents all the other servers in the cluster which are not this one/the
    local one.
    """
    def __init__(self, id, hostname, port):
        self.id = id
        self.address = (hostname, port)

    async def start(self, server):
        self.out_queue = server.out_queue
        self.in_queue = asyncio.Queue()

    async def transceive(self, message):
        await self.out_queue.put((message, self.address))
        response = await self.in_queue.get()

        assert(type(response) in (None, message.Response))
        return response

    async def receive(self):
        return await self.in_queue.get()

# Circular imports
from .role import CandidateRole, FollowerRole, LeaderRole, Role