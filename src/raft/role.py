import asyncio
from dataclasses import dataclass
import random

from .exception import NewTermError
from .messages import Message, AppendEntries
from .server import RemoteServer

import logging
log = logging.getLogger('raft.role')

class Role:
    def __init__(self, server):
        self.local_server = server

    def inititate(self):
        """
        Transitions the server into the role and handles message accordingly.

        Returns: Role
        Returns the new role to which the server should transistion.
        """
        raise NotImplemented

    async def handle_message(self, message: Message, sender):
        try:
            return await message.handle(self.local_server, sender)
        except asyncio.CancelledError:
            # Node changed roles while processing the message. The message will
            # have to be resent
            pass

class FollowerRole(Role):
    def __init__(self, server):
        super().__init__(server)
        # Election timeout is between 150 and 300ms
        self.timeout_time = random.random() * 0.15 + 0.15

    async def initiate(self):
        # Await APPEND_ENTRIES message from the leader, request change to
        # CandidateRole if timed out
        while True:
            try:
                self.timeout = asyncio.create_task(asyncio.sleep(self.timeout_time))
                await asyncio.wait_for(self.timeout)
                # Suggest the server should transistion to the CandidateRole
                return CandidateRole
            except asyncio.CancelledError:
                # Received a message from the server, so start the timeout over
                pass

    async def handle_message(self, message: Message, sender):
        if type(message) is AppendEntries:
            self.timeout.cancel()

        return await super().handle_message(message, sender)

class CandidateRole(Role):
    def __init__(self, server):
        super().__init__(server)
        # Election timeout is between 150 and 300ms
        self.timeout_time = random.random() * 0.15 + 0.15

    async def initiate(self):
        try:
            while True:
                self.local_server.incrementTerm()
                votes = await self.hold_election(self.timeout_time)

                # Determine if this server won, if so then promote self to leader
                votes_needed = self.local_server.cluster.quorum_count
                if votes >= votes_needed:
                    return LeaderRole
        except NewTermError:
            # Demote self to a follower
            return FollowerRole

    async def hold_election(self, timeout):
        votes = 1 # Vote for self
        try:
            for request in asyncio.as_completed(
                    (self.request_vote(server) for server in self.config.remote_servers),
                    timeout=timeout):
                response = await request
                if response.term > self.local_server.currentTerm:
                    raise NewTermError
                if response.voteGranted:
                    votes += 1
        except asyncio.TimeoutError:
            # Continue with the responses we have
            pass

        # Return the results of the vote
        return votes

    async def request_vote(self, server: RemoteServer):
        # Send a RequestVote message to the server and await the response
        return await server.transceive(RequestVote(
            term=self.local_server.currentTerm,
            candidateId=self.local_server.id,
            lastLogIndex=self.local_server.log.lastIndex,
            lastLogTerm=self.local_server.log.previousEntry.term
        ))

class LeaderRole(Role):
    @dataclass
    class ServerState:
        nextIndex: int
        matchIndex: int

    def __init__(self, server):
        super().__init__(server)
        # AppendEntries timeout is between 50ms
        # XXX: Make this part of the ClusterConfig
        self.timeout_time = 0.05

    async def initiate(self):
        # await timeout, and then send noop APPEND_ENTRIES message
        self.server_tasks = {
            server: asyncio.create_task(self.sync_server(server))
            for server in self.local_server.cluster.remote_servers
        }

        # Watch the transaction application and update the local server
        # commitIndex when the cluster reaches concensus on the appending of the
        # LogEntries.
        while True:
            await self.local_server.log.apply_event.wait()
            if self.local_server.currentTerm == self.local_server.log.previousEntry.term:
                self.local_server.commitIndex = self.local_server.cluster.lastCommitIndex
                await self.local_server.log.apply_up_to(self.local_server.commitIndex)

    async def sync_server(self, server):
        # Assume the remote server is at the same place in its logs are the
        # local server
        nextIndex = self.local_server.log.lastIndex
        matchIndex = 0
        while True:
            # Determine the last commit ID
            entries = self.local_server.log.since(nextIndex, max_entries=2)
            if nextIndex == -1:
                previousTerm = 0
            else:
                previousTerm = self.local_server.log[nextIndex].term
            response = await server.transceive(
                AppendEntries(
                    term=self.local_server.currentTerm,
                    leaderId=self.local_server.id,
                    prevLogIndex=nextIndex,
                    prevLogTerm=previousTerm,
                    entries=entries,
                    leaderCommit=self.local_server.commitIndex
                )
            )

            assert type(response) is AppendEntries.Response

            if response.term > self.local_server.currentTerm:
                # XXX: This won't work as currently designed. Nothing is
                # monitoring this task for errors.
                raise NewTermError(response.term)

            if response.success == True:
                nextIndex += len(entries)
                matchIndex = nextIndex
            else:
                log.info(f"Server refused the AppendEntries message")
                nextIndex -= 1

            # If the server is not yet caught up, then keep sending more packets
            if self.local_server.log.lastIndex > nextIndex:
                log.info(f"Sending more entries to the server, starting at {nextIndex}")
                continue

            log.info(f"Remote server {server} is all caught up")

            # Wait for either the idle timeout or a new append entry request to
            # be broadcast.
            try:
                await asyncio.wait_for(
                    self.local_server.append_event.wait(),
                    timeout=self.timeout_time
                )
            except asyncio.TimeoutError:
                pass