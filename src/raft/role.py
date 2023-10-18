import asyncio
import random
import time

from .exception import NewTermError, TheresAnotherLeader
from .util import BroadcastEvent

import logging
log = logging.getLogger('raft.role')

class Role:
    def __init__(self, server: 'LocalServer'):
        self.local_server = server

    def inititate(self):
        """
        Transitions the server into the role and handles message accordingly.

        Returns: Role
        Returns the new role to which the server should transistion.
        """
        raise NotImplementedError

    async def handle_message(self, message: 'Message', sender):
        return await message.handle(self.local_server, sender)

class FollowerRole(Role):
    def __init__(self, server):
        super().__init__(server)
        # Election timeout is between half and full election_timeout
        self.timeout_time = server.cluster.config.election_timeout
        self.got_message = asyncio.Event()

    async def initiate(self):
        # Await APPEND_ENTRIES message from the leader, request change to
        # CandidateRole if timed out
        try:
            while True:
                await asyncio.wait_for(
                    self.got_message.wait(),
                    timeout=self.timeout_time)
                self.got_message.clear()
        except asyncio.TimeoutError:
            # Suggest the server should transistion to the CandidateRole
            return CandidateRole

    async def handle_message(self, message: 'Message', sender):
        if type(message) in (AppendEntries,):
            self.got_message.set()

        return await super().handle_message(message, sender)

class CandidateRole(Role):
    def __init__(self, server):
        super().__init__(server)
        # Election timeout is between 150 and 300ms
        self.election_timeout = server.cluster.config.election_timeout

    async def initiate(self):
        votes_needed = self.local_server.cluster.quorum_count
        max_wait = self.election_timeout
        half_wait = max_wait / 2
        while True:
            wait_time = random.random() * half_wait + half_wait
            self.local_server.incrementTerm()
            start = time.monotonic()
            votes = await self.hold_election(wait_time, votes_needed)

            log.info(f"{self.local_server.id}: Election's over. Got {votes} votes, need {votes_needed}")

            if votes >= votes_needed:
                return LeaderRole

            # Wait the remainder of the election timeout. Note this happens if
            # the other nodes say NO.
            elapsed = time.monotonic() - start
            await asyncio.sleep(max(0, wait_time - elapsed))

    async def handle_message(self, message: 'Message', sender):
        if type(message) is AppendEntries:
            if message.leaderId in self.local_server.cluster.remote_server_ids:
                raise TheresAnotherLeader()

        return await super().handle_message(message, sender)

    async def hold_election(self, wait_time, votes_needed):
        log.info(f"{self.local_server.id}: Holding a new election, term := {self.local_server.currentTerm}, wait_time := {wait_time:.3f}")
        # Vote for self
        self.local_server.voteFor(term=self.local_server.currentTerm,
            candidate=self.local_server.id)
        votes = 1

        try:
            constituants = [
                self.request_vote(server)
                for server in self.local_server.cluster.remote_servers
            ]
            for request in asyncio.as_completed(constituants, timeout=wait_time):
                response = await request

                if response.term > self.local_server.currentTerm:
                    raise NewTermError(response.term)

                assert response.term == self.local_server.currentTerm

                if response.voteGranted:
                    votes += 1

                if votes >= votes_needed:
                    break
        except asyncio.TimeoutError:
            # Continue with the votes we collected (probably not enough
            # though..). But cancel the remaining vote requests first
            pass

        # Return the results of the vote
        return votes

    async def request_vote(self, server: 'RemoteServer'):
        # Send a RequestVote message to the server and await the response
        lastIndex = self.local_server.log.lastIndex
        if lastIndex == 0:
            lastLogTerm = 0
        else:
            lastLogTerm = self.local_server.log.lastEntry.term

        return await server.transceive(RequestVote(
            term=self.local_server.currentTerm,
            candidateId=self.local_server.id,
            lastLogIndex=lastIndex,
            lastLogTerm=lastLogTerm
        ))

class LeaderRole(Role):
    def __init__(self, server, max_entry_count=25):
        """
        Parameters:
        server: LocalServer
            The local server (which happens to be the leader)
        max_entry_count: int
            Maximum number of entries to send to a remote server in the
            AppendEntries message
        """
        super().__init__(server)
        # AppendEntries timeout is between 50ms, but can be configured in the
        # cluster configuration
        self.heartbeat_time = server.cluster.config.broadcast_timeout
        self.sync_event = asyncio.Event()
        self.max_entry_count = max_entry_count

    async def initiate(self):
        # Start tasks to send heartbeats to all the cluster members
        local = self.local_server

        # Watch the synchronization events and update the local server
        # commitIndex when the cluster reaches concensus on the appending of the
        # LogEntries.

        server_sync_tasks = [
            self.sync_server(server)
            for server in local.cluster.remote_servers
        ]

        while True:
            # Also wait for exceptions from any of the sync tasks
            try:
                await asyncio.wait((
                    self.sync_event.wait(),
                    *server_sync_tasks
                ), return_when=asyncio.FIRST_COMPLETED)
                self.sync_event.clear()
            except asyncio.CancelledError:
                break
            except:
                log.exception("Received unexpected error in leader sync; restarting")
                return LeaderRole

            # Advance the commitIndex; however, only items in the
            # current leader's term can be used to advance the commit
            # index. Once advanced. Then all previous entries are also
            # committed.
            if len(local.log):
                if local.currentTerm == local.log.lastEntry.term:
                    await local.advanceCommitIndex(local.cluster.lastCommitIndex())

    async def sync_server(self, server: 'RemoteServer'):
        """
        Remote server (follower) synchronization protocol.

        Parameters:
        server: RemoteServer
            The remote server in the cluster which should receive the
            AppendEntries messages.
        """
        # Assume the remote server is at the same place in its logs are the
        # local server, but that we have no idea as to the actual state of the
        # remote server's log. This will result in the first message having
        # empty entries array which is what Raft requires.
        local = self.local_server
        nextIndex = local.log.lastIndex + 1
        heartbeat_time = min_heartbeat_time = self.heartbeat_time
        max_heartbeat_time = heartbeat_time * 2
        entryCount = 5
        while True:
            # Okay- so the local log extends from 1 to lastIndex, unless
            # it's empty, in which case it extends from 0, prevIndex := the
            # entry index previous to the current LAST entry of the remote
            # server's log (which may not yet be known).
            prevIndex = max(0, nextIndex - 1)

            # Fetch a list of entries AFTER (not including) the previous
            # entry
            entries = local.log.since(prevIndex, max_entries=entryCount)
            assert entries is not None

            # Determine the TERM of the entry in the prevIndex slot
            if prevIndex == 0:
                previousTerm = 0
            elif prevIndex < local.log.lastIndex:
                previousTerm = local.log.get(prevIndex).term
            else:
                # Logs are synced, so send the term of the most recent entry in
                # the local log
                previousTerm = local.log.lastEntry.term

            # Calculate round-trip time to back out of wait time below
            start = time.monotonic()
            try:
                response = await server.transceive(
                    AppendEntries(
                        term=local.currentTerm,
                        leaderId=local.id,
                        # Note that ::since gives items AFTER this mark
                        prevLogIndex=prevIndex,
                        prevLogTerm=previousTerm,
                        entries=entries,
                        leaderCommit=local.commitIndex
                    ),
                    timeout=heartbeat_time
                )
            except asyncio.TimeoutError:
                # Maybe the message was too long?
                entryCount = max(entryCount - 1, 1)
                # Back off from the message send time
                heartbeat_time = min(heartbeat_time * 1.05, max_heartbeat_time)
                server.state.lostMessages += 1
                if server.state.lostMessages == 10:
                    log.warning(f"{self.local_server.id}: Trouble communicating with server {server.id}")
                continue

            if type(response) is not AppendEntries.Response:
                log.error(f"Got unexpected message type {type(response)}: {response}")
                continue

            # Handle returning from lost messages
            if server.state.lostMessages > 10:
                # This is the first message after missing several
                log.info(f"Resuming sync of server {server.id}")
                heartbeat_time = min_heartbeat_time

            server.state.lostMessages = 0

            if response.success == True:
                nextIndex += len(entries)
                entryCount = min(entryCount + 1, self.max_entry_count)
            else:
                nextIndex = min(nextIndex - 1, response.matchIndex)
                entryCount = max(entryCount - 1, 1)

            server.state.matchIndex = response.matchIndex
            server.state.nextIndex = nextIndex

            # Don't signal sync event if no new entries were sent
            if len(entries):
                self.sync_event.set()

            # TODO: Impose a minimum wait time between packets

            # If the server is not yet caught up, then keep sending more packets
            if local.log.lastIndex >= nextIndex:
                continue

            # Wait for either the idle timeout or a new append entry request to
            # be broadcast.
            try:
                elapsed = time.monotonic() - start
                await asyncio.wait_for(
                    local.append_event.wait(),
                    timeout=max(0, heartbeat_time - elapsed)
                )
            except asyncio.TimeoutError:
                pass

# Circular dependency imports
from .server import LocalServer, RemoteServer
from .messages import AppendEntries, RequestVote