import asyncio
import random
import time

from .exception import NewTermError
from .messages import Message, AppendEntries, RequestVote
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
        # Election timeout is between half and full election_timeout
        split = server.cluster.config.election_timeout / 2
        self.timeout_time = random.random() * split + split
        self.message_event = asyncio.Condition()

    async def initiate(self):
        # Await APPEND_ENTRIES message from the leader, request change to
        # CandidateRole if timed out
        while True:
            try:
                async with self.message_event:
                    await asyncio.wait_for(
                        self.message_event.wait(),
                        timeout=self.timeout_time)
            except asyncio.TimeoutError:
                # Suggest the server should transistion to the CandidateRole
                log.info(f"No AppendEntries messages received after {self.timeout_time}s")
                return CandidateRole

    async def handle_message(self, message: Message, sender):
        if type(message) is AppendEntries:
            async with self.message_event:
                self.message_event.notify()

        return await super().handle_message(message, sender)

class CandidateRole(Role):
    def __init__(self, server):
        super().__init__(server)
        # Election timeout is between 150 and 300ms
        self.half_timeout = server.cluster.config.election_timeout / 2

    async def initiate(self):
        try:
            while True:
                self.local_server.incrementTerm()
                start = time.monotonic()
                votes = await self.hold_election(self.half_timeout)

                # Determine if this server won, if so then promote self to leader
                votes_needed = self.local_server.cluster.quorum_count
                if votes >= votes_needed:
                    return LeaderRole

                # Wait the remainder of the election timeout
                delta = time.monotonic() - start
                await asyncio.sleep(max(0, self.half_timeout * 2 - delta))
        except NewTermError:
            # Demote self to a follower
            return FollowerRole

    async def hold_election(self, half_timeout):
        votes = 1 # Vote for self
        try:
            timeout = random.random() * half_timeout + half_timeout
            for request in asyncio.as_completed([
                    self.request_vote(server)
                    for server in self.local_server.cluster.remote_servers
                    ], timeout=timeout):
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
    def __init__(self, server):
        super().__init__(server)
        # AppendEntries timeout is between 50ms, but can be configured in the
        # cluster configuration
        self.timeout_time = server.cluster.config.broadcast_timeout
        self.sync_event = asyncio.Event()

    async def initiate(self):
        # await timeout, and then send noop APPEND_ENTRIES message
        local = self.local_server
        self.server_tasks = [
            asyncio.create_task(self.sync_server(server))
            for server in local.cluster.remote_servers
        ]

        # Watch the transaction application and update the local server
        # commitIndex when the cluster reaches concensus on the appending of the
        # LogEntries.
        try:
            while True:
                try:
                    # Also wait for exceptions from any of the sync tasks
                    for task in asyncio.as_completed((
                        local.log.apply_event.wait(),
                        *self.server_tasks
                    )):
                        # Look for first completed or exception
                        await task
                        break

                    # Advance the commitIndex; however, only items in the
                    # current leader's term can be used to advance the commit
                    # index. Once advanced. Then all previous entries are also
                    # committed.
                    if local.currentTerm == local.log.lastEntry.term:
                        local.commitIndex = local.cluster.lastCommitIndex
                        await local.log.apply_up_to(local.commitIndex)
                except NewTermError:
                    log.info("Another server has a newer term than me")
                    return FollowerRole
        except asyncio.CancelledError:
            # Propagate to the caller (server.do_role)
            raise
        except:
            log.exception("got one")
            raise
        finally:
            for task in self.server_tasks:
                task.cancel()

    async def sync_server(self, server):
        # Assume the remote server is at the same place in its logs are the
        # local server, and that we have no idea as to the state of the remote
        # server's log. This will result in the first message having empty
        # entries array which is what Raft requires.
        local = self.local_server
        nextIndex = local.log.lastIndex + 1
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

            # Determine the TERM of the entry in the prevIndex slot
            if prevIndex == 0:
                previousTerm = 0
            elif prevIndex < local.log.lastIndex:
                previousTerm = local.log.get(prevIndex).term
            else:
                # Logs are synced, so send the term of the most
                previousTerm = local.log.lastEntry.term

            # Calculate round-trip time to back out of timeout below
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
                    timeout=self.timeout_time
                )
            except asyncio.TimeoutError:
                # Maybe the message was too long?
                entryCount = max(entryCount - 1, 1)
                continue

            assert type(response) is AppendEntries.Response

            if response.term > local.currentTerm:
                # Notify the main role task that the
                log.error("OOPS: New sherrif in town")
                raise NewTermError(response.term)

            if response.success == True:
                nextIndex += len(entries)
                entryCount = max(entryCount + 1, 10)
            else:
                nextIndex -= 1
                entryCount = max(entryCount - 1, 1)

            # If the server is not yet caught up, then keep sending more packets
            if local.log.lastIndex >= nextIndex:
                continue

            server.state.matchIndex = response.matchIndex
            server.state.nextIndex = nextIndex

            # Wait for either the idle timeout or a new append entry request to
            # be broadcast.
            try:
                elapsed = time.monotonic() - start
                await asyncio.wait_for(
                    local.append_event.wait(),
                    timeout=max(0, self.timeout_time - elapsed)
                )
            except asyncio.TimeoutError:
                pass