import asyncio
from typing import Optional

from .cluster import Cluster
from .config import ClusterConfig
from .exception import NotALeader
from .log import LogEntry
from .util import _timeout as timeout

import logging
log = logging.getLogger('raft.app')

class Application:
    """
    Abstract Raft application. Should be derived into a specific application
    which should create and submit LogEntries via the ::submit method. LogEntry
    items are then fed through the Raft cluster and, eventually, the ::commit
    method is called to actually apply the LogEntry to the state machine
    locally.

    Usage:
        class MyRaftBackedApp(raft.app.Application):
            async def commit(self, message):
                # Handle message sent through ::submit
                ...

            return True

            async def handle_connection(self, reader, writer):
                # Handle new client connections, start transactions through
                # ::submit
                ...

        app = MyRaftBackedApp()
        await app.start_cluter(local_id, config)
        await app.run((host, port))
    
    Caveats:
    Note that ::commit is called for the application on *all* members of the
    Raft cluster, for every transaction sent to ::submit.
    """
    async def start_cluster(self, local_id: str, disk_path: str=None,
        config: Optional[dict]=None):
        """
        Start the local server as part of the Raft cluster for distributed
        replication and consensus.

        Parameters:
        local_id: str
            The ID of the local server in the config file
        disk_path: str
            The path where the components of the cluster system are store,
            primarily the JSON config file and transaction log.
        config: optional[dict]
            Send configuration directly if starting the cluster from a JSON
            config rather than from on-disk configuration. Checkout the
            ClusterConfig class for example configuation keys.
        """
        if disk_path is not None:
            cluster = Cluster.from_disk(local_id, disk_path)
        elif config is not None:
            cluster_conf = ClusterConfig.from_json(local_id, config)
            cluster = Cluster(cluster_conf)

        self.cluster = cluster
        self.local_server = cluster.local_server
        self.local_server.log.add_apply_callback(self.commit)
        await self.local_server.start()

    @property
    def has_quorum(self):
        return self.cluster.has_quorum

    async def commit(self, content):
        """
        A callback from the Raft system when a log entry is "committed", it is
        to be safely applied to the local application.

        Caveats:
        MUST return True to indicate that the local commit was successful. If
        not, then the LogEntry will be resent to this method again in the future
        to be retried.
        """
        return True

    async def submit(self, content):
        """
        Handles a request from a client to perform a transaction. This should be
        performed as an async task, because it might take a while.

        Parameters:
        content: any
            The content to be placed in the transaction log. Once the log has
            been successfully replicated to the Raft cluter and can be
            committed, this data will be passed to ::commit. Note that it won't
            be the self-same object.

        Raises:
            NotALeader:
                If the local node in the Raft cluster is not a leader, then it
                cannot accept the transaction. Instead, the client application
                would need to connect to the leader node in the cluster
                directly.
            LocalAppendFailed:
                If for some reason the append failed to be added to the
                transaction log locally. The attempt should be retried.

        Returns: Coroutine
        If successful, returns a coroutine that can be awaited to discover when
        the entry has been committed to the system. The ::apply method will be
        called automatically by the backend when the entry can be committed.
        This is useful if the application client wants to wait for the
        transaction to be committed.

        Caveats:
        Will block until the transaction has been applied locally (via the
        ::commit method). Use a wait primitive of asyncio to provide a timeout.
        """
        if not self.local_server.is_leader():
            raise NotALeader

        entry = LogEntry(term=self.local_server.currentTerm, value=content)
        indexId = self.local_server.append_entry(entry)

        # Await entry to become "committed"
        while self.local_server.log.lastApplied < indexId:
            await self.local_server.log.apply_event.wait()

        return True
        
    def is_local_leader(self):
        return self.local_server.is_leader()

    # Application server interface ------

    async def start_server(self, address, handler=None):
        """
        Start the TCP server for the client application. By default,
        ::handle_connection is called for each new client connection.

        Parameters:
        address: tuple(str, int)
            Address on which to bind the listening socket
        handler: async callable(reader, writer)
            Connection handler for incoming connection requests.
        """
        server = await asyncio.start_server(handler or self.handle_connection,
            *address)
        return server

    async def handle_connection(self, reader, writer):
        # Should be derived by a subclass to handle messages from the reader,
        # create a LogEntry, call `request_transaction`, and finally return
        # success.
        writer.close()

    async def run(self, address):
        """
        Run the application indefinitely. When the local system becomes the Raft
        leader, the application server will be started via the ::start_server
        method. Once started, this will continue to monitor the local system
        Raft role and will stop and re-start the application server as the
        system changes state.
        """
        # Start out with something awaitable
        server_task = asyncio.get_event_loop().create_future()
        while True:
            try:
                role_changed = asyncio.ensure_future(
                    self.local_server.role_changed.wait())
                for first in asyncio.as_completed((
                    role_changed,
                    server_task
                )):
                    # This construct will help discover exceptions in the _run
                    # coroutine
                    await first
                    break
            finally:
                role_changed.cancel()

            if self.local_server.is_leader():
                log.info("Local system is the leader. Starting the application")
                if server_task:
                    server_task.cancel()
                server_task = asyncio.create_task(self._run(address))
            if server_task.done():
                server_task = asyncio.get_event_loop().create_future()


    async def _run(self, address, start_timeout=5):
        try:
            with timeout(start_timeout):
                while True:
                    try:
                        server = await self.start_server(address)
                        # XXX: Is it sensible to assume this interface?
                        await server.start_serving()
                        log.info("Application is running")
                        break
                    except OSError:
                        # Sometimes this happens when switching leaders
                        pass
                    await asyncio.sleep(0.1)
        except asyncio.TimeoutError:
            raise SystemError("Unable to start application server")

        try:
            if self.local_server.is_leader():
                await self.local_server.role_changed.wait()

            log.warning("No longer the leader. Shutting down application")
        finally:
            server.close()
            for x in self.clients:
                if not x.done():
                    x.cancel()
            await server.wait_closed()
            log.info("Server shut down")
