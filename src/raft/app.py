import asyncio

from .cluster import Cluster
from .config import ClusterConfig
from .exception import NotALeader
from .log import LogEntry

class Application:
    """
    Abstract Raft application. Should be derived into a specific application
    which should create and submit LogEntries via the ::submit method. LogEntry
    items are then fed through the Raft cluster and, eventually, the ::commit
    method is called to actually apply the LogEntry to the state machine
    locally.
    
    Caveats:
    Note that ::commit is called for the application on *all* members of the
    Raft cluster.
    """
    async def start_cluster(self, local_id: str, disk_path: str=None,
        config: object=None):
        """
        Start the Raft cluster for distributed replication and consensus.

        Parameters:
        local_id: str
            The ID of the local server in the config file
        disk_path: str
            The path where the components of the cluster system are store,
            primarily the JSON config file and transaction log.
        """
        if disk_path is not None:
            cluster = Cluster.from_disk(local_id, disk_path)
        elif config is not None:
            cluster_conf = ClusterConfig.from_json(local_id, config)
            cluster = Cluster(cluster_conf)

        self.cluster = cluster
        self.local_server = cluster.local_server
        await self.local_server.start()
        self.local_server.log.add_apply_callback(self.commit)

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