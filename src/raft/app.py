import asyncio

from .cluster import Cluster
from .exception import NotALeader
from .log import LogEntry

class Application:
    """
    Abstract Raft application. Should be derived into a specific application
    which should create and submit LogEntries via the ::submit_transaction
    method. LogEntry items are then fed through the Raft cluster and,
    eventually, the ::apply_transaction method is called to actually apply the
    LogEntry to the state machine locally. NOTE that ::apply_transaction is
    called for the application on *all* members of the Raft cluster.
    """
    async def start_cluster(self, local_id: str, disk_path: str=None,):
        """
        Start the Raft cluster for distributed replication and consensus.

        Parameters:
        local_id: str
            The ID of the local server in the config file
        disk_path: str
            The path where the components of the cluster system are store,
            primarily the JSON config file and transaction log.
        """
        self.raft_cluster = Cluster(local_id, disk_path)
        self.local_server = self.raft_cluster.local_server
        await self.local_server.start()
        await self.local_server.log.add_apply_callback(self.apply_transaction)

    async def apply_transaction(self, entry: LogEntry):
        """
        A callback from the Raft system when a log entry is "committed", it is
        to be safely applied to the local application.

        Caveats:
        MUST return True to indicate that the local commit was successful
        """
        return True

    async def submit_transaction(self, entry: LogEntry):
        """
        Handles a request from a client to perform a transaction. This should be
        performed as an async task, because it might take a while.

        Raises:
            NotALeader:
                If the local node in the Raft cluster is not a leader, then it
                cannot accept the transaction. Instead, the client application
                would need to connect to the leader node in the cluster
                directly.

        Caveats:
        Will block until the transaction has been applied locally (via the
        ::apply_transaction method).
        """
        if not self.local_server.is_leader():
            raise NotALeader

        indexId = await self.local_server.append_entry(entry)
        if not indexId:
            # Unable to apply. Weird
            return False

        # Await entry to become "committed"
        while self.local_server.log.lastApplied < indexId:
            await self.local_server.log.apply_event.wait()

        return True

    # Application server interface ------

    async def start_server(self, address):
        await asyncio.start_server(self.handle_connection, *address)

    async def handle_connection(self, reader, writer):
        # Should be derived by a subclass to handle messages from the reader,
        # create a LogEntry, call `request_transaction`, and finally return
        # success.
        writer.close()