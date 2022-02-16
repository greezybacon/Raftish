from functools import lru_cache

from .config import ClusterConfig
from .server import RemoteServer, LocalServer

class Cluster:
    """
    Represents the cluster as a whole, which has its own configuration.
    """

    def __init__(self, config):
        self.config = config
        self.local_id = config.local_id
        self.setup()

    @classmethod
    def from_disk(self, local_id, storage_path):
        config = ClusterConfig(local_id, storage_path)
        return self(local_id, config)

    def setup(self):
        self._remote_servers = [
            RemoteServer(x['id'], x['hostname'], x['port'])
            for x in self.config.get_remote_nodes()
        ]

        local_info = self.config.get_local_node()
        self._local_server = LocalServer(
            local_info['id'], (local_info['hostname'], local_info['port']),
            self
        )

    @property
    def remote_servers(self):
        return self._remote_servers

    def lastCommitIndex(self):
        nextIndexes = [x.state.nextIndex for x in self.remote_servers]
        # Find the nextIndex number that the majority of the systems are greater
        # than. To find that, sort the list and take the ::quorum_count item
        majority = self.quorum_count
        if len(nextIndexes) < majority:
            return 0

        return sorted(nextIndexes, reverse=True)[majority - 1]

    @property
    def local_server(self):
        return self._local_server

    async def start_server(self):
        await self._local_server.start()

    def shutdown(self):
        self._local_server.shutdown()

    @property
    @lru_cache
    def quorum_count(self):
        # XXX: This can be memoized
        return ((len(self._remote_servers) + 1) // 2) + 1