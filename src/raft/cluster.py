import asyncio
from functools import lru_cache

from .config import ClusterConfig
from .server import RemoteServer, LocalServer

import logging
log = logging.getLogger('raft.cluster')

class Cluster:
    """
    Represents the cluster as a whole, which has its own configuration.
    """

    def __init__(self, config):
        self.config = config
        self.local_id = config.local_id
        self.leaderId = None
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
        matchIndexes = [x.state.matchIndex for x in self.remote_servers]
        matchIndexes.append(self._local_server.log.lastIndex)
        # Find the nextIndex number that the majority of the systems are greater
        # than. To find that, sort the list and take the ::quorum_count item
        majority = self.quorum_count
        if len(matchIndexes) < majority:
            return 0

        return sorted(matchIndexes, reverse=True)[majority - 1]

    @property
    def has_quorum(self):
        online = sum(1 if x.is_online else 0 for x in self.remote_servers)
        return online + 1 >= self.quorum_count

    @property
    def local_server(self):
        return self._local_server

    async def start_server(self):
        await self._local_server.start()

    def shutdown(self):
        self._local_server.shutdown()

    @property
    def leader(self):
        if self.leaderId == self.local_id:
            return self._local_server

        for server in self._remote_servers:
            if server.id == self.leaderId:
                return server

        # Otherwise the leader is not known?

    @property
    @lru_cache
    def quorum_count(self):
        return ((len(self._remote_servers) + 1) // 2) + 1