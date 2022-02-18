import asyncio
import json
import os, os.path
import shutil

from .exception import ConfigurationError

class ClusterConfig:
    """
    Contains the information about the Raft cluster and provides basic methods
    used for message passing and such.
    """
    default_config = {
        #"storage_path": "/tmp/raft_cluster/",
        "election_timeout": 0.3,
        "broadcast_timeout": 0.05,
        "nodes": [
            {
                "id": "server1",                # Used in cluster comm
                "hostname": "localhost",        # External name
                "listen": "0.0.0.0",            # Bind address
                "port": 10000,                  # Bind port
                "app_address": "hostname:port"  # Redirect if not leader
            },
        ],
    }

    def __init__(self, local_id, json):
        self.local_id = local_id
        self.config = dict(self.default_config, **json)
        self.config_changed = asyncio.Event()

    @classmethod
    def from_json(self, local_id, json):
        return self(local_id, json)

    @classmethod
    def from_disk(self, local_id, path):
        disk_path = os.path.join(path, "config.json")
        with open(disk_path, 'rt') as config_file:
            return self(local_id, json.load(config_file))

    def for_id(self, local_id):
        return type(self)(local_id, self.config)

    def get_local_node(self):
        for x in self.config['nodes']:
            if x['id'] == self.local_id:
                return x

        raise ConfigurationError(f"{self.local_id}: Local node not in configuration")

    def get_remote_nodes(self):
        return [
            x for x in self.config['nodes']
            if x['id'] != self.local_id
        ]

    def get_storage_path(self, cleanup_temp=True):
        if 'storage_path' in self.config:
            return self.config['storage_path']

        # Create and return a temporary storage location
        path = f'/tmp/raft_node_{self.local_id}'
        if cleanup_temp and os.path.exists(path):
            shutil.rmtree(path)

        os.makedirs(path, exist_ok=not cleanup_temp)

        return path

    @property
    def election_timeout(self):
        return self.config['election_timeout']

    @property
    def broadcast_timeout(self):
        return self.config['broadcast_timeout']