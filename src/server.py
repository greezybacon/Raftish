import asyncio
from inspect import ClosureVars
import time

import logging
log = logging.getLogger('servers')

async def timeserver(host='localhost', port=12345):
    async def handle_client(reader, writer):
        print("Connection from:", writer.get_extra_info('peername'))
        msg = time.ctime()
        writer.write(msg.encode('utf-8'))
        writer.write("\n".encode('utf-8'))

    server = await asyncio.start_server(handle_client, host, port)
    print(f"Listening on {host}:{port}")
    await server.serve_forever()

from message import send_message, receive_message

async def echoserver(host='localhost', port=12346, middleware=[]):
    async def handler(reader, writer):
        print("Connection from:", writer.get_extra_info('peername'))
        try:
            while True:
                message = await receive_message(reader)
                for F in middleware:
                    message = F(message) or message
                send_message(writer, message)
        except asyncio.IncompleteReadError:
            # Client likely disconnected
            pass

    server = await asyncio.start_server(handler, host, port)
    print(f"Listening on {host}:{port}")
    await server.serve_forever()

async def echoserver_upper(host='localhost', port=12346):
    return await echoserver(host, port, [lambda x: x.upper()])

from store import KeyValueStore
import pickle
async def kvserver_handler(store, reader, writer):
    async def do_get(key):
        if key not in store:
            return None
        return store.get(key)

    async def do_set(key, value):
        await store.set(key, value)
        return True

    async def do_del(key):
        await store.delete(key)
        return True

    print("Connection from:", writer.get_extra_info('peername'))
    try:
        while True:
            message = await receive_message(reader)
            cmd_name, *args = message
            try:
                cmd = locals()['do_' + cmd_name]
                send_message(writer, await cmd(*args))
            except AttributeError:
                send_message(writer, False)    
            
    except asyncio.IncompleteReadError:
        # Client likely disconnected
        pass

async def kvserver(host='localhost', port=12347, db_path="/tmp/kvstore.db"):
    store = KeyValueStore(db_path)
    async def handler(reader, writer):
        return await kvserver_handler(store, reader ,writer)

    server = await asyncio.start_server(handler, host, port)
    print(f"Listening on {host}:{port}")
    await server.serve_forever()


from raft.app import Application
class KVStoreApp(Application):
    class StoreProxy:
        def __init__(self, app: Application):
            self.app = app

        async def set(self, name, value):
            await self.app.submit(('set', name, value))
        
        def get(self, name):
            # Ensure local machine is leader and that the cluster has a quorum
            assert self.app.is_local_leader() and self.app.cluster.has_quorum
            # TODO: If application is accessible from a read replica, ensure the
            # cluster has consensus and the local server (follower) is up to
            # date
            return self.app.store.get(name)

        def __contains__(self, name):
            assert self.app.is_local_leader()
            return name in self.app.store

        async def delete(self, name):
            await self.app.submit(('delete', name))

    def __init__(self, db_path=None):
        self.store = KeyValueStore(db_path)

    async def commit(self, message):
        cmd, *args = message
        if cmd == 'set':
            key, value = args
            await self.store.set(key, value)
        elif cmd == 'delete':
            await self.store.delete(args[0])

        return True

    async def handle_connection(self, reader, writer):
        return await kvserver_handler(self.StoreProxy(self), reader, writer)

from itertools import count
async def raft_kvserver(host='localhost', port=12347, db_path="/tmp/kvstore.db",
    local_id=1, raft_port=20000, cluster_size=3):
    app = KVStoreApp(None)
    id = count(1)
    config = {
        "nodes": [
            {
                "id": (lid := next(id)),
                "port": raft_port + lid,
                "listen": "::1",
                "hostname": "::1"
            }
            for _ in range(int(cluster_size))
        ]
    }

    await app.start_cluster(local_id, config=config)
    server = None
    while True:
        await app.local_server.role_changed.wait()

        if app.local_server.is_leader():
            print("Local system is the leader. Starting the application")
            for _ in range(100):
                try:
                    server = await app.start_server((host, port))
                    await server.start_serving()
                    print("Application is running")
                    break
                except OSError:
                    # Sometimes this happens when switching leaders
                    pass
                await asyncio.sleep(0.1)
            else:
                log.error("Unable to start application")
        elif server:
            print("No longer the leader. Shutting down application")
            server.close()
            await server.wait_closed()
            server = None

### Command-line interface to run the servers

servers = {
    'time': timeserver,
    'echo': echoserver,
    'echoup': echoserver_upper,
    'kv': kvserver,
    'raft_kv': raft_kvserver,
}

import argparse
parser = argparse.ArgumentParser("RAFT :: Warmup Exercise")
parser.add_argument("server", choices=servers.keys())

parser.add_argument("--port", help="Application listening port", type=int)
parser.add_argument("--local-id", help="LocalId for Raft server", type=int)
parser.add_argument("--raft-port", help="Base listen port for Raft. The local-id is added to the port", type=int)
parser.add_argument("--cluster-size", help="Size of the Raft cluster", type=int)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
        format="%(relativeCreated)d: %(levelname)s:%(name)s:%(message)s"
    )

    args = parser.parse_args()
    params = {k: v for k, v in args._get_kwargs() if v is not None and k != 'server'}
    asyncio.run(servers[args.server](**params))