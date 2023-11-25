### Command-line interface to run the servers
import asyncio
from itertools import count
import uvloop
uvloop.install()

from .client import kvclient
from .server import KVStoreApp, KvUpdate

import logging
logging.basicConfig(level=logging.INFO,
    format="%(relativeCreated)d: %(levelname)s:%(name)s:%(message)s"
)

async def client_app(args):
    return await kvclient(host=args.host, port=args.port)

async def server_app(args):
    #host='localhost', port=12347, db_path="/tmp/kvstore.db",
    #local_id=1, raft_port=20000, cluster_size=3):
    app = KVStoreApp(None)
    id = count(1)
    config = {
        "election_timeout": 0.5,
        "heartbeat_timeout": 0.05,
        "nodes": [
            {
                "id": (lid := next(id)),
                "port": args.raft_port + lid,
                "listen": args.raft_listen or "::1",
                "hostname": "::1"
            }
            for _ in range(args.cluster_size)
        ]
    }

    await app.start_cluster(args.local_id, config=config)
    app.local_server.log.set_apply_callback(KvUpdate, app.commit)
    await app.run((args.host, args.port))

import argparse
parser = argparse.ArgumentParser(description="RAFT :: KV Store Example")
subparser = parser.add_subparsers(dest="command", help="KV application to run")

server: argparse.ArgumentParser = subparser.add_parser('server')
server.add_argument("--listen", help="Interface to use for Raft", type=str,
    default='::1', dest="raft_listen")
server.add_argument("--host", help="Interface to use for application", type=str,
    default='::1')
server.add_argument("--port", help="Application listening port", type=int, default=20000)
server.add_argument("--local-id", help="LocalId for Raft server", type=int, default=1)
server.add_argument("--raft-port",
    help="Base listen port for Raft. The local-id is added to the port",
    type=int, default=20000)
server.add_argument("--cluster-size", help="Size of the Raft cluster", type=int)
server.add_argument('--raft-state-path', '-P',
    help="Folder for Raft state", type=str)
server.set_defaults(handler=server_app)

client = subparser.add_parser('client')
client.add_argument("--port", help="Application listening port", type=int, default=20000)
client.add_argument("--host", help="Host to connect to", type=str, default='localhost')
client.set_defaults(handler=client_app)

args = parser.parse_args()
asyncio.run(args.handler(args))