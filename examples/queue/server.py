import asyncio

import setup
from command import ManageQueue, FetchMessage, PublishMessage
from message import receive_message, send_message
from mqueue import Queues, Channels

from raft.app import Application

import logging
log = logging.getLogger('queue.server')

class MessageQueueApp(Application):
    def __init__(self, db_path=None):
        self.channels = Channels()
        self.queues = Queues()

    async def handle_connection(self, reader, writer):
        async def do_create(queue: str):
            return await self.submit(ManageQueue.create(queue))

        async def do_del(queue: str):
            return await self.submit(ManageQueue.delete(queue))

        async def do_subscribe(queue: str, channel):
            return await self.submit(ManageQueue.subscribe(queue, channel))

        async def do_unsubscribe(queue: str, channel):
            return await self.submit(ManageQueue.unsubscribe(queue, channel))

        async def do_fetch(queue: str):
            # Hold the client connection until a message is available
            if queue not in self.queues:
                return KeyError(f"{queue}: No such q")

            await self.queues[queue].wait_nonempty()
            return await self.submit(FetchMessage(queue))

        async def do_publish(channel: str, message: object):
            # TODO: Do same wait as ::do_fetch if the Q is full and wait for it
            # not to be before submitting the message
            return await self.submit(PublishMessage(channel, message))

        print("Connection from:", writer.get_extra_info('peername'))
        try:
            while True:
                message = await receive_message(reader)
                cmd_name, *args = message
                try:
                    cmd = locals()['do_' + cmd_name]
                    send_message(writer, await cmd(*args))
                except Exception as e:
                    send_message(writer, e)    
                
        except asyncio.IncompleteReadError:
            # Client likely disconnected
            pass

    async def commit(self, message):
        # Called from Raft when the message is distributed to the cluster and a
        # consensus is reached. 
        return await message.apply(self)

from itertools import count
async def raft_mqserver(host='localhost', port=12347, db_path="/tmp/kvstore.db",
    local_id=1, raft_port=20000, cluster_size=3):
    app = MessageQueueApp(None)
    id = count(1)
    config = {
        "election_timeout": 0.5,
        "heartbeat_timeout": 0.05,
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
    app.local_server.log.set_apply_callback(ManageQueue, app.commit)
    app.local_server.log.set_apply_callback(PublishMessage, app.commit)
    app.local_server.log.set_apply_callback(FetchMessage, app.commit)
    await app.run((host, port))

import argparse
parser = argparse.ArgumentParser("RAFT :: Message-Queue Server Example")

parser.add_argument("--port", help="Application listening port", type=int)
parser.add_argument("--local-id", help="LocalId for Raft server", type=int)
parser.add_argument("--raft-port", help="Base listen port for Raft. The local-id is added to the port", type=int)
parser.add_argument("--cluster-size", help="Size of the Raft cluster", type=int)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO,
        format="%(relativeCreated)d: %(levelname)s:%(name)s:%(message)s"
    )

    args = parser.parse_args()
    params = {k: v for k, v in args._get_kwargs() if v is not None}

    import uvloop
    uvloop.install()

    asyncio.run(raft_mqserver(**params))
