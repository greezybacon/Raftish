import asyncio
from inspect import ClosureVars
import time

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
async def kvserver(host='localhost', port=12347, db_path="/tmp/kvstore.db"):
    store = KeyValueStore(db_path)

    async def handler(reader, writer):
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

    server = await asyncio.start_server(handler, host, port)
    print(f"Listening on {host}:{port}")
    await server.serve_forever()




### Command-line interface to run the servers

servers = {
    'time': timeserver,
    'echo': echoserver,
    'echoup': echoserver_upper,
    'kv': kvserver,
}

import argparse
parser = argparse.ArgumentParser("RAFT :: Warmup Exercise")
parser.add_argument("server", choices=servers.keys())

if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(servers[args.server]())