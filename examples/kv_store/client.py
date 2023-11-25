import asyncio

from .message import send_message, receive_message

class StoreClient:
    intro = "Welcome to the key-value storage system"
    prompt = "(kv) "

    def __init__(self, server, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader, self.writer = server

    async def request(self, cmd, *args):
        send_message(self.writer, (cmd, *args))
        return await receive_message(self.reader)

    async def do_get(self, key):
        response = await self.request('get', key)
        if response is not None:
            print(f"{key} => {response}")

        else:
            print(f"{key} not in the store")

        return response
    get = do_get

    async def do_set(self, key, value):
        response = await self.request('set', key, value)
        print(response)
    set = do_set

    async def do_del(self, key):
        response = await self.request('del', key)
        print(response)
    delete = do_del

    async def cmdloop(self):
        print(self.intro)
        while True:
            try:
                cmd = input(self.prompt)
            except (EOFError, KeyboardInterrupt):
                break

            cmd, *parts = cmd.split(' ')
            try:
                handler = getattr(self, 'do_' + cmd)
                await handler(*parts)
            except AttributeError:
                print(f"Oops: {cmd} is not a supported command")
            except TypeError as e:
                print(f"Oops: {e}")

async def kvclient(host='localhost', port=12347):
    reader, writer = await asyncio.open_connection(host or 'localhost', port)
    handler = StoreClient(server=(reader, writer))
    await handler.cmdloop()


### Command-line interface to run the clients

import argparse
parser = argparse.ArgumentParser("RAFT :: Key-Value Store Application Client")

parser.add_argument("--port", help="Application listening port", type=int, default=12347)
parser.add_argument("--raft-port", help="Base listen port for Raft. The local-id is added to the port", type=int)

if __name__ == '__main__':
    args = parser.parse_args()
    import uvloop
    uvloop.install()
    asyncio.run(kvclient(port=args.port))