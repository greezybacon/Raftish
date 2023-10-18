import asyncio
from urllib import request

async def gettime(host=None, port=12345):
    reader, writer = await asyncio.open_connection(host or 'localhost', port)
    time = await reader.readline()
    time = time.decode('utf-8').rstrip()
    print('Current time is:', time)

from message import send_message, receive_message

async def echo(host='localhost', port=12346):
    reader, writer = await asyncio.open_connection(host or 'localhost', port)
    while True:
        try:
            message = input('Say > ')
            if not message:
                break
            send_message(writer, message)
            print('Received >', await receive_message(reader))
        except (EOFError, KeyboardInterrupt):
            break

import pickle
    
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

clients = {
    'time': gettime,
    'echo': echo,
    'kv': kvclient,
}

import argparse
parser = argparse.ArgumentParser("RAFT :: Warmup Exercise")
parser.add_argument("client", choices=clients.keys())

if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(clients[args.client]())