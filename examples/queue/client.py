import argparse
import asyncio
import shlex
from util import input

from message import send_message, receive_message

class MessageQueueClient:
    intro = "Welcome to the Raft async MQ system"
    prompt = "(mq) "

    def __init__(self, server):
        self.addr = server

    async def connect(self, tries=50):
        host, port = self.addr
        for _ in range(tries):
            try:
                self.reader, self.writer = await asyncio.open_connection(host, port)
                break
            except ConnectionRefusedError:
                await asyncio.sleep(0.05)

    async def request(self, cmd, *args):
        send_message(self.writer, (cmd, *args))
        try:
            return await receive_message(self.reader) or "OK"
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            print("ERROR: Server went away, reconnecting and retrying")
            await self.connect()
            return await self.request(cmd, *args)

    async def do_create(self, q_name):
        response = await self.request('create', q_name)
        print(repr(response))

    async def do_del(self, q_name):
        response = await self.request('del', q_name)
        print(repr(response))

    async def do_sub(self, q_name, channel):
        response = await self.request('subscribe', q_name, channel)
        print(repr(response))

    async def do_unsub(self, q_name, channel):
        response = await self.request('unsubscribe', q_name, channel)
        print(repr(response))

    async def do_pub(self, channel, message):
        response = await self.request('publish', channel, message)
        print(repr(response))

    async def do_fetch(self, q_name):
        response = await self.request('fetch', q_name)
        print(repr(response))

    async def cmdloop(self):
        await self.connect()
        print(self.intro)
        while True:
            try:
                cmd = await input(self.prompt)
            except (EOFError, KeyboardInterrupt):
                break

            cmd, *parts = shlex.split(cmd)
            try:
                handler = getattr(self, 'do_' + cmd)
                await handler(*parts)
            except AttributeError:
                print(f"Oops: {cmd} is not a supported command")
            except TypeError as e:
                print(f"Oops: {e}")

async def mqclient(host='localhost', port=12347):
    try:
        handler = MessageQueueClient(server=(host or 'localhost', port))
        await handler.cmdloop()
    except:
        raise

parser = argparse.ArgumentParser("RAFT :: Message Queue Application Client")

if __name__ == '__main__':
    args = parser.parse_args()
    import uvloop
    uvloop.install()
    asyncio.run(mqclient())