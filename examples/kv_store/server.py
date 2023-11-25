import asyncio
import os

from raft.app import Application
from raft.log import SnapshotCapable

from .message import send_message, receive_message
from .store import KeyValueStore, KvUpdate

import logging
log = logging.getLogger('servers')

class KVStoreApp(Application, SnapshotCapable):
    def __init__(self, db_path=None):
        self.store = KeyValueStore(db_path)

    async def commit(self, message: KvUpdate):
        return await message.apply(self.store)

    # SnapshotCapable interface ------------

    async def get_state(self):
        block_size = (yield)
        with self.store.state.get_state_snapshot() as state:
            while True:
                start_offset = state.tell()
                block = state.read(block_size)
                if not block:
                    break
                block_size = (yield start_offset, block)

    async def set_state(self):
        return self.store.state.set_state_snapshot()

    async def handle_connection(self, reader, writer):
        print("Connection from:", writer.get_extra_info('peername'))
        try:
            while True:
                message = await receive_message(reader)
                cmd_name, *args = message
                try:
                    cmd = getattr(self, 'do_' + cmd_name)
                    send_message(writer, await cmd(*args))
                except AttributeError:
                    send_message(writer, False)    
                except:
                    log.exception("Got unexpected error handling message")
                
        except asyncio.IncompleteReadError:
            # Client likely disconnected
            pass
        
    # CLI Interface ----
    async def do_get(self, key):
        if key not in self.store:
            return None
        return self.store.get(key)

    async def do_set(self, key, value):
        await self.submit(KvUpdate(type=KvUpdate.Type.SET, key=key, value=value))
        return True

    async def do_del(self, key):
        await self.submit(KvUpdate(type=KvUpdate.Type.DELETE, key=key))
        return True
