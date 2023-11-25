import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
import io
import os
import pickle
import shelve

from raft.log import Commitable

@dataclass
class KvUpdate(Commitable):
    class Type(Enum):
        SET = 1
        DELETE = 2

    type: Type
    key: str
    value: object = None

    async def apply(self, store):
        if self.type == self.Type.SET:
            return await store.set(self.key, self.value)


class MemoryState:
    def __init__(self, store: shelve.Shelf):
        self.state = iter(store.items())
        self.position = 0

    def read(self, size=0):
        block = io.BytesIO()
        while size and block.tell() < size:
            try:
                pickle.dump(tuple(next(self.state)), block, pickle.DEFAULT_PROTOCOL)
            except StopIteration:
                break

        self.position += block.tell()
        return block.getvalue()

    def seek(self, position):
        raise NotImplementedError

    def tell(self):
        return self.position


class ShelfStore:
    def __init__(self, disk_path=None):
        self.save_path = disk_path
        if disk_path is not None:
            self.store = shelve.open(disk_path, writeback=False)
        else:
            self.store = shelve.Shelf({}, writeback=False)

    async def save(self):
        return await asyncio.to_thread(self.store.sync)

    def get(self, name):
        return self.store[name]

    def set(self, name, value):
        self.store[name] = value
        return True

    def delete(self, name):
        del self.store[name]
        return True

    def __contains__(self, name):
        return name in self.store

    @contextmanager
    def get_state_snapshot(self):
        # Transition to write-back mode
        if not self.save_path:
            yield MemoryState(self.store)
        else:
            wb = self.store.writeback
            try:
                self.store.sync()
                self.store.writeback = True
                with open(self.save_path, 'rb') as state:
                    yield state
            finally:
                self.store.sync()
                self.store.writeback = wb

    async def set_state_snapshot(self):
        if self.save_path:
            # TODO: Forbid SETs while SST?
            with open(self.save_path + "._sst") as sst:
                try:
                    success = True
                    while True:
                        block, boffs = (yield success)
                        if sst.tell() != boffs:
                            sst.seek(boffs, os.SEEK_SET)
                        written = sst.write(block)
                        success = written > 0
                except StopAsyncIteration:
                    pass

            os.rename(self.save_path + "._sst", self.save_path) 

            self.store.close()
            self.store = shelve.open(self.save_path, writeback=False)

        else:
            self.store.clear()
            try:
                while True:
                    block, boffs = (yield True)
                    # TODO: Ensure boffs is contiguous
                    block = io.BytesIO(block)
                    try:
                        while True:
                            k, v = pickle.load(block)
                            self.store[k] = v
                    except EOFError:
                        pass
            except StopAsyncIteration:
                pass

class KeyValueStore:
    def __init__(self, disk_path=None):
        self.state = ShelfStore(disk_path=disk_path)

    def get(self, key):
        return self.state.get(key)

    async def set(self, key, value, persist=True):
        self.state.set(key, value)
        if persist:
            await self.state.save()
        return True

    async def delete(self, key, persist=True):
        self.state.delete(key)
        if persist:
            await self.state.save()
        return True

    def __contains__(self, key):
        return key in self.state
