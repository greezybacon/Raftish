import asyncio
import shelve

class ShelfStore:
    def __init__(self, disk_path=None):
        if disk_path is not None:
            self.save_path = disk_path
            self.store = shelve.open(disk_path, writeback=False)
        else:
            self.store = shelve.Shelf({}, writeback=False)

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

