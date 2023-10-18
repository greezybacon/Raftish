import asyncio
from enum import Enum

class Channels(dict):
    def subscribe(self, q, name):
        membership = self.setdefault(name, set())
        membership.add(q)

    def unsubscribe(self, q, name):
        membership = self.setdefault(name, set())
        if q in membership:
            membership.remove(q)

    async def publish(self, name, message):
        for q in self.get(name, set()):
            await q.put(message)

class Queue(asyncio.Queue):
    class DeliveryPolicy(Enum):
        AT_MOST_ONCE = 1
        AT_LEAST_ONCE = 2
        EXACTLY_ONCE = 3

    async def wait_nonempty(self):
        """
        A copy of most of Queue::wait(), but only the not-empty wait part.
        """
        while self.empty():
            getter = self._get_loop().create_future()
            self._getters.append(getter)
            try:
                await getter
            except:
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    # Clean self._getters from canceled getters.
                    self._getters.remove(getter)
                except ValueError:
                    # The getter could be removed from self._getters by a
                    # previous put_nowait call.
                    pass
                if not self.empty() and not getter.cancelled():
                    # We were woken up by put_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._getters)
                raise

class Queues(dict):
    def create(self, name):
        self[name] = Queue()

    def delete(self, name):
        del self[name]