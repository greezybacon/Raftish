import asyncio
from dataclasses import dataclass
from enum import Enum

# Basic queue commands:
# ----
# CREATE queue
# DELETE queue
# SUBSCRIBE queue to channel
# UNSUBSCRIBE queue from channel
# PUBLISH message to channel
# FETCH message from queue

@dataclass
class ManageQueue:
    class Type(Enum):
        CREATE = 1
        DELETE = 2
        SUBSCRIBE = 3
        UNSUBSCRIBE = 4

    type: Type
    name: str
    channel: str = None

    @classmethod
    def create(self, name):
        return self(type=self.Type.CREATE, name=name)

    @classmethod
    def delete(self, name):
        return self(type=self.Type.DELETE, name=name)

    @classmethod
    def subscribe(self, queue, channel):
        return self(type=self.Type.SUBSCRIBE, name=queue, channel=channel)

    @classmethod
    def unsubscribe(self, queue, channel):
        return self(type=self.Type.UNSUBSCRIBE, name=queue, channel=channel)

    async def _create(self, context):
        return context.queues.create(self.name)

    async def _delete(self, context):
        return context.queues.delete(self.name)

    async def _subscribe(self, context):
        try:
            q = context.queues[self.name]
            return context.channels.subscribe(q, self.channel)
        except KeyError as e:
            return e

    async def _unsubscribe(self, context):
        q = context.queues[self.name]
        return context.channels.unsubscribe(q, self.channel)

    async def apply(self, context):
        handlers = {
            self.Type.CREATE: self._create,
            self.Type.DELETE: self._delete,
            self.Type.SUBSCRIBE: self._subscribe,
            self.Type.UNSUBSCRIBE: self._unsubscribe,
        }

        try:
            return await handlers[self.type](context)
        except Exception as e:
            return e

@dataclass
class PublishMessage:
    channel: str
    message: object

    async def apply(self, context):
        return await context.channels.publish(self.channel, self.message)

@dataclass
class FetchMessage:
    queue: str

    async def apply(self, context):
        try:
            q = context.queues[self.queue]
            return q.get_nowait()
        except asyncio.QueueEmpty as e:
            return e