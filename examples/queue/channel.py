class Channel:
    def __init__(self, name):
        self.name = name

    async def publish(self, message):
        for q in self.queues:
            await q.add(message)