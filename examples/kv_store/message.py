import asyncio
import pickle
import struct

def send_message(writer: asyncio.StreamWriter, message):
    msg = pickle.dumps(message)
    writer.write(struct.pack('!I', len(msg)))
    writer.write(msg)

async def receive_message(reader: asyncio.StreamReader):
    hsize = struct.calcsize('!I')
    mlen, = struct.unpack('!I', await reader.readexactly(hsize))
    return pickle.loads(await reader.readexactly(mlen))
