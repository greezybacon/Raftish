import asyncio
import pickle

def send_message(writer, message):
    message = pickle.dumps(message)
    sizeof = b'%10d' % (len(message),)
    writer.write(sizeof)

    if isinstance(message, str):
        message = message.encode('utf-8')

    writer.write(message)

async def receive_message(reader):
    try:
        length = await reader.readexactly(10)
        response = await reader.readexactly(int(length, 10))
        return pickle.loads(response)
    except asyncio.IncompleteReadError:
        raise