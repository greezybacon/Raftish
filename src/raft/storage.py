import asyncio
from dataclasses import dataclass, field
from functools import lru_cache
import os, os.path
import pickle
import struct
import time
from typing import Iterable, Type

import logging
log = logging.getLogger('raft.storage')

class LogStorageBackend:
    """
    Backend which can load or persist a TransactionLog to and from disk.

    Format:
    The on-disk format for the transaction log is a number of pickle objects
    followed by a single 4-byte integer:

        pickle<list[LogEntry]>
        pickle<list[LogEntry]>
        ...
        pickle<LogStorageBackend.Footer>
        sizeof<Footer>

    Seeking the file is simplest by reading the 4-byte footer-size at the end of
    the file. Then seek to the end of the file minus that size, minus the
    4-bytes. Then read the Footer.

    The Footer object includes a list of location offsets of each of the chunks.
    To read all the chunks, start reading with pickle.load() from the beginning
    of the file. To read a specific chunk, use the offset list in the footer to
    find the location of the desired chunk. Then seek to that location and use
    pickle.load to read the chunk.
    """
    @dataclass
    class Footer:
        startIndex: int             # First index in THIS file
        chunkSize: int              # Size of the chunks
        chunksInFile: int = 0       # Number of chunks in THIS file
        firstIndex: int = 0         # Absolute start of the entire log
        lastIndex: int = 0          # Absolute end of the entire log
        sequence: int = 1           # Number of this file among all
        offsets: list[int] = field(default_factory=list)

    filename = "transactions"

    def __init__(self, path, chunk_size=100):
        self.path = path
        self.chunk_size = chunk_size

        # Write-ahead log init
        self.wal = WriteAheadLog(path, chunk_size)
        self.wal_flush_task = None

    async def startup(self):
        await self.wal.load(self)

    @property
    def _footer_size(self):
        return struct.calcsize("!I")

    def _read_footer(self, open_file):
        open_file.seek(-self._footer_size, os.SEEK_END)
        footer_offset, = struct.unpack("!I", open_file.read(self._footer_size))
        open_file.seek(footer_offset, os.SEEK_SET)
        return pickle.load(open_file), footer_offset

    def save(self, transactions: Iterable, starting=0, start_index=0,
        first_index=1):
        """
        Commit the transcations to the write-ahead log. And then schedule the
        write-ahead log to be flushed after some time.
        """
        if self.wal_flush_task is not None:
            self.wal_flush_task.cancel()

        self.wal.extend(transactions[starting - start_index:], starting)
        self.wal_flush_task = asyncio.create_task(self.delay_flush())

        # TODO: If the WAL log is over N entries, then pause and replay all or
        # part of it.

    async def delay_flush(self, delay=5):
        try:
            await asyncio.sleep(delay)
            # This doesn't need to be shielded because it's not async and so
            # will not yield. So there'd be no opportunity to switch and cancel
            await asyncio.shield(self.wal.replay(self))
            self.wal.clear()
        except asyncio.CancelledError:
            pass
        except:
            log.exception("Error in WAL replay")

    @property
    def fullpath(self):
        return os.path.join(self.path, self.filename)

    def write(self, transactions: Iterable, starting=0, start_index=0,
        first_index=1):
        """
        Write the transactions out to disk.

        Parameters:
        transactions: iterable[LogEntry]
            The transaction to be written. Note the iterable must support
            slicing.
        starting: int = 0
            The first item to actually be written
        start_index: int = 0
            The absolute index of the first item in the transactions list. This
            would generally be if the list were truncated after application.
            When writing, the complete transaction list is no longer available
            nor provided to this method.
        first_index: int = 1
            The absolute first index of the transaction log on disk. If the log
            was compacted and the stored log starts from some point after index
            1, this would be that index.
        """
        # TODO: Consider making the operation atomic by copying the part of the
        # file to be kept to a new file and adding the new stuff to the end and
        # then moving it into place.

        # Load the footer from the existing file
        start = time.monotonic()
        file_name = self.fullpath
        try:
            with open(file_name, 'rb') as infile:
                footer, footer_offset = self._read_footer(infile)
        except FileNotFoundError:
            # XXX: The transaction log needs the concept of an offset/firstIndex
            # to support truncating for snapshots.
            footer = self.Footer(startIndex=first_index,
                chunkSize=self.chunk_size, offsets=[0])
            footer_offset = 0

        # What chunk do we want to start writing
        chunk_size = footer.chunkSize
        start_chunk = (starting - first_index) // chunk_size
        footer.chunksInFile = start_chunk
        footer.firstIndex = first_index
        footer.lastIndex = start_chunk * chunk_size

        if start_chunk < len(footer.offsets):
            start_offset = footer.offsets[start_chunk]
            del footer.offsets[start_chunk:]
        else:
            # New chunk
            start_offset = footer_offset

        # Truncate the file at the offset plus header size
        if os.path.exists(file_name):
            os.truncate(file_name, start_offset)

        # Write the chunks starting as requested
        starting -= starting % chunk_size
        chunks = chunkize(transactions, chunk_size,
            starting=0, offset=start_index)

        with open(file_name, 'ab') as outfile:
            # (re)write new chunks
            outfile.seek(start_offset, os.SEEK_SET)
            for i, chunk in enumerate(chunks, start=start_chunk):
                if start_chunk <= len(footer.offsets):
                    footer.offsets.append(outfile.tell())
                else:
                    footer.offsets[i] = outfile.tell()
                footer.chunksInFile += 1
                footer.lastIndex += len(chunk)
                pickle.dump(chunk, outfile)

            # Now write the footer
            footer_offset = outfile.tell()
            pickle.dump(footer, outfile)

            # Now write the footer_size
            outfile.write(struct.pack("!I", footer_offset))

        elapsed = time.monotonic() - start
        log.debug(f"Updating log file took {elapsed:.3f}s")

    def load(self, starting: int=0):
        """
        Yields "chunks" of the log file. The size of each chunk is configured by
        the ::chunk_size instance property.

        Parameters:
        starting: int = 0
            Start loading at index <starting+1> (instead of starting from the
            beginning of the log on disk)
        """
        if starting < 0:
            raise ValueError('`starting` value cannot be negatie')

        file_path = self.fullpath
        try:
            with open(file_path, "rb") as infile:
                # Skip to the chunk where `starting` offset can be found
                if starting > 0:
                    footer, _ = self._read_footer(infile)
                    start_chunk = starting // footer.chunkSize
                    starting = starting % footer.chunkSize
                    infile.seek(footer.offsets[start_chunk], os.SEEK_SET)

                while True:
                    chunk = pickle.load(infile)
                    # The footer represents the end of the file
                    if type(chunk) is self.Footer:
                        break
                    if starting:
                        yield chunk[starting:]
                        starting = 0
                    else:
                        yield chunk
        except FileNotFoundError:
            pass

    def load_partial(self, count, starting=0):
        # Fast-forward the load process to skip to the first chunk with
        # <starting>
        for chunk in iter_from_chunks(self.load(starting), count):
            yield from chunk

    @lru_cache(maxsize=128)
    def get(self, index):
        try:
            entries = self.load_partial(1, index)
            return list(entries)[0]
        except IndexError:
            # TODO: Attempt to fetch from WAL
            return self.wal.try_find(index)

    def purge(self):
        file_path = os.path.join(self.path, self.filename)
        if os.path.exists(file_path):
            os.unlink(file_path)

def chunkize(iterable, chunk_size, starting=0, offset=0):
    l = len(iterable)
    # Adjust starting to start at an even chunk_size offset
    starting -= starting % chunk_size
    start = starting - offset
    while start < l:
        yield iterable[start:start+chunk_size]
        start += chunk_size

def iter_from_chunks(chunks, count=None):
    for chunk in chunks:
        if count:
            yield chunk[:count]
            count -= len(chunk)
            if count <= 0:
                break
        else:
            yield chunk

# Impelement a sort-of write-ahead-log where the entries are written out
# incrementally to a separate file. Then, when the file reaches a certain number
# of items, or after some timeout, the items are added as a "chunk" to the
# storage log.
class WriteAheadLog(list):
    @dataclass
    class Entry:
        startIndex: int         # Where these entries start with respect to the
                                # start of the stored log
        items: tuple[object]    # The entries

    filename = "transactions.wal"

    def __init__(self, path, chunk_size):
        self.path = path
        self.chunk_size = chunk_size
        self.entries = []
        super().__init__()

    async def load(self, backend: LogStorageBackend):
        try:
            with self.open('rb') as walfile:
                try:
                    while True:
                        super().append(pickle.load(walfile))
                except EOFError:
                    pass
            await self.replay(backend)
        except FileNotFoundError:
            pass

    async def replay(self, backend: LogStorageBackend):
        for i, (offset, chunk) in enumerate(self.chunkize()):
            log.info(f"Committing WAL chunk @{offset}+#{len(chunk)}")
            backend.write(chunk, starting=offset)
            if i % 10 == 9:
                await asyncio.sleep(0)

    def open(self, mode='ab'):
        return open(os.path.join(self.path, self.filename), mode)

    def extend(self, items, start_offset):
        with self.open() as walfile:
            entry = self.Entry(
                startIndex=start_offset,
                items=tuple(items)
            )
            pickle.dump(entry, walfile)
        super().append(entry)

    def clear(self):
        super().clear()
        with self.open('wb'):
            pass

    def chunkize(self):
        """
        Construct an iterable list of chunks of contiguous chunks where all the
        items in the chunk come immediately after one another in the log.
        """
        if len(self) == 0:
            yield []
            return

        firstIndex = startIndex = self[0].startIndex
        chunk = []
        for entry in self:
            if entry.startIndex != startIndex:
                yield firstIndex, chunk
                chunk = []
                firstIndex = entry.startIndex

            chunk.extend(entry.items)
            startIndex = entry.startIndex + len(entry.items)

        yield firstIndex, chunk

    def try_find(self, index):
        for entry in reversed(self):
            if entry.startIndex <= index <= (entry.startIndex + len(entry.items)):
                return entry.items[index - entry.startIndex]

        log.error(f"Cannot find index {index} in {self}")
        raise IndexError