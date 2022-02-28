import asyncio
from dataclasses import dataclass, field
from functools import lru_cache
import os, os.path
import pickle
import shutil
import time
from typing import Iterable

import logging

log = logging.getLogger('raft.storage')

class PersistenceBase:
    async def startup(self):
        raise NotImplemented

    def save(self, transactions: Iterable, starting=0, start_index=0,
        first_index=1):
        """
        Commit the transcations to the write-ahead log. And then schedule the
        write-ahead log to be flushed after some time.
        """
        raise NotImplemented

    def load(self, starting: int=0):
        """
        Yields "chunks" of the log file. The size of each chunk is configured by
        the ::chunk_size instance property.

        Parameters:
        starting: int = 0
            Start loading at index <starting+1> (instead of starting from the
            beginning of the log on disk)
        """
        raise NotImplemented

    def load_partial(self, count: int, starting: int=0):
        # Fast-forward the load process to skip to the first chunk with
        # <starting>
        for chunk in iter_from_chunks(self.load(starting), count):
            yield chunk

    def get(self, index):
        raise NotImplemented

    def purge(self):
        raise NotImplemented

class LogStorageBackend(PersistenceBase):
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
    class HeaderEntry:
        startIndex: int             # First index in THIS file
        chunkSize: int              # Size of the chunks
        filename: str = ''          # Name of THIS file
        chunksInFile: int = 0       # Number of chunks in THIS file
        firstIndex: int = 0         # Absolute start of the entire log
        lastIndex: int = 0          # Absolute end of the entire log
        sequence: int = 1           # Number of this file among all
        offsets: list[int] = field(default_factory=list)

    filename = "transactions"

    def __init__(self, path, chunk_size=200, wal_max_length=100):
        self.path = path
        self.chunk_size = chunk_size

        # Write-ahead log init
        self.wal = WriteAheadLog(path, chunk_size)
        self.wal_flush_task = None
        self.wal_max_len = wal_max_length

    async def startup(self):
        await self.wal.load(self)

    def _read_dir(self) -> 'LogStorageBackend.HeaderEntry':
        with open(self.fullpath + ".dir", 'rb') as header_file:
            return pickle.load(header_file)

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

    async def delay_flush(self, delay=5):
        try:
            # If the WAL log is over N entries, then don't pause and replay it
            # immediately.
            await asyncio.sleep(delay)
            await self.wal.replay_and_clear(self)
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
            footer  = self._read_dir()
        except FileNotFoundError:
            # XXX: The transaction log needs the concept of an offset/firstIndex
            # to support truncating for snapshots.
            footer = self.HeaderEntry(startIndex=first_index,
                chunkSize=self.chunk_size, offsets=[0])

        # What chunk do we want to start writing
        chunk_size = footer.chunkSize
        start_chunk = (starting - first_index + 1) // chunk_size
        footer.chunksInFile = start_chunk
        footer.firstIndex = first_index
        footer.lastIndex = start_chunk * chunk_size
        footer.filename = file_name

        # If the write starts in the middle of a chunk, load the current chunk
        # so the new data can be placed in it.
        offset = (starting - first_index + 1) % chunk_size
        last_chunk = []

        # Truncate the chunk offsets if replacing a chunk before the end. (The
        # file will be truncated below.)
        if start_chunk < len(footer.offsets):
            start_offset = footer.offsets[start_chunk]
            del footer.offsets[start_chunk:]
            if offset > 0:
                last_chunk = next(self.load(starting - offset))
        else:
            # New chunk
            start_offset = os.path.getsize(self.fullpath)

        # Truncate the file at the offset plus header size
        if os.path.exists(file_name):
            os.truncate(file_name, start_offset)

        # Write the chunks starting as requested
        chunks = chunkize(transactions, chunk_size, chunk_size - offset)

        with open(file_name, 'ab') as outfile:
            outfile.seek(start_offset, os.SEEK_SET)
            for i, chunk in enumerate(chunks, start=start_chunk):
                if i >= len(footer.offsets):
                    footer.offsets.append(outfile.tell())
                else:
                    footer.offsets[i] = outfile.tell()
                footer.chunksInFile += 1
                footer.lastIndex += len(chunk)

                # Rewrite the last chunk of the file to include the new edit
                if offset != 0:
                    last_chunk[offset:] = chunk
                    chunk = last_chunk
                    offset = 0
                pickle.dump(chunk, outfile)

        # Now write the footer
        with open(self.fullpath + "._dir", 'wb') as header_file:
            pickle.dump(footer, header_file)

        shutil.move(self.fullpath + "._dir", self.fullpath + ".dir")

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
            raise ValueError('`starting` value cannot be negative')

        file_path = self.fullpath
        try:
            with open(file_path, "rb") as infile:
                # Skip to the chunk where `starting` offset can be found
                if starting > 0:
                    footer = self._read_dir()
                    start_chunk = starting // footer.chunkSize
                    if start_chunk >= len(footer.offsets):
                        raise ValueError(f"`starting` ({starting}) value beyond end of log")
                    starting = starting % footer.chunkSize
                    infile.seek(footer.offsets[start_chunk], os.SEEK_SET)

                while True:
                    chunk = pickle.load(infile)
                    if starting:
                        yield chunk[starting:]
                        starting = 0
                    else:
                        yield chunk
        except FileNotFoundError:
            pass
        except EOFError:
            # End of input
            pass

    @lru_cache(maxsize=128)
    def get(self, index):
        try:
            for chunk in self.load_partial(1, index):
                return chunk[0]
        except (IndexError, ValueError):
            log.info(f"Log doesn't have entry at offset {index}. Searching WAL")
            # TODO: Attempt to fetch from WAL
            return self.wal.try_find(index)

    def purge(self):
        file_path = os.path.join(self.path, self.filename)
        if os.path.exists(file_path):
            os.unlink(file_path)

def chunkize(iterable, chunk_size, first_chunk_size=None):
    l = len(iterable)
    start = 0
    # Adjust starting to start at an even chunk_size offset
    while start < l:
        if first_chunk_size:
            yield iterable[:first_chunk_size]
            start += first_chunk_size
            first_chunk_size = None 
        else:
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
    alt_filename = "transactions._wal"

    def __init__(self, path, chunk_size):
        self.path = path
        self.chunk_size = chunk_size
        self.entries = []
        self.replay_lock = asyncio.Lock()
        super().__init__()

    async def load(self, backend: LogStorageBackend):
        try:
            with self.open('rb') as walfile:
                try:
                    while True:
                        self.append(pickle.load(walfile))
                except EOFError:
                    pass
            await self.replay_and_clear(backend)
        except FileNotFoundError:
            pass

    async def replay(self, backend: LogStorageBackend):
        async with self.replay_lock:
            log.info(f"Committing WAL log ({len(self)} chunks)")
            for i, (offset, chunk) in enumerate(self.chunkize()):
                backend.write(chunk, starting=offset)
                if i % 10 == 9:
                    await asyncio.sleep(0)

    async def replay_and_clear(self, backend: LogStorageBackend):
        """
        Replay the log up to the current position and then clear up to that
        position. This allows for the WAL to be appended to while it is also
        being applied/replayed.
        """
        # XXX: There's a race here where starting replay_and_clear concurrently
        # would probably arrive at corruption.
        size = len(self)
        await self.replay(backend)
        self.clear(up_through=size)

    def open(self, mode='ab'):
        return open(os.path.join(self.path, self.filename), mode)

    def open_alt(self, mode='wb'):
        return open(os.path.join(self.path, self.alt_filename), mode)

    def commit_alt(self):
        shutil.move(os.path.join(self.path, self.alt_filename),
            os.path.join(self.path, self.filename))

    def extend(self, items, start_offset):
        with self.open() as walfile:
            entry = self.Entry(
                startIndex=start_offset,
                items=tuple(items)
            )
            pickle.dump(entry, walfile)
        self.append(entry)

    def clear(self, up_through=None):
        if up_through is not None:
            remainder = self[up_through:]

        super().clear()
        with self.open_alt('wb') as walfile:
            if up_through is not None and remainder:
                for entry in remainder:
                    pickle.dump(entry, walfile)
                    self.append(entry)

        self.commit_alt()

    def chunkize(self):
        """
        Construct an iterable list of chunks of contiguous chunks where all the
        items in the chunk come immediately after one another in the log.
        """
        if len(self) == 0:
            yield 0, []
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