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
            The absolut first index of the transaction log on disk. If the log
            was compacted and the stored log starts from some point after index
            1, this would be that index.
        """
        # TODO: Consider making the operation atomic by copying the part of the
        # file to be kept to a new file and adding the new stuff to the end and
        # then moving it into place.

        # Load the footer from the existing file
        start = time.monotonic()
        file_name = os.path.join(self.path, self.filename)
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
        start_chunk = starting // chunk_size
        footer.chunksInFile = start_chunk
        footer.firstIndex = first_index
        footer.lastIndex = start_chunk * chunk_size
        if (starting - start_index) % chunk_size > 0:
            # Will actually have to back up into the previous chunk
            pass
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
            starting=starting, offset=start_index)

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
        """
        if starting < 0:
            raise ValueError('`starting` value cannot be negatie')

        file_path = os.path.join(self.path, self.filename)
        try:
            with open(file_path, "rb") as infile:
                # Skip to the chunk where `starting` offset can be found
                if starting > 0:
                    footer, _ = self._read_footer(infile)
                    start_chunk = starting // footer.chunkSize
                    starting = starting % footer.chunkSize
                    infile.seek(footer.offsets[start_chunk], os.SEEK_SET)

                # Skip the header
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
        # TODO: Fast-forward the load process to skip to the first chunk with
        # <starting>
        for chunk in iter_from_chunks(self.load(starting), count):
            yield from chunk

    @lru_cache(maxsize=128)
    def get(self, index):
        entries = self.load_partial(1, index)
        try:
            return list(entries)[0]
        except IndexError:
            log.exception(f"Failed to load index {index}")
            raise

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

def iter_from_chunks(chunks, count=None, start_offset=0):
    for chunk in chunks:
        if start_offset > 0:
            if start_offset < len(chunk):
                if count:
                    yield (C := chunk[start_offset:start_offset+count])
                    count -= len(C)
                else:
                    yield chunk[start_offset:]
            start_offset -= len(chunk)
        elif count:
            yield chunk[:count]
            count -= len(chunk)
        else:
            yield chunk
        if count <= 0:
            break