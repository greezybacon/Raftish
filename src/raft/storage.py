from dataclasses import dataclass, field
import os, os.path
import pickle
import struct
import time
from typing import Iterable

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
        startIndex: int
        chunkSize: int
        chunksInFile: int = 0
        lastIndex: int = 0
        offsets: list[int] = field(default_factory=list)

    filename = "transactions"

    def __init__(self, path, chunk_size=1000):
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

    def save(self, transactions: Iterable, starting=0, start_index=0):
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
            footer = self.Footer(startIndex=start_index,
                chunkSize=self.chunk_size, offsets=[0])
            footer_offset = 0

        # What chunk do we want to start writing
        chunk_size = footer.chunkSize
        start_chunk = starting // chunk_size
        footer.chunksInFile = start_chunk
        if start_chunk < len(footer.offsets):
            start_offset = footer.offsets[start_chunk]
        else:
            # New chunk
            start_offset = footer_offset
            footer.offsets.append(start_offset)

        # Truncate the file at the offset plus header size
        if os.path.exists(file_name):
            os.truncate(file_name, start_offset)

        # Write the chunks starting as requested
        starting -= starting % chunk_size
        chunks = chunkize(transactions, chunk_size, starting=starting)

        with open(file_name, 'ab') as outfile:
            # (re)write new chunks
            outfile.seek(start_offset, os.SEEK_SET)
            for i, chunk in enumerate(chunks, start=start_chunk):
                footer.offsets[i] = outfile.tell()
                footer.chunksInFile += 1
                pickle.dump(chunk, outfile)

            # Now write the footer
            footer_offset = outfile.tell()
            pickle.dump(footer, outfile)

            # Now write the footer_size
            outfile.write(struct.pack("!I", footer_offset))

        elapsed = time.monotonic() - start
        log.debug(f"Updating log file took {elapsed:.3f}s")

    def load(self, starting=0):
        """
        Yields "chunks" of the log file. The size of each chunk is configured by
        the ::chunk_size instance property.
        """
        assert starting == 0
        file_path = os.path.join(self.path, self.filename)
        try:
            with open(file_path, "rb") as infile:
                log.info(f"Loading transaction log from file {file_path}")
                # Skip the header
                while True:
                    chunk = pickle.load(infile)
                    # The footer represents the end of the file
                    if type(chunk) is self.Footer:
                        break
                    yield chunk
        except FileNotFoundError:
            pass

def chunkize(iterable, chunk_size, starting=0):
    l = len(iterable)
    # Adjust starting to start at an even chunk_size offset
    starting -= starting % chunk_size
    start = starting
    while start < l:
        yield iterable[start:start+chunk_size]
        start += chunk_size