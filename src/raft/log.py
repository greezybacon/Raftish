import asyncio
from dataclasses import dataclass
from itertools import count
import os, os.path
import pickle

import logging
log = logging.getLogger('raft.log')

class TransactionLog(list):
    """
    The main thing in Raft. The transation log is replicated to all servers in
    the cluster and is guaranted to be exactly the same as the version on the
    current leader.

    LogEntry items are appended to the log. Subsequently, the server will
    indicate that a certain log index is considered "committed" which means it
    is safe to apply it to the application state machine locally. To do this,
    the application implementing Raft will need to register a callback via the
    ::add_apply_callback method. The callback will be called and supplied with
    the LogEntry instance of each and every LogEntry which gets
    applied/committed.

    Entries in the log are 1-based. 0 represents the index of an empty log, 1
    represents the first items and so on.
    """
    def __init__(self, disk_path):
        self.disk_path = os.path.join(disk_path or '.', 'transcation_log')
        self.lastApplied = 0
        self.apply_callbacks = set()
        self.apply_event = asyncio.Event()
        self.application_lock = asyncio.Lock()
        self.load()

    def load(self):
        try:
            with open(self.disk_path, 'rb') as logfile:
                self.extend(pickle.load(logfile))
        except FileNotFoundError:
            pass

    def save(self):
        try:
            # XXX: Maybe background, but ensure _log is not modified in the mean
            # time?
            with open(self.disk_path, 'wb') as logfile:
                pickle.dump(self, logfile)
        except IOError:
            raise

    def append(self, entry):
        if len(self):
            term = self.lastEntry.term
        else:
            term = 0

        if self.append_entries([entry], self.lastIndex, term):
            return self.lastIndex
        
        return False

    def append_entries(self, entries, previousIndex=None, previousTerm=None):
        if previousIndex > len(self):
            # Gap
            return False

        if previousIndex > 0:
            previous_entry = self.get(previousIndex)
            if previous_entry.term != previousTerm:
                # Log does not match and cannot be appended
                return False
            if len(entries) and previous_entry.term > min(x.term for x in entries):
                # Term mismatch
                return False

        elif previousIndex < 0:
            # Bogus index number
            return False

        if len(entries) > 0:
            # If there are existing log entries at the specified position, but
            # they are from a different term, then the existing entry and
            # everything that follows it needs to be deleted.
            if previousIndex < len(self):
                for index, entry in zip(count(previousIndex), entries):
                    # NOTE This is checking for REPLACING an entry which would
                    # mean to check the entry AFTER the previousIndex
                    if self.get(index).term != entry.term:
                        # Truncate the log after the last index
                        log.warning(f"Truncating log entries after {previousIndex}")
                        del self[previousIndex:]
                        break

                # else: Same index and term -- simple replacement?

            self[previousIndex:previousIndex+len(entries)] = entries

            # TODO: Commit to disk

        return True

    def commit(self, entry):
        pass

    @property
    def lastIndex(self):
        return len(self)

    @property
    def previousTerm(self):
        try:
            return self.lastEntry.term
        except AttributeError:
            return 0 

    @property
    def lastEntry(self):
        if len(self) == 0:
            return None

        return self[-1]

    def since(self, index, max_entries=10):
        # Start at the lesser of the requested ID and the last appended index
        start = min(index, self.lastIndex)
        # Stop at the lesser of MAX_ENTRIES from the end of the log
        stop = min(start + max_entries, self.lastIndex)
        # index is 1-based, so no need to add one
        return self[start:stop]

    def get(self, index):
        return self[index-1]

    def entryBefore(self, index):
        start = min(index, self.lastIndex)
        if start < 0:
            return None

        return self.get(start - 1)

    def add_apply_callback(self, callback):
        self.apply_callbacks.add(callback)

    async def apply_up_to(self, index):
        # Can't apply past the end of the local log
        assert index <= self.lastIndex

        # If index > lastApplied, then apply all items in the transaction log up
        # to entry.index
        while self.lastApplied < index:
            if not await self.apply(self.lastApplied + 1):
                return False

        return True

    async def apply(self, index):
        # It is safe to apply this index into the application. This is called
        # from the concensus backend. The local server
        assert self.lastApplied < index

        async with self.application_lock:
            entry: LogEntry = self.get(index)
            # XXX: If the apply fails for someone, then it will be resent for
            # everyone, which is probably outside the scope/intention of Raft.
            if not all(await asyncio.gather(*(
                    cb(entry)
                    for cb in self.apply_callbacks
                ))):
                return False

            self.lastApplied = index

            # XXX: Timeout?

        # Wake up anyone awaiting a transaction application
        self.apply_event.set()
        self.apply_event.clear()

        return True

@dataclass
class LogEntry:
    """
    Represents each item in the transaction log. This is used to represent
    individual transactions to the application.

    Note that the content should be kept small because all properties are sent
    in messages to other servers for application in their log.
    """
    term : int
    value : object