import asyncio
from dataclasses import dataclass
import os, os.path
import pickle

import logging
log = logging.getLogger('raft.log')

class TransactionLog:
    def __init__(self, disk_path):
        self.disk_path = os.path.join(disk_path or '.', 'transcation_log')
        self.lastApplied = -1
        self.apply_callbacks = set()
        self.apply_evet = asyncio.Event()
        self.application_lock = asyncio.Lock()
        self.load()

    def load(self):
        try:
            with open(self.disk_path, 'rb') as logfile:
                self._log = pickle.load(logfile)
        except FileNotFoundError:
            self._log = []

    def save(self):
        try:
            # XXX: Maybe background, but ensure _log is not modified in the mean
            # time?
            with open(self.disk_path, 'wb') as logfile:
                pickle.dump(self._log, logfile)
        except IOError:
            raise

    def append(self, entry):
        if len(self._log):
            term = self.lastEntry.term
        else:
            term = 0

        if self.append_entries([entry], self.lastIndex, term):
            return self.lastIndex
        
        return False

    def append_entries(self, entries, previousIndex=None, previousTerm=None):
        if previousIndex >= len(self._log):
            # Gap
            return False

        if previousIndex >= 0:
            previous_entry = self._log[previousIndex]
            if previous_entry.term != previousTerm:
                # Log does not match and cannot be appended
                return False
            if len(entries) and previous_entry.term > min(x.term for x in entries):
                # Term mismatch
                return False

        elif previousIndex < -1:
            # Bogus index number
            return False

        if len(entries) > 0:
            # If there are existing log entries at the specified position, but
            # they are from a different term, then the existing entry and
            # everything that follows it needs to be deleted.
            if previousIndex + 1 < len(self._log):
                if self._log[previousIndex+1].term != entries[0].term:
                    # Truncate the log after the last index
                    log.warning(f"Truncating log entries after {previousIndex+1}")
                    del self._log[previousIndex+1:]
                else:
                    # Same index and term -- simple replacement?
                    pass

            self._log[previousIndex+1:previousIndex+1+len(entries)] = entries

            # TODO: Commit to disk

        return True

    def commit(self, entry):
        pass

    @property
    def lastIndex(self):
        try:
            return len(self._log) - 1
        except IndexError:
            return -1 

    @property
    def previousTerm(self):
        try:
            return self.lastEntry.term
        except AttributeError:
            return 0 

    @property
    def lastEntry(self):
        if len(self._log) == 0:
            return None

        return self._log[self.lastIndex]

    def since(self, index, max_entries=100):
        # Start at the lesser of the requested ID and the last appended index
        start = min(index+1, self.lastIndex)

        # Stop at the lesser of MAX_ENTRIES from the end of the log
        stop = min(start + max_entries, self.lastIndex+1)
        return self._log[start:stop]

    def entryBefore(self, index):
        start = min(index, self.lastIndex)
        if start < 0:
            return None

        return self._log[start]


    def add_apply_callback(self, callback):
        self.apply_callbacks.add(callback)

    async def apply_up_to(self, index):
        while self.lastApplied < index:
            if not self.apply(self.lastApplied):
                return False

        return True

    async def apply(self, index):
        # It is safe to apply this index into the application. This is called
        # from the concensus backend. The local server
        assert self.commitIndex < entry.index
        assert self.lastApplied < entry.index

        # TODO: If entry.index > lastApplied, then apply all items in the
        # transaction log up to entry.index

        with self.application_lock:
            entry = self._log[index]

            if not all(await asyncio.gather(*(
                    cb(entry)
                    for cb in self.apply_callbacks
                ))):
                return False

            self.lastApplied = entry.index

            # XXX: Timeout?

        self.apply_event.set()
        self.apply_event.clear()

        return True

    def __len__(self):
        return len(self._log)

    def __getitem__(self, index):
        return self._log[index]

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