import asyncio
from dataclasses import dataclass
import os, os.path
import pickle

class TransactionLog:
    def __init__(self, disk_path):
        self.disk_path = os.path.join(disk_path or '.', 'transcation_log')
        self.commitIndex = 0
        self.lastApplied = -1
        self.apply_callbacks = set()
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

    def append_entries(self, entries, previousIndex=None, previousTerm=None):
        if previousIndex >= len(self._log):
            # Gap
            return False

        if previousIndex >= 0:
            previous_entry = self._log[previousIndex]
            if previous_entry.term != previousTerm:
                # Log does not match and cannot be appended
                return False
            if previous_entry.term > min(x.term for x in entries):
                # Term mismatch
                return False

        elif previousIndex < -1:
            # Bogus index number
            return False

        if previousIndex + 1 < len(self._log):
            # Truncate the log after the last index
            del self._log[previousIndex+1:]

        self._log[previousIndex+1:previousIndex+1+len(entries)] = entries
        self.lastApplied = len(self._log) - 1

        # TODO: Commit to disk

        return True

    def commit(self, entry):
        pass

    @property
    def previousIndex(self):
        try:
            return self.previousEntry.index
        except IndexError:
            return -1 

    @property
    def previousTerm(self):
        try:
            return self.previousEntry.term
        except IndexError:
            return 0 

    @property
    def previousEntry(self):
        return self._log[self.lastApplied]

    def add_apply_callback(self, callback):
        self.apply_callbacks.add(callback)

    async def apply(self, entry):
        # It is safe to apply this index into the application. This is called
        # from the concensus backend. The local server
        assert self.commitIndex < entry.index

        # TODO: If entry.index > lastApplied, then apply all items in the
        # transaction log up to entry.index

        with self.application_lock:
            self.commitIndex = entry.index

            await asyncio.gather(*(
                cb(entry)
                for cb in self.apply_callbacks
            ))
            # XXX: Timeout?

    def __len__(self):
        return len(self._log)

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