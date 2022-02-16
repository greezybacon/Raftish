class ConfigurationError(Exception): pass
class NotALeader(Exception): pass

class NewTermError(Exception):
    def __init__(self, term, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term = term