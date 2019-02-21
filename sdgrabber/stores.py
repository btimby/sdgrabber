import abc
import tempfile
import pickle

from os.path import isfile
from os.path import join as pathjoin


def _diff(old, new):
    '''
    Compares two different data structures.

    - old is a dictionary {key: value}
    - new is a list of tupes: (key, value)

    We want anything in new where the key or value does not match. Like
    set(new).difference(list(old.items())) without the conversion.
    '''
    for key, hash in (item for item in new
                      if old.get(item[0], None) != item[1]):
        old[key] = hash
        yield key


class BaseStore(abc.ABC):
    @abc.abstractmethod
    def save_lineups(self, lineups):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_lineups(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def save_schedules(self, schedules):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_schedules(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def save_programs(self, programs):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_programs(self):
        raise NotImplementedError()

    def diff_lineups(self, lineups):
        old = self.load_lineups()
        yield from _diff(old, lineups)
        self._lineups = old

    def diff_schedules(self, schedules):
        old = self.load_schedules()
        yield from _diff(old, schedules)
        self._schedules = old

    def diff_programs(self, programs):
        old = self.load_programs()
        yield from _diff(old, programs)
        self._programs = old

    def save(self):
        self.save_lineups(self._lineups)
        self.save_schedules(self._schedules)
        self.save_programs(self._programs)


class NullStore(BaseStore):
    def save_schedules(self, schedules):
        pass

    def load_schedules(self):
        return {}

    def save_lineups(self, lineups):
        pass

    def load_lineups(self):
        return {}

    def save_programs(self, programs):
        pass

    def load_programs(self):
        return {}


class PickleStore(BaseStore):
    def __init__(self, path=None):
        self.path = path or tempfile.gettempdir()

    def save_schedules(self, schedules):
        with open(pathjoin(self.path, 'schedules.sd'), 'wb') as f:
            pickle.dump(schedules, f)

    def load_schedules(self):
        path = pathjoin(self.path, 'schedules.sd')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save_lineups(self, lineups):
        with open(pathjoin(self.path, 'lineups.sd'), 'wb') as f:
            pickle.dump(lineups, f)

    def load_lineups(self):
        path = pathjoin(self.path, 'lineups.sd')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save_programs(self, programs):
        with open(pathjoin(self.path, 'programs.sd'), 'wb') as f:
            pickle.dump(programs, f)

    def load_programs(self):
        path = pathjoin(self.path, 'programs.sd')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)