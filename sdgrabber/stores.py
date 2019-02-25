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

    def diff_schedules(self, schedules):
        old = self.load_schedules()
        yield from _diff(old, schedules)
        self._schedules = old

    def remove_schedule(self, key):
        del self._schedules[key]

    def diff_programs(self, programs):
        old = self.load_programs()
        yield from _diff(old, programs)
        self._programs = old

    def save(self):
        self.save_schedules(self._schedules)
        self.save_programs(self._programs)


class NullStore(BaseStore):
    def save_schedules(self, schedules):
        pass

    def load_schedules(self):
        return {}

    def save_programs(self, programs):
        pass

    def load_programs(self):
        return {}


class PickleStore(BaseStore):
    def __init__(self, path=None):
        self.path = path or tempfile.gettempdir()

    def save_schedules(self, schedules):
        with open(pathjoin(self.path, 'schedules.sdg'), 'wb') as f:
            pickle.dump(schedules, f)

    def load_schedules(self):
        path = pathjoin(self.path, 'schedules.sdg')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save_programs(self, programs):
        with open(pathjoin(self.path, 'programs.sdg'), 'wb') as f:
            pickle.dump(programs, f)

    def load_programs(self):
        path = pathjoin(self.path, 'programs.sdg')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)
