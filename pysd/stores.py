import abc
import tempfile
import pickle

from os.path import isfile
from os.path import join as pathjoin


class BaseStore(abc.ABC):
    @abc.abstractmethod
    def save_lineups(self, hashes):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_lineups(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def save_schedules(self, hashes):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_schedules(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def save_programs(self, hashes):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_programs(self):
        raise NotImplementedError()

    def _diff(self, old, new):
        for key, hash in (item for item in new if item[0] not in old):
            old[key] = hash
            yield key

    def diff_lineups(self, lineups):
        old = self.load_lineups()
        yield from self._diff(old, lineups)
        self._lineups = old

    def diff_schedules(self, schedules):
        old = self.load_schedules()
        yield from self._diff(old, schedules)
        self._schedules = old

    def diff_programs(self, programs):
        old = self.load_programs()
        yield from self._diff(old, programs)
        self._programs = old

    def save(self):
        self.save_lineups(self._lineups)
        self.save_schedules(self._schedules)
        self.save_programs(self._programs)


class NullStore(BaseStore):
    def save_schedules(self, hashes):
        pass

    def load_schedules(self):
        return {}

    def save_lineups(self, hashes):
        pass

    def load_lineups(self):
        return {}

    def save_programs(self, hashes):
        pass

    def load_programs(self):
        return {}


class PickleStore(BaseStore):
    def __init__(self, path=None):
        self.path = path or tempfile.gettempdir()

    def save_schedules(self, schedules):
        with open(pathjoin(self.path, 'schedules.pysd'), 'wb') as f:
            pickle.dump(schedules, f)

    def load_schedules(self):
        path = pathjoin(self.path, 'schedules.pysd')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save_lineups(self, schedules):
        with open(pathjoin(self.path, 'lineups.pysd'), 'wb') as f:
            pickle.dump(schedules, f)

    def load_lineups(self):
        path = pathjoin(self.path, 'lineups.pysd')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save_programs(self, programs):
        with open(pathjoin(self.path, 'programs.pysd'), 'wb') as f:
            pickle.dump(programs, f)

    def load_programs(self):
        path = pathjoin(self.path, 'programs.pysd')
        if not isfile(path):
            return {}
        with open(path, 'rb') as f:
            return pickle.load(f)
