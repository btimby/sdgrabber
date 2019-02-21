import json

from datetime import datetime
from os.path import join as pathjoin

from .client import SDGrabber


def _sort_dict(d):
    s = {}
    for key, value in sorted(d.items()):
        if isinstance(value, dict):
            value = _sort_dict(value)
        s[key] = value
    return s


def dump(path=None, username=None, password=None):
    '''
    Writes json to files.

    Useful for using diff to verify diffing algorithm.
    '''

    path = path or '.'

    today = datetime.now().strftime('%Y%m%d-%H%M%S')

    api = SDGrabber(username=username, password=password)
    api.login()

    with open(pathjoin(path, 'lineups-%s.json' % today), 'w') as f:
        for lineup in api.get_lineups():
            json.dump(lineup.data, f, indent=2)

    with open(pathjoin(path, 'programs-%s.json' % today), 'w') as f:
        for program in api.get_programs():
            json.dump(program.data, f, indent=2)
