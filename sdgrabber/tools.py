import json
import glob

from datetime import datetime, timedelta
from os.path import join as pathjoin

from .client import SDGrabber
from .models import _parse_datetime


def _sort_dict(d):
    s = {}
    for key, value in sorted(d.items()):
        if isinstance(value, dict):
            value = _sort_dict(value)
        elif isinstance(value, list):
            value = sorted(value)
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
            f.write('\n')

    with open(pathjoin(path, 'programs-%s.json' % today), 'w') as f:
        for program in api.get_programs():
            json.dump(program.data, f, indent=2)
            f.write('\n')


def validate(path=None):
    '''
    Validate time slots for each channel in a dump file.
    '''

    if path is None:
        dumps = {}
        for fn in glob.glob('programs-*-*.json'):
            dt = datetime.strptime(fn[9:-6], '%Y%m%d-%H%M%S')
            dumps[dt] = fn
        path = dumps[max(dumps.keys())]

    schedules = {}
    with open(path, 'r') as f:
        lines = []
        while True:
            line = f.readline()
            if line != '':
                lines.append(line)
                continue

            obj = json.loads(''.join(lines))
            lines.clear()

            start = _parse_datetime(obj['airDateTime'])
            duration = int(obj['duration'])
            schedules.setdefault(obj['stationID'], []) \
                .append((start, duration, obj['programID']))

    for station_id, schedule in schedules.items():
        schedule = sorted(schedule)
        for i in range(len(schedule)):
            curr_start, curr_duration, curr_program_id = schedule[i]
            try:
                next_start, _, next_program_id = schedule[i+1]

            except IndexError:
                break

            if curr_start + timedelta(seconds=curr_duration) > next_start:
                print('Schedule conflict: %s: %s @%s %s @ %s' % (
                    station_id, curr_program_id, curr_start, next_program_id,
                    next_start))
