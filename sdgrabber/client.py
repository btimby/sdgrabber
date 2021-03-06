import os
import requests
import hashlib
import logging
import json

from functools import wraps
from itertools import groupby, chain, islice

from .models import (
    _parse_datetime, StatusModel, StatusLineupModel, ProgramModel, LineupModel,
    ScheduleModel,
)
from .stores import NullStore


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class ErrorResponse(Exception):
    def __init__(self, message, data=None):
        super().__init__(message)
        self.data = data


class LoginRequired(Exception):
    def __init__(self):
        super().__init__('Call login() first.')


class NoCredentials(Exception):
    def __init__(self):
        super().__init__('No credentials. Set environment or arguments.')


def chunker(iterable, size=10):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def login_required(fn):
    @wraps(fn)
    def inner(self, *args, **kwargs):
        if self.token is None:
            raise LoginRequired()

        return fn(self, *args, **kwargs)

    return inner


def _get_credentials():
    try:
        username = os.environ['SD_USERNAME']
        password = os.environ['SD_PASSWORD']
    except KeyError:
        raise NoCredentials()
    return username, password


class SDGrabber(object):
    token = None
    base_url = 'https://json.schedulesdirect.org/20141201/'

    def __init__(self, username=None, password=None, store=None):
        super().__init__()

        if username is None and password is None:
            username, password = _get_credentials()

        self.username = username
        self.password = password
        self.store = store or NullStore()
        self._station_ids = None
        self._program_ids = None

    def _request(self, method, path, headers=None, data=None, **kwargs):
        _headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python / sdgrabber 1.0 / '
                          'https://github.com/btimby/sdgrabber/',
        }
        if self.token is not None:
            _headers['Token'] = self.token
        if headers is not None:
            _headers.update(headers)
        if data is not None:
            data = json.dumps(data)
        url = self.base_url + path.lstrip('/')
        LOGGER.debug('Making request to: %s', url)
        r = requests.request(
            method, url, headers=_headers, data=data, **kwargs)
        data = r.json()
        if isinstance(data, dict) and 'response' in data:
            raise ErrorResponse(data['message'], data)
        return data

    def login(self):
        '''
        Performs login to obtain a token.
        '''
        password_hash = hashlib.sha1(self.password.encode('utf8')).hexdigest()
        data = self._request(
            'post', '/token',
            data={'username': self.username, 'password': password_hash})
        self.token = data['token']

    @login_required
    def status(self):
        '''
        Gets system status, account status and lineups.
        '''

        return StatusModel(self._request('get', '/status'))

    @login_required
    def get_lineups(self, channels=None):
        '''
        Obtains the lineups registered to the account.

        Call should provide channels as a dictionary:

        {
            channel_number: channel_callsign,
            ...
        }

        or a list of tuples:

        [
            (channel_number, channel_name),
            ...
        ]

        If provided, only schedules for these channels will be downloaded.
        '''

        if isinstance(channels, dict):
            numbers, names = set(channels.keys()), set(channels.values())

        elif channels is not None:
            numbers = [i[0] for i in channels]
            names = [i[1] for i in channels]

        else:
            numbers, names = [], []

        status, station_ids = self.status(), []
        for lineup in status.lineups:
            data = self._request('get', '/lineups/%s' % lineup.name)

            # So we can look up the channel number in the next step...
            station_map = {m['stationID']: m['channel'] for m in data['map']}

            # Lineup is a dictionary containing "maps" and "stations". Below we
            # extract all the stationIDs belonging to lineups that have
            # changed. We also account for stations (channels) that the user
            # does not have by skipping them if a list is provided.
            for s in data['stations']:
                number, name = station_map[s['stationID']], s['callsign']
                if channels:
                    if number not in numbers and name not in names:
                        continue
                station_ids.append({'stationID': s['stationID']})

            yield LineupModel(data)

        # Store the station_ids for the next step...
        self._station_ids = station_ids

    @login_required
    def get_schedules(self):
        # We need the data that this method caches. If the user did not call it
        # call it now.
        if self._station_ids is None:
            list(self.get_lineups())

        # Using station_ids, we can find out which schedules have changed. We
        # can download md5 hashes for each station's daily schedules. We can
        # only download 5000 stations at a time (but multiple days per station,
        # many more than 5000 hashes), so we must chunk them up.
        #
        # The return data is in the form:
        # {
        #    stationID: {
        #      'days': {
        #        '9/1/2019': { 'md5': md5_sum }
        #      }
        #    }
        # }
        # We transform it to:
        #
        # [(stationId, '9/1/2019', md5)]
        #
        # Our data is naturally sorted by stationID, day, thus groupby() is
        # going to be able to keep station information together. This allows us
        # to meet the 5000 station limit but request all days for each station.
        shashes = []
        for chunk in chunker(self._station_ids, 5000):
            data = self._request('post', '/schedules/md5', data=list(chunk))

            for station_id, days in data.items():
                if not days:
                    continue
                for day, d in days.items():
                    shashes.append(((station_id, day), d['md5']))

        program_ids = []
        # Here we group the data by station, then chunk 5000 stations at a
        # time.
        for chunk in chunker(
            groupby(
                self.store.diff_schedules(shashes), lambda x: x[0]), 5000):
            # Convert each group into a dictionary in the form:
            #
            # {
            #   stationID: '1234',
            #   'date': [
            #     'yyyy-mm-dd', ...
            #   ]
            # }
            station_days = [
                {'stationID': station, 'date': [d[1] for d in days]}
                for station, days in chunk
            ]
            # Now we get the schedule for each day of each station that has
            # changed. We pull out the program IDs and md5 hashes for the next
            # step and yield the schedule for caller to store.
            data = self._request('post', '/schedules', data=station_days)

            for schedule in data:
                if 'response' in schedule:
                    station_id = schedule['stationID']
                    day = _parse_datetime(schedule['airDateTime'])
                    day = day.date().strftime('%Y-%m-%d')

                    self.store.remove_schedule((station_id, day))

                    LOGGER.error(schedule['message'])
                    continue

                program_ids.extend(
                    ((s['programID'], s['md5']) for s in schedule['programs'])
                )

                yield ScheduleModel(schedule)

        # Store program_ids for the next step...
        self._program_ids = program_ids

    @login_required
    def get_programs(self, lineups=None, schedules=None, channels=None):
        if lineups is None:
            lineups = self.get_lineups(channels=channels)
        stations = {}
        for lineup in lineups:
            for station in lineup.stations:
                stations[station.id] = station

        # Map program IDs to their station / airtime for data merge.
        if schedules is None:
            schedules = self.get_schedules()
        airtimes = {}
        for station in schedules:
            for program in station.programs:
                # Use `.data` attribute to get original dictionary values.
                airtime = {
                    'airDateTime': program.data['airDateTime'],
                    'duration': program.data['duration'],
                    'programID': program.data['programID'],
                }
                airtime.update(stations[station.id].data)
                airtimes.setdefault(program.id, []).append(airtime)

        # We have a list of programIDs and hashes that we need to diff and
        # fetch in batches. The programs endpoint allows 5000, but the metadata
        # endpoint allows 500. We use the smaller one so we can merge data.
        for chunk in chunker(
                self.store.diff_programs(self._program_ids), 500):
            data = list(chunk)
            programs = self._request('post', '/programs', data=data)
            metadata = self._request(
                'post', '/metadata/description', data=data)

            # For some reason they only want the leftmost 10 chars for just
            # this endpoint, we aim to please...
            data = [d[:10] for d in data]

            # This data is in a different format, a list not a dict, make dict
            # for easy merge...
            artwork = {}
            for item in self._request(
                    'post', '/metadata/programs', data=data):
                # Skip art that is not found... This happens because we don't
                # check the flags in the program data, we just ask for artwork
                # for all programs.
                data = item['data']
                if isinstance(data, dict) and data.get('code') == 5000:
                    continue
                artwork[item.pop('programID')] = item

            # Merge the schedule, program, metadata and description and yield
            # that.
            for program in programs:
                program['schedules'] = airtimes.pop(program['programID'])

                try:
                    program.update(metadata.pop(program['programID']))

                except (TypeError, AttributeError):
                    # Not sure what this is, but the metadata endpoint returns
                    # empty lists sometimes...
                    pass

                except KeyError:
                    # Not every program has metadata.
                    pass

                try:
                    # Don't forget to pop with the 10 chars...
                    program['artwork'] = \
                        (artwork.pop(program['programID'][:10])['data'])

                except KeyError:
                    # Not all programs have artwork...
                    pass

                yield ProgramModel(program)

        # Clear our cached data.
        del self._station_ids
        del self._program_ids

        # Store hashes for next run.
        self.store.save()
