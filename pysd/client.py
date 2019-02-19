import os
import requests
import hashlib
import logging
import json

from datetime import timedelta
from itertools import groupby, chain, islice

from lxml import etree

from stores import NullStore, PickleStore
from models import ProgramModel, LineupModel, ScheduleModel


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


def chunker(iterable, size=10):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


class ErrorResponse(Exception):
    def __init__(self, message, data=None):
        super().__init__(message)
        self.data = data


class ServiceOffline(Exception):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class SDClient(object):
    token = None
    base_url = 'https://json.schedulesdirect.org/20141201/'

    def __init__(self, username, password, store=None):
        super().__init__()
        self.username = username
        self.password = password
        self.store = store or NullStore()
        self._station_ids = None
        self._program_ids = None

    def _request(self, method, path, headers=None, data=None, **kwargs):
        _headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python / pysd 1.0 / '
                          'https://github.com/btimby/pysd/',
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
        if data['code'] != 0:
            raise ServiceOffline(data['response'], data)
        self.token = data['token']

    def get_lineups(self):
        '''
        Downloads system status and obtains the lineups.
        '''
        data = self._request('get', '/status')
        # While not a hash, we can use the modified date like one. If the
        # modified date differs from what we have in our store, then the lineup
        # has changed since our last download.
        lhashes = [
            (l['lineup'], l['modified']) for l in data['lineups']
        ]

        # Iterate over lineup names where the "hash" (actually a modified date)
        # has changed from what the store contains.
        station_ids = []
        for name in self.store.diff_lineups(lhashes):
            data = self._request('get', '/lineups/%s' % name)

            # Lineup is a dictionary containing "maps" and "stations". Below we
            # extract all the stationIDs belonging to lineups that have change.
            station_ids.extend((
                {'stationID': s['stationID']} for s in data['stations']
            ))

            # TODO: yield a model here to aid in extraction of data.
            yield LineupModel(data)

        # Store the station_ids for the next step...
        self._station_ids = station_ids

    def _get_schedules(self):
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
        # going to be able to keep station information together.
        shashes = []
        for chunk in chunker(self._station_ids, 5000):
            data = self._request('post', '/schedules/md5', data=list(chunk))

            for station_id, days in data.items():
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
            #   stationID: {
            #     'days': [
            #       '9/1/2019', ...
            #     ]
            #   }
            # }
            station_days = [
                {'stationID': station, 'days': [d[1] for d in days]}
                for station, days in chunk
            ]
            # Now we get the schedule for each day of each station that has
            # changed. We pull out the program IDs and md5 hashes for the next
            # step and yield the schedule for caller to store.
            data = self._request('post', '/schedules', data=station_days)

            for schedule in data:
                program_ids.extend(
                    ((s['programID'], s['md5']) for s in schedule['programs'])
                )

                yield ScheduleModel(schedule)

        # Store program_ids for the next step...
        self._program_ids = program_ids

    def get_programs(self):
        # Map program IDs to their station / airtime for data merge.
        schedules = {}
        for station in self._get_schedules():
            for program in station.programs:
                # Use `.data` attribute to get original dictionary values.
                schedules[program.id] = {
                    'airDateTime': program.data['airDateTime'],
                    'duration': program.data['duration'],
                    'stationID': station.data['stationID'],
                }

        # This phase is simpler, we have just a list of programIDs and hashes
        # that we need to diff and fetch in batches. The programs
        # endpoint allows 5000, but the metadata endpoint allows 500. We use
        # 500 so we can merge data.
        for chunk in chunker(
                self.store.diff_programs(self._program_ids), 500):
            data = list(chunk)
            programs = self._request('post', '/programs', data=data)
            metadata = self._request(
                'post', '/metadata/description', data=data)

            # For some reason they only want the leftmost 10 chars, we aim to
            # please...
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

            # Merge the schedule, program, metadata and description data and
            # yield that.
            for program in programs:
                program.update(schedules.pop(program['programID']))

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

                # TODO: yield a model here to aid in extraction of data.
                yield ProgramModel(program)

        # Clear our cached data.
        del self._station_ids
        del self._program_ids

        # Store hashes for next run.
        self.store.save()


def main():
    LOGGER.setLevel(logging.DEBUG)
    LOGGER.addHandler(logging.StreamHandler())

    try:
        username = os.environ['SD_USERNAME']
        password = os.environ['SD_PASSWORD']
    except KeyError:
        print('export SD_USERNAME=username and SD_PASSWORD=password')
        return

    store = PickleStore(path='.')
    api = SDClient(username, password, store)
    api.login()

    with open('xmltv.xml', 'wb') as f, etree.xmlfile(f) as x:
        attrs = {
            'source-info-url': 'https://www.schedulesdirect.org/',
            'source-info-name': 'Schedules Direct',
            'generator-info-name': 'pysd',
            'generator-info-url': 'https://github.com/btimby/pysd/',
        }
        with x.element('tv', attrs):

            LOGGER.info('Fetching lineups...')
            for lineup in api.get_lineups():
                for station in lineup.stations:
                    attrs = {
                        'id': station.id,
                    }
                    with x.element('channel', attrs):
                        with x.element('display-name'):
                            x.write(station.name)
                        if station.logo:
                            x.element('icon', {'src': station.logo})

            LOGGER.info('Got lineups, fetching programs...')
            for program in api.get_programs():
                start = program.schedule.airdatetime
                duration = program.schedule.duration
                stop = start + timedelta(seconds=duration)
                attrs = {
                    'start': start.strftime('%Y%m%d%H%M%S'),
                    'stop': stop.strftime('%Y%m%d%H%M%S'),
                    'duration': duration,
                    'channel': program.station.id,
                }
                with x.element('programme'):
                    with x.element('title'):
                        x.write(program.title)

                    if program.subtitle:
                        with x.element('sub-title', {'lang': 'en'}):
                            x.write(program.subtitle)

                    if program.description:
                        with x.element('desc', {'lang': 'en'}):
                            x.write(program.description)

                    if program.actors:
                        with x.element('credits'):
                            for actor in program.actors:
                                with x.element('actor'):
                                    x.write(actor.name)

                    for genre in program.genres:
                        with x.element('category', {'lang': 'en'}):
                            x.write(genre)

                    if program.orig_airdate:
                        with x.element('date'):
                            x.write(
                                program.orig_airdate.strftime('%Y%m%d%H%M%S'))

                    # x.element()


if __name__ == '__main__':
    main()
