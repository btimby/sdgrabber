import os
import requests
import hashlib
import logging
import json

from itertools import groupby, chain, islice

import splitstream

from stores import NullStore, PickleStore


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


def chunker(iterable, size=10):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


class ServiceOffline(Exception):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class HTTPClient(object):
    token = None
    base_url = 'https://json.schedulesdirect.org/20141201/'

    def __init__(self):
        pass

    def _request(self, method, path, headers=None, data=None, params=None):
        url = self.base_url + path.lstrip('/')
        LOGGER.debug('Making request to: %s', url)
        r = requests.request(
            method, url, headers=headers, params=params, data=data)
        return r


class SDClient(HTTPClient):
    def __init__(self, username, password, store=None):
        super().__init__()
        self.username = username
        self.password = password
        self.store = store or NullStore()
        self.token = None
        self.status = None

    def _request(self, method, path, headers=None, data=None, params=None):
        _headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python / pysd 1.0',
        }
        if self.token is not None:
            _headers['Token'] = self.token
        if headers is not None:
            _headers.update(headers)
        if data is not None:
            data = json.dumps(data)
        r = super()._request(
            method, path, headers=_headers, data=data, params=params)
        return r

    def login(self):
        '''
        Performs login to obtain a token.
        '''
        password_hash = hashlib.sha1(self.password.encode('utf8')).hexdigest()
        r = self._request(
            'post', '/token',
            data={'username': self.username, 'password': password_hash})
        data = r.json()
        if data['code'] != 0:
            raise ServiceOffline(data['response'], r)
        self.token = data['token']

    def get_lineups(self):
        '''
        Downloads system status and obtains the lineups.
        '''
        status = self._request('get', '/status').json()
        # While not a hash, we can use the modified date like one. If the
        # modified date differs from what we have in our store, then the lineup
        # has changed since our last download.
        lhashes = [
            (l['lineup'], l['modified']) for l in status['lineups']
        ]

        # Iterate over lineup names where the "hash" (actually a modified date)
        # has changed from what the store contains.
        station_ids = []
        for name in self.store.diff_lineups(lhashes):
            r = self._request('get', '/lineups/%s' % name)
            lineup = r.json()
            # Lineup is a dictionary containing "maps" and "stations". Below we
            # extract all the stationIDs belonging to lineups that have change.
            station_ids.extend((
                {'stationID': s['stationID']} for s in lineup['stations']
            ))

            # We yield the data so caller can save it.
            yield lineup

        # Store the station_ids for the next step...
        self._station_ids = station_ids

    def get_schedules(self):
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
            r = self._request('post', '/schedules/md5', data=list(chunk))
            for station_id, days in r.json().items():
                for day, d in days.items():
                    shashes.append(((station_id, day), d['md5']))
        del self._station_ids

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
            r = self._request('post', '/schedules', data=station_days)
            import pdb; pdb.set_trace()
            for schedule in r.json():
                program_ids.extend(
                    ((s['programID'], s['md5']) for s in schedule['programs'])
                )
                yield schedule

        # Store program_ids for the next step...
        self._program_ids = program_ids

    def get_programs(self):
        # This phase is simpler, we have just a list of programIDs and hashes
        # that we need to diff and fetch in batches if 5000.
        for chunk in chunker(
                self.store.diff_programs(self._program_ids), 5000):
            r = self._request('post', '/programs', data=list(chunk))
            for program in r.json():
                yield program
        del self._program_ids
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

    LOGGER.info('Fetching lineups...')
    i = 0
    for l in api.get_lineups():
        i += 1

    LOGGER.info('Got %i lineups, fetching schedules...', i)
    i = 0
    for s in api.get_schedules():
        i += 1

    LOGGER.info('Got %i schedules, fetching programs...', i)
    i = 0
    for p in api.get_programs():
        i += 1

    LOGGER.info('Got %i programs. Done.', i)


if __name__ == '__main__':
    main()
