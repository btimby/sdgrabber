import time

from datetime import timedelta

from datetime import date, datetime, timezone
from itertools import chain
from pprint import pprint
from functools import wraps


DATETIME_FMT = '%Y-%m-%dT%H:%M:%SZ'
DATE_FMT = '%Y-%m-%d'


def _to_absolute_uri(uri):
    if uri.startswith('http'):
        return uri
    return 'https://json.schedulesdirect.org/20141201/image/%s' % uri


def _parse_datetime(text):
    '''
    Parse date / time in format: "2014-10-03T00:00:00Z"
    '''
    # TODO: test that this supports UTC properly, all times are UTC.
    if text is None:
        return
    dt = datetime.strptime(text, DATETIME_FMT)
    return dt.replace(tzinfo=timezone.utc)


def _parse_date(text):
    '''
    Parse date / time in format: "2014-10-03"
    '''
    # TODO: test that this supports UTC properly, all times are UTC.
    if text is None:
        return
    dt = datetime.strptime(text, DATE_FMT)
    return dt.replace(tzinfo=timezone.utc)


def handle_parse_error(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        try:
            return fn(*args, **kwargs)

        except (TypeError, AttributeError) as e:
            # TODO: interrogate `e`
            data = None
            if len(args) > 0:
                data = getattr(args[0], 'data', None)
            raise ParseError('Error parsing data', fn.__name__, data, e)

    return inner


class ParseError(Exception):
    def __init__(self, message, attr, data, e):
        super().__init__(message)
        self.attr = attr
        self.data = data
        self.orig_exception = e


class BaseModel(object):
    def __init__(self, data):
        self.data = data


class LineupModel(BaseModel):
    '''
    Represents a lineup.

    A lineup consists of a mapping of station tuning information as well as the
    station list.

    {
        "map": [
            {
                "channel": "55.29",
                "virtualChannel": "55.29",
                "deliverySystem": "ATSC",
                "stationID": "80606",
                "channelMajor": 55,
                "channelMinor": 29,
                "providerCallsign": "FSPLUS",
                "matchType": "providerCallsign"
            },
            ...
        ],
        "stations": [
            {
                "stationID": "11299",
                "name": "WBBM",
                "callsign": "WBBM",
                "affiliate": "CBS",
                "broadcastLanguage": [
                    "en"
                ],
                "descriptionLanguage": [
                    "en"
                ],
                "broadcaster": {
                    "city": "Chicago",
                    "state": "IL",
                    "postalcode": "60611",
                    "country": "United States"
                },
                "stationLogo": [
                    {
                        "URL": "https://s3.amazonaws.com/schedulesdirect/assets/stationLogos/s10098_h3_aa.png",
                        "height": 270,
                        "width": 360,
                        "md5": "3461b24f174a57f844fa56",
                        "source": "Gracenote"
                    },
                    {
                        "URL": "https://s3.amazonaws.com/schedulesdirect/assets/stationLogos/s10098_h3_ba.png",
                        "height": 270,
                        "width": 360,
                        "md5": "f27da5a7604ffbc6bfd532",
                        "source": "Gracenote"
                    }
                ],
                "logo": {
                    "URL": "https://s3.amazonaws.com/schedulesdirect/assets/stationLogos/s10098_h3_aa.png",
                    "height": 270,
                    "width": 360,
                    "md5": "3461b24f174a57f844fa56"
            }
        ]
    }
    '''

    @property
    @handle_parse_error
    def map(self):
        return {
            s['stationID']: s for s in self.data['map']
        }

    @property
    @handle_parse_error
    def stations(self):
        stations, map = [], self.map
        for s in self.data['stations']:
            s.update(map[s['stationID']])
            stations.append(StationModel(s))
        return stations


class ProgramModel(BaseModel):
    '''
    Represents a program.

    We fudge things a bit and combine the schedule / program / metadata / images.

    {
        "resourceID": "16598552",
        "programID": "SH031652540000",
        "titles": [
            {
            "title120": "Utah State of the State"
            }
        ],
        "descriptions": {
            "description100": [
            {
                "descriptionLanguage": "en",
                "description": "Gov. Gary Herbert (R-Utah) delivers his State of the State address."
            }
            ],
            "description1000": [
            {
                "descriptionLanguage": "en",
                "description": "Gov. Gary Herbert (R-Utah) delivers his State of the State address from the state capital of Salt Lake City."
            }
            ]
        },
        "originalAirDate": "2019-02-17",
        "genres": [
            "Politics",
            "Special"
        ],
        "entityType": "Show",
        "showType": "Special",
        "hasImageArtwork": true,
        "hasSeriesArtwork": true,
        "md5": "bsQozbvQrGujMpSqgXZhYQ",
        "airDateTime": "2019-02-18T00:30:00Z",
        "duration": 1800,
        "stationID": "10161"
    }
    '''

    @handle_parse_error
    def __init__(self, data):
        super().__init__(data)
        self.genres = data.get('genres', [])
        self.entity_type = data.get('entityType', None)
        self.show_type = data.get('showType', None)

    @handle_parse_error
    def _get_people(self, role):
        people = [PersonModel(p) for p in self.data.get('role', [])]
        return sorted(people, key=lambda p: p.billing_order)

    @property
    @handle_parse_error
    def actors(self):
        return self._get_people(role='cast')

    cast = actors

    @property
    @handle_parse_error
    def crew(self):
        return self._get_people(role='crew')

    @property
    @handle_parse_error
    def titles(self):
        return {
            int(tn[5:]): tt for tn, tt
            in chain(*[t.items() for t in self.data.get('titles', {})])
        }

    @property
    @handle_parse_error
    def descriptions(self):
        return {
            int(tn[12:]): tt[0]['description'] for
            tn, tt in self.data.get('descriptions', {}).items()
        }

    @property
    @handle_parse_error
    def title(self):
        titles = self.titles
        if titles:
            return titles[min(titles.keys())]

    @property
    @handle_parse_error
    def title_long(self):
        titles = self.titles
        if titles:
            return titles[max(titles.keys())]

    @property
    @handle_parse_error
    def subtitle(self):
        return self.title_long

    @property
    @handle_parse_error
    def description_short(self):
        descs = self.descriptions
        if descs:
            return descs[min(descs.keys())]

    @property
    @handle_parse_error
    def description(self):
        descs = self.descriptions
        if descs:
            return descs[max(descs.keys())]

    @property
    @handle_parse_error
    def orig_airdate(self):
        return _parse_date(self.data.get('originalAirDate', None))

    @property
    @handle_parse_error
    def schedule(self):
        return ProgramScheduleModel(self.data)

    @property
    @handle_parse_error
    def station(self):
        return StationModel(self.data)

    @property
    @handle_parse_error
    def artwork(self):
        return [ArtModel(a) for a in self.data.get('artwork', [])]


class PersonModel(BaseModel):
    '''
    Represents a person.

    {
        "billingOrder": "01",
        "role": "Actor",
        "nameId": "68293",
        "personId": "68293",
        "name": "Lauren Graham"
    }
    '''

    @handle_parse_error
    def __init__(self, data):
        super().__init__(data)
        # Or is it roleId? And why camel case here instead of roleID?
        self.id = data['personId']
        self.name = data['name']
        self.billing_order = data.get('billingOrder', 0)
        self.role = data['role']


class ArtModel(BaseModel):
    '''
    Represents artwork.

    {
        "URL": "https://s3.amazonaws.com/schedulesdirect/assets/stationLogos/s10098_h3_ba.png",
        "height": 270,
        "width": 360,
        "md5": "f27da5a7604ffbc6bfd532",
        "source": "Gracenote"
    }
    '''

    @handle_parse_error
    def __init__(self, data):
        super().__init__(data)
        self.width = int(data['width'])
        self.height = int(data['height'])
        self.text = True if data.get('text', None) == 'yes' else False
        self.aspect = data.get('aspect', None)
        self.size = data.get('size', None)
        self.category = data['category']

    @property
    @handle_parse_error
    def url(self):
        return _to_absolute_uri(self.data['uri'])


class StationModel(BaseModel):
    '''
    Represents a station.

    {
        "channel": "55.29",
        "virtualChannel": "55.29",
        "deliverySystem": "ATSC",
        "stationID": "80606",
        "channelMajor": 55,
        "channelMinor": 29,
        "providerCallsign": "FSPLUS",
        "matchType": "providerCallsign"
    },

    - and -

    {
        "stationID": "11299",
        "name": "WBBM",
        "callsign": "WBBM",
        "affiliate": "CBS",
        "broadcastLanguage": [
            "en"
        ],
        "descriptionLanguage": [
            "en"
        ],
        "broadcaster": {
            "city": "Chicago",
            "state": "IL",
            "postalcode": "60611",
            "country": "United States"
        },
        "stationLogo": [
            {
                "URL": "https://s3.amazonaws.com/schedulesdirect/assets/stationLogos/s10098_h3_aa.png",
                "height": 270,
                "width": 360,
                "md5": "3461b24f174a57f844fa56",
                "source": "Gracenote"
            },
            {
                "URL": "https://s3.amazonaws.com/schedulesdirect/assets/stationLogos/s10098_h3_ba.png",
                "height": 270,
                "width": 360,
                "md5": "f27da5a7604ffbc6bfd532",
                "source": "Gracenote"
            }
        ],
        "logo": {
            "URL": "https://s3.amazonaws.com/schedulesdirect/assets/stationLogos/s10098_h3_aa.png",
            "height": 270,
            "width": 360,
            "md5": "3461b24f174a57f844fa56"
    }
    '''

    @handle_parse_error
    def __init__(self, data):
        super().__init__(data)
        self.id = data['stationID']

    @property
    @handle_parse_error
    def name(self):
        return self.data.get('name', None)

    @property
    @handle_parse_error
    def callsign(self):
        return self.data.get('callsign', None)

    @property
    @handle_parse_error
    def logo(self):
        try:
            return ArtModel(self.data['logo'])

        except KeyError:
            return

    @property
    def channel(self):
        return self.data.get('channel', None)


class ScheduleModel(BaseModel):
    '''
    Represents a schedule.

    {
        "stationID": "10021",
        "programs": [
            {
                "programID": "EP018632100004",
                "airDateTime": "2015-03-03T01:56:00Z",
                "duration": 3840,
                "md5": "J+AOJ/ofAQdp12Bh3U+C+A",
                "audioProperties": [
                    "cc"
                ],
                "ratings": [
                    {
                        "body": "USA Parental Rating",
                        "code": "TV14"
                    }
                ]
            },
        ]
    }
    '''

    @handle_parse_error
    def __init__(self, data):
        super().__init__(data)
        self.id = data['stationID']
        self.station = StationModel(data)

    @property
    @handle_parse_error
    def programs(self):
        return [ProgramScheduleModel(ps) for ps in self.data['programs']]


class ProgramScheduleModel(BaseModel):
    '''
    Represents a Program Schedule.

    {
        'programID': 'SH003777210000',
        'airDateTime': '2019-02-19T20:00:00Z',
        'duration': 10800,
        'md5': 'Tybuis3yn7VqQ0PSYtFuMw'
    }
    '''

    @handle_parse_error
    def __init__(self, data):
        super().__init__(data)
        self.id = data['programID']
        self.duration = int(data['duration'])
        self.program = ProgramModel(data)

    @property
    @handle_parse_error
    def airdatetime(self):
        return _parse_datetime(self.data['airDateTime'])

    @property
    @handle_parse_error
    def enddatetime(self):
        return self.airdatetime + timedelta(seconds=self.duration)

    @property
    @handle_parse_error
    def audio_properties(self):
        return self.data['audioProperties']

    @property
    @handle_parse_error
    def ratings(self):
        return [r['code'] for r in self.data['ratings']]

    @property
    @handle_parse_error
    def rating(self):
        return self.ratings[0]
