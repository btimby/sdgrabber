from datetime import datetime
from itertools import chain
from pprint import pprint


DATETIME_FMT = ''


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
    return datetime.strptime(text, '%Y-%m-%dT%H:%M:%SZ')


def _parse_date(text):
    '''
    Parse date / time in format: "2014-10-03"
    '''
    # TODO: test that this supports UTC properly, all times are UTC.
    if text is None:
        return
    return datetime.strptime(text, '%Y-%m-%d')


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
    pass


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
    def __init__(self, data):
        super().__init__(data)
        self.actors = data.get('cast', [])
        self.genres = data.get('genres', [])
        self.entity_type = data['entityType']
        self.show_type = data['showType']

    def _get_titles(self):
        return {
            int(tn[5:]): tt for tn, tt
            in chain(*[t.items() for t in self.data.get('titles', {})])
        }

    def _get_descriptions(self):
        return {
            int(tn[12:]): tt for
            tn, tt in self.data.get('descriptions', {}).items()
        }

    @property
    def short_title(self):
        titles = self._get_titles()
        if titles:
            return titles[min(titles.keys())]

    @property
    def long_title(self):
        titles = self._get_titles()
        if titles:
            return titles[max(titles.keys())]

    @property
    def title(self):
        return self.short_title

    @property
    def short_desc(self):
        descs = self._get_descriptions()
        if descs:
            return descs[min(descs.keys())]

    @property
    def long_desc(self):
        descs = self._get_descriptions()
        if descs:
            return descs[max(descs.keys())]

    @property
    def description(self):
        return self.long_desc

    @property
    def orig_airdate(self):
        return _parse_date(self.data.get('originalAirDate', None))


    @property
    def schedule(self):
        return ScheduleModel(self.data)

    @property
    def station(self):
        return StationModel(self.data)

    @property
    def artwork(self):
        return [ArtModel(a) for a in self.data.get('artwork', [])]


class ArtModel(BaseModel):
    def __init__(self, data):
        super().__init__(data)
        self.width = int(data['width'])
        self.height = int(data['height'])
        self.text = True if data.get('text', None) == 'yes' else False
        self.aspect = data.get('aspect', None)
        self.size = data.get('size', None)
        self.category = data['category']

    @property
    def url(self):
        return _to_absolute_uri(self.data['uri'])


class StationModel(BaseModel):
    def __init__(self, data):
        super().__init__(data)
        self.id = data['stationID']


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
    def __init__(self, data):
        super().__init__(data)
        self.duration = int(data['duration'])

    @property
    def airdatetime(self):
        return _parse_datetime(self.data['airDateTime'])
