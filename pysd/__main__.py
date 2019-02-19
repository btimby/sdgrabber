import os
import logging

from datetime import timedelta

from lxml import etree

from .client import SDClient
from .stores import PickleStore


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

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
