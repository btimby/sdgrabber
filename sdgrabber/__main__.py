import os
import logging

from lxml import etree

from .client import SDGrabber
from .stores import PickleStore


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    store = PickleStore(path='.')
    api = SDGrabber(store=store)
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
            # Get lineups as a list so we can traverse it and also pass it to
            # api._get_programs(). This saves it making a duplicate call.
            i, lineups = 0, list(api.get_lineups())
            for lineup in lineups:
                i += 1
                for station in lineup.stations:
                    attrs = {
                        'id': station.id,
                    }
                    with x.element('channel', attrs):
                        with x.element('display-name'):
                            x.write(station.name)
                        if station.logo:
                            x.element('icon', {'src': station.logo})

            LOGGER.info('Got %i lineup(s), fetching programs...', i)

            i = 0
            for program in api.get_programs(lineups=lineups):
                for schedule in program.schedules:
                    i += 1
                    attrs = {
                        'start': schedule.airdatetime.strftime('%Y%m%d%H%M%S'),
                        'stop': schedule.enddatetime.strftime('%Y%m%d%H%M%S'),
                        'duration': schedule.duration,
                        'channel': schedule.station.id,
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
                                    program.orig_airdate
                                    .strftime('%Y%m%d%H%M%S'))

                LOGGER.info('Got %i programs.', i)


if __name__ == '__main__':
    main()
