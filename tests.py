import unittest

from datetime import timezone

import responses
from responses import GET, POST

from sdgrabber import SDClient, ErrorResponse
from sdgrabber.stores import _diff
from sdgrabber.models import _parse_datetime, _parse_date


USERNAME = 'test_username'
PASSWORD = 'test_password'
URL_PATTERN = 'https://json.schedulesdirect.org/20141201/%s'


def mock_post(path, body):
    responses.add(
        POST, URL_PATTERN % path.lstrip('/'), body=body,
        content_type='application/json')


def mock_get(path, body):
    responses.add(
        GET, URL_PATTERN % path.lstrip('/'), body=body,
        content_type='application/json')


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.client = SDClient(USERNAME, PASSWORD)


class SDClientTestCase(BaseTestCase):
    @responses.activate
    def test_login_fail(self):
        mock_post('/token', '{"code": 3000, "response": "SERVICES_OFFLINE", '
                  '"message": "Server offline for maintenance"}')
        with self.assertRaises(ErrorResponse):
            self.client.login()

    @responses.activate
    def test_login_succeed(self):
        mock_post('/token', '{"code": 0, "message": "OK", "token": "IamAtoken"}')
        mock_get('/status', '{"code": 0, "status": "green"}')
        self.client.login()
        self.assertEqual(self.client.token, 'IamAtoken')


class DiffTestCase(unittest.TestCase):
    def test_same(self):
        old = {
            'a': 1,
            'b': 2,
            'c': 3,
        }
        new = list(old.items())
        self.assertEqual(list(_diff(old, new)), [])

    def test_extra_old(self):
        old = {
            'a': 1,
            'b': 2,
            'c': 3,
        }
        new = list(old.items())
        del new[0]
        self.assertEqual(list(_diff(old, new)), [])

    def test_extra_new(self):
        old = {
            'a': 1,
            'b': 2,
            'c': 3,
        }
        new = list(old.items())
        new.append(('d', 4))
        self.assertEqual(list(_diff(old, new)), [('d')])

    def test_different_value_new(self):
        old = {
            'a': 1,
            'b': 2,
            'c': 3,
        }
        new = old.copy()
        new['c'] = 4
        new = list(new.items())
        self.assertEqual(list(_diff(old, new)), [('c')])


class DateTimeTestCase(unittest.TestCase):
    def test_utc(self):
        dt = _parse_datetime("2014-10-03T00:00:00Z")
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.timetuple()[:6], (2014, 10, 3, 0, 0, 0))

    def test_date(self):
        dt = _parse_date("2014-10-03")
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.timetuple()[:6], (2014, 10, 3, 0, 0, 0))


if __name__ == '__main__':
    unittest.main()
