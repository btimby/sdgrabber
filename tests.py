import unittest

import responses
from responses import GET, POST

from pysd import SDClient, ServiceOffline


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
        with self.assertRaises(ServiceOffline):
            self.client.login()

    @responses.activate
    def test_login_succeed(self):
        mock_post('/token', '{"code": 0, "message": "OK", "token": "IamAtoken"}')
        mock_get('/status', '{"code": 0, "status": "green"}')
        self.client.login()
        self.assertEqual(self.client.token, 'IamAtoken')
        self.assertEqual(self.client.status, {'code': 0, 'status': 'green'})



if __name__ == '__main__':
    unittest.main()
