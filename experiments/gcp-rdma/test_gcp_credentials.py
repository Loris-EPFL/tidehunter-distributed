"""One credential path: delegated on the workstation, metadata on a guest.

The workstation cannot read the campaign bucket as its own user. Every laptop
client delegates to the campaign service account and must mint a new token
before the recorded expiry, because a cell runs for hours.
"""
import json
import time
import unittest
import urllib.parse
from unittest import mock

import gcp_api


class FakeIam:
    """A GoogleApi stand-in that mints tokens and records every mint."""

    project = 'mysten-research'

    def __init__(self, tokens):
        self.tokens = list(tokens)
        self.mints = []
        self.opener = None

    def _request(self, method, url, body=None, headers=None, read_retries=3):
        lifetime = json.loads(body)['lifetime']
        token, expire = self.tokens.pop(0)
        self.mints.append((lifetime, token))
        return json.dumps({'accessToken': token, 'expireTime': expire}).encode()


def expire_in(seconds):
    import datetime
    return (datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=seconds)).isoformat().replace('+00:00', 'Z')


class DelegatedToken(unittest.TestCase):
    def test_a_token_is_reused_until_its_recorded_expiry(self):
        api = FakeIam([('first', expire_in(3600)), ('second', expire_in(3600))])
        provider = gcp_api.delegated_token(api, 'bench-guest@example')
        self.assertEqual(provider(), 'first')
        self.assertEqual(provider(), 'first')
        self.assertEqual(len(api.mints), 1)
        self.assertEqual(api.mints[0][0], '3600s')

    def test_a_long_cell_mints_a_second_token_before_expiry(self):
        api = FakeIam([('first', expire_in(3600)), ('second', expire_in(3600))])
        clock = [1000.0]

        def now():
            return clock[0]

        with mock.patch.object(gcp_api.time, 'monotonic', now):
            provider = gcp_api.delegated_token(api, 'bench-guest@example')
            self.assertEqual(provider(), 'first')
            clock[0] += 3600 - gcp_api.TOKEN_REFRESH_MARGIN_SECONDS - 1
            self.assertEqual(provider(), 'first')
            clock[0] += 2
            self.assertEqual(provider(), 'second')
        self.assertEqual([mint[1] for mint in api.mints], ['first', 'second'])

    def test_an_unparsable_expiry_is_not_trusted_for_long(self):
        api = FakeIam([('first', 'not-a-time'), ('second', expire_in(3600))])
        clock = [500.0]

        def now():
            return clock[0]

        with mock.patch.object(gcp_api.time, 'monotonic', now):
            provider = gcp_api.delegated_token(api, 'bench-guest@example')
            self.assertEqual(provider(), 'first')
            clock[0] += gcp_api.TOKEN_LIFETIME_SECONDS
            self.assertEqual(provider(), 'second')

    def test_impersonation_returns_a_client_with_the_refreshing_provider(self):
        api = FakeIam([('first', expire_in(3600))])
        api.token_provider = lambda: 'caller'
        api.endpoint = gcp_api.STORAGE_ENDPOINT
        client = gcp_api.GoogleApi.impersonated(api, 'bench-guest@example')
        self.assertEqual(client.endpoint, gcp_api.STORAGE_ENDPOINT)
        self.assertEqual(client.token_provider(), 'first')

    def test_an_empty_delegation_is_refused(self):
        api = FakeIam([])
        with self.assertRaises(ValueError):
            gcp_api.delegated_token(api, '')


class CampaignClient(unittest.TestCase):
    def config(self, **extra):
        return dict({'project_id': 'mysten-research',
                     'service_account': 'bench-guest@mysten-research.iam.gserviceaccount.com'},
                    **extra)

    def test_the_guest_uses_its_metadata_identity(self):
        client = gcp_api.campaign_client(self.config(), guest=True)
        self.assertIs(client.token_provider, gcp_api.metadata_token)
        self.assertEqual(client.endpoint, gcp_api.STORAGE_ENDPOINT)

    def test_the_workstation_delegates_to_the_campaign_account(self):
        with mock.patch.object(gcp_api.GoogleApi, 'impersonated') as delegated:
            client = gcp_api.campaign_client(self.config(store_endpoint=None))
        self.assertIs(client, delegated.return_value)
        delegated.assert_called_once_with(
            'bench-guest@mysten-research.iam.gserviceaccount.com')

    def test_a_configuration_without_an_account_is_refused(self):
        with self.assertRaises(ValueError):
            gcp_api.campaign_client({'project_id': 'mysten-research'})


class ObjectCopy(unittest.TestCase):
    """A server-side copy keeps the digest metadata and the destination precondition."""

    class Api:
        endpoint = gcp_api.STORAGE_ENDPOINT
        token_provider = staticmethod(lambda: 'token')

        def __init__(self):
            self.calls = []

        def _request(self, method, url, body=None, headers=None, read_retries=3):
            self.calls.append((method, url, body))
            destination = urllib.parse.unquote(
                url.split('/copyTo/b/bucket/o/', 1)[1].split('?', 1)[0])
            return json.dumps({'name': destination, 'generation': '42', 'size': '17',
                               'metadata': {'sha256': 'a' * 64}}).encode()

    def test_the_copy_names_the_source_and_destination_precondition(self):
        api = self.Api()
        store = gcp_api.ObjectStore(api, 'bucket')
        receipt = store.copy('runs/r/staging/build/d.tar.gz',
                             'runs/r/transfer/7/op/stdin', if_generation_match=0)
        method, url, body = api.calls[0]
        self.assertEqual(method, 'POST')
        self.assertIn('/b/bucket/o/runs%2Fr%2Fstaging%2Fbuild%2Fd.tar.gz/copyTo/b/bucket/o/'
                      'runs%2Fr%2Ftransfer%2F7%2Fop%2Fstdin', url)
        self.assertIn('ifGenerationMatch=0', url)
        self.assertEqual(body, b'')
        self.assertEqual(receipt['generation'], '42')

    def test_a_receipt_for_another_object_is_refused(self):
        api = self.Api()
        api._request = lambda *a, **k: json.dumps(
            {'name': 'other', 'generation': '42'}).encode()
        with self.assertRaisesRegex(ValueError, 'copy receipt mismatch'):
            gcp_api.ObjectStore(api, 'bucket').copy('a', 'b')


class RehearsalEndpoint(unittest.TestCase):
    def test_a_loopback_endpoint_is_admitted(self):
        with mock.patch.dict('os.environ', {gcp_api.REHEARSAL_ENDPOINT:
                                            'http://127.0.0.1:8080'}):
            self.assertEqual(gcp_api.rehearsal_endpoint(), 'http://127.0.0.1:8080')

    def test_a_private_docker_gateway_is_admitted(self):
        with mock.patch.dict('os.environ', {gcp_api.REHEARSAL_ENDPOINT:
                                            'http://10.90.0.1:8080'}):
            self.assertEqual(gcp_api.rehearsal_endpoint(), 'http://10.90.0.1:8080')

    def test_a_public_or_secure_endpoint_is_refused(self):
        for value in ('https://storage.googleapis.com', 'http://8.8.8.8:80',
                      'http://example.com:80', 'ftp://127.0.0.1'):
            with self.subTest(value=value), \
                    mock.patch.dict('os.environ', {gcp_api.REHEARSAL_ENDPOINT: value}):
                with self.assertRaises(ValueError):
                    gcp_api.rehearsal_endpoint()


if __name__ == '__main__':
    unittest.main()
