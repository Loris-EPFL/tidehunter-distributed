import datetime as dt
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gcp_campaign
import gcp_lease
from gcp_api import ApiError
from test_gcp_lifecycle import Store, FakeApi


class CasStore(Store):
    def delete(self, key, *, if_generation_match):
        if key not in self.data:
            raise ApiError(404, 'missing')
        if str(self.data[key][1]) != str(if_generation_match):
            raise ApiError(412, 'CAS')
        del self.data[key]


class LeaseTests(unittest.TestCase):
    def setUp(self):
        self.store = CasStore()
        self.state = {'project_id': 'mysten-research', 'run_id': 'bench-0123456789abcdef',
                      'source_sha256': 'a' * 64,
                      'deadline': (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).isoformat(),
                      'instances': []}

    def test_contention_never_steals_expired_lease(self):
        self.state['deadline'] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat()
        lease = gcp_lease.acquire(self.store, self.state)
        other = {**self.state, 'run_id': 'bench-fedcba9876543210'}
        with self.assertRaisesRegex(RuntimeError, 'another campaign'):
            gcp_lease.acquire(self.store, other)
        with self.assertRaisesRegex(RuntimeError, 'identity mismatch'):
            gcp_lease.owned(self.store, other)
        self.assertEqual(gcp_lease.read(self.store), lease)

    def test_lost_lease_response_adopts_only_own_generation(self):
        original = self.store.put
        def lost(*args, **kwargs):
            original(*args, **kwargs)
            raise ApiError(None, 'lost')
        with mock.patch.object(self.store, 'put', side_effect=lost):
            lease = gcp_lease.acquire(self.store, self.state)
        self.assertEqual(gcp_lease.owned(self.store, self.state), lease)
        gcp_lease.release(self.store, *lease)
        self.assertIsNone(gcp_lease.read(self.store))

    def test_budget_failure_releases_only_after_empty_cleanup(self):
        lease = gcp_lease.acquire(self.store, self.state)
        api = mock.Mock()
        api.objects.return_value = self.store
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(gcp_campaign, 'launch', side_effect=ValueError('budget exceeded')), \
             mock.patch.object(gcp_campaign, 'collect_results', return_value=[]):
            with self.assertRaisesRegex(ValueError, 'budget exceeded'):
                gcp_campaign.run_campaign(api, {'bucket': 'b'}, {}, self.state,
                                          Path(tmp) / 'state', {}, {}, tmp, lease)
        self.assertTrue(self.state['cleanup_verified'])
        self.assertIsNone(gcp_lease.read(self.store))

    def test_uncertain_create_retains_lease_and_never_deletes_unidentified_vm(self):
        lease = gcp_lease.acquire(self.store, self.state)
        record = {'name': self.state['run_id'] + '-0', 'zone': 'us-central1-a',
                  'uncertain': True, 'create_path': 'zones/us-central1-a/instances',
                  'create_body': {}, 'create_request_id': 'same-id', 'scope': 'zones/us-central1-a'}
        self.state['instances'] = [record]
        api = FakeApi()
        api.resources['zones/us-central1-a/instances/' + record['name']] = {'id': 'foreign'}
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(gcp_campaign, 'mutation', side_effect=ApiError(None, 'lost')):
            with self.assertRaisesRegex(RuntimeError, 'cleanup incomplete'):
                gcp_campaign.cleanup(api, self.state, Path(tmp) / 'state')
        self.assertEqual(gcp_lease.read(self.store), lease)
        self.assertTrue(record['uncertain'])
        self.assertFalse(any(call[0] == 'DELETE' for call in api.calls))

    def test_known_orphan_disk_deleted_only_with_exact_numeric_id(self):
        name = self.state['run_id'] + '-0'
        token = 'c' * 32
        rec = {'name': name, 'zone': 'us-central1-a', 'id': '42', 'disk_token': token}
        self.state['instances'] = [rec]
        disk_path = 'zones/us-central1-a/disks/' + name
        description = json.dumps({'run': self.state['run_id'], 'source': self.state['source_sha256'],
                                  'expiry': self.state['deadline'], 'token': token}, sort_keys=True)
        api = FakeApi({disk_path: {'name': name, 'id': '99', 'description': description}})
        with tempfile.TemporaryDirectory() as tmp:
            gcp_campaign.cleanup(api, self.state, Path(tmp) / 'state')
        self.assertEqual(rec['disk_id'], '99')
        self.assertNotIn(disk_path, api.resources)
        api = FakeApi({disk_path: {'name': name, 'id': '100', 'description': description}})
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, 'unidentified boot disk'):
                gcp_campaign.cleanup(api, self.state, Path(tmp) / 'state')
        self.assertIn(disk_path, api.resources)
        rec.pop('disk_id')
        api = FakeApi({disk_path: {'name': name, 'id': '99', 'description': description.replace(token, 'd' * 32)}})
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, 'unidentified boot disk'):
                gcp_campaign.cleanup(api, self.state, Path(tmp) / 'state')
        self.assertIn(disk_path, api.resources)

    def test_http_500_retries_identical_request_id(self):
        api = mock.Mock()
        api.request.side_effect = [ApiError(500, 'ambiguous'), {'status': 'DONE', 'targetId': '42'}]
        result = gcp_campaign.mutation(api, 'POST', 'zones/z/instances', {}, 'same-id', 'zones/z')
        self.assertEqual(result['targetId'], '42')
        self.assertEqual([c.args[3] for c in api.request.call_args_list], [{'requestId': 'same-id'}] * 2)

    def test_terminal_workflow_recovers_only_exact_run(self):
        run_id = 'bench-' + hashlib.sha256(b'123:1').hexdigest()[:16]
        self.state['run_id'] = run_id
        gcp_lease.acquire(self.store, self.state)
        api = mock.Mock()
        api.objects.return_value = self.store
        env = {'GITHUB_ACTIONS': 'true', 'GITHUB_REF': 'refs/heads/bench/gcp-rdma',
               'GITHUB_JOB': 'cleanup', 'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp, 'config.json')
            config.write_text(json.dumps({'project_id': 'mysten-research', 'bucket': 'b'}))
            args = ['recover-cleanup', '--terminal-workflow', '--config', str(config),
                    '--run-id', run_id, '--output-dir', tmp]
            with mock.patch.object(gcp_campaign, 'GoogleApi', return_value=api), \
                 mock.patch.object(gcp_campaign.StateFile, 'recover', return_value=(Path(tmp, 'state'), self.state)), \
                 mock.patch.dict('os.environ', {**env, 'GITHUB_RUN_ATTEMPT': '2'}, clear=True):
                with self.assertRaisesRegex(RuntimeError, 'early recovery'):
                    gcp_campaign.main(args)
            self.assertIsNotNone(gcp_lease.read(self.store))
            with mock.patch.object(gcp_campaign, 'GoogleApi', return_value=api), \
                 mock.patch.object(gcp_campaign.StateFile, 'recover', return_value=(Path(tmp, 'state'), self.state)), \
                 mock.patch.dict('os.environ', env, clear=True):
                gcp_campaign.main(args)
            self.assertIsNone(gcp_lease.read(self.store))


if __name__ == '__main__':
    unittest.main()
