import copy
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gcp_campaign
import gcp_lease
import gcp_manual_preflight
import manual_campaign
import workflow_input
from gcp_api import ApiError
from test_fleet_concurrency import state
import gcp_budget
from gcp_manual_compute import Compute, ComputeError, CreateRejected
from test_manual_compute import FakeMcp
from test_gcp_lease import CasStore
from test_manual_campaign import CONFIG, QUOTAS


class AdmissionRefusalTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_admission_failures_are_terminal_and_recover_without_compute(self):
        image = 'projects/mysten-research/global/images/base-image'
        spec = dict(manual_campaign.REVIEWED_SPECS['nvmeof_f7_highmem32'], reservation_hours=3)
        receipt = dict(source_revision='b'*40, source_sha256='a'*64,
                       manifest_sha256='c'*64, sha256='d'*64)
        for failure in ('quota_metric', 'quota_query', 'run_cap'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root, store = Path(tmp), CasStore()
                (root/'spec.json').write_text(json.dumps(spec))
                config = dict(CONFIG, machine_type='z4m-highmem-32')
                if failure == 'run_cap':
                    config['max_concurrent_runs'] = 1
                    sibling = gcp_lease.acquire(store, state(2))
                quotas = [row for row in QUOTAS if failure != 'quota_metric' or row['metric'] != 'CPUS']
                query_error = ApiError(403, 'quota query denied') if failure == 'quota_query' else None
                expected = {'quota_metric': ValueError, 'quota_query': ApiError, 'run_cap': RuntimeError}[failure]
                execute = mock.AsyncMock(side_effect=AssertionError('no Compute call before admission'))
                with mock.patch.object(manual_campaign, '_admit_package', return_value=(receipt, {'tree_sha256': 'f'*64, 'dirty': False})), \
                     mock.patch.object(manual_campaign, '_qualify_observer', return_value={}), \
                     mock.patch.object(manual_campaign, '_check_snapshot'), \
                     mock.patch.object(gcp_manual_preflight, 'permission_preflight'), \
                     mock.patch.object(gcp_manual_preflight, 'infrastructure_preflight', new_callable=mock.AsyncMock,
                                       return_value=dict(id='42', status='READY', selfLink='https://example/'+image)), \
                     mock.patch.object(gcp_manual_preflight, 'quota_preflight', new_callable=mock.AsyncMock,
                                       return_value=quotas, side_effect=query_error), \
                     mock.patch.object(manual_campaign, '_stage') as stage, \
                     mock.patch.object(manual_campaign.gcp_budget, 'reserve') as reserve, \
                     mock.patch('gcp_manual_compute.Compute') as compute:
                    with self.assertRaises(expected):
                        await manual_campaign.run(config, package_dir=root, revision='b'*40,
                            spec=spec, wheels=[], base_image=image, base_image_id='42', rdma_device='irdma0',
                            output=root/'out', execute=execute, artifacts=store, surcharge_cents=889)
                    stage.assert_not_called()
                    reserve.assert_not_called()
                    compute.assert_not_called()
                parent = json.loads((root/'out/state.json').read_text())
                self.assertTrue(gcp_campaign.is_admission_refusal(parent))
                self.assertEqual(parent['children'], {})
                self.assertNotIn('lease_token', parent)
                self.assertEqual(parent['reservation']['instances'], 2)
                recovered = await manual_campaign.recover_cleanup(config, run_id=parent['run_id'],
                    state_path=root/'recovered-state', execute=execute, artifacts=store)
                self.assertEqual(recovered['cleanup_result'], 'no lease, no resources by construction')
                execute.assert_not_called()
                self.assertFalse(any('/package' in key for key in store.data))
                if failure == 'run_cap':
                    self.assertEqual(gcp_lease.owned(store, state(2)), sibling)

    def test_direct_admission_failures_are_terminal_and_recover_without_compute(self):
        for failure in ('quota_metric', 'quota_query', 'run_cap'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root, store = Path(tmp), CasStore()
                owner = state(1)
                expiry = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).isoformat()
                config = dict(CONFIG, version=1, image='projects/p/global/images/i')
                config['candidates'] = [dict(CONFIG['candidates'][0], network_profile='falcon')]
                if failure == 'run_cap':
                    config['max_concurrent_runs'] = 1
                    sibling = gcp_lease.acquire(store, state(2))
                budget = dict(weekly_limit_cents=10000, run_upper_bound_cents=9000,
                              max_duration_seconds=7200, quote_description='bounded quote')
                receipt = dict(source_sha256=owner['source_sha256'], source_revision='b'*40)
                spec = workflow_input.specification('kv', 'native', 'standard')
                for name, value in (('config', config), ('budget', budget), ('receipt', receipt), ('spec', spec)):
                    (root/(name+'.json')).write_text(json.dumps(value))
                args = ['run', '--config', str(root/'config.json'), '--budget', str(root/'budget.json'),
                    '--run-id', owner['run_id'], '--source-sha', owner['source_sha256'],
                    '--package-receipt', str(root/'receipt.json'), '--spec', str(root/'spec.json'),
                    '--deadline', expiry, '--state', str(root/'state.json'),
                    '--output-dir', str(root/'out'), '--activate']
                quotas = [row for row in QUOTAS if failure != 'quota_metric' or row['metric'] != 'CPUS']
                api = mock.Mock()
                api.objects.return_value = store
                api.request.return_value = {'name': 'us-central1', 'quotas': quotas}
                if failure == 'quota_query':
                    api.request.side_effect = ApiError(403, 'quota query denied')
                env = {'GITHUB_ACTIONS': 'true', 'GITHUB_REF': 'refs/heads/bench/gcp-rdma',
                       'GITHUB_EVENT_NAME': 'workflow_dispatch', 'GITHUB_SHA': 'b'*40}
                expected = {'quota_metric': ValueError, 'quota_query': ApiError, 'run_cap': RuntimeError}[failure]
                with mock.patch.object(gcp_campaign, 'GoogleApi', return_value=api), \
                     mock.patch.object(gcp_campaign, 'admit_sealed_observer'), \
                     mock.patch.object(gcp_campaign, 'preflight'), \
                     mock.patch.object(gcp_campaign, 'run_campaign') as run, \
                     mock.patch.dict('os.environ', env, clear=True):
                    with self.assertRaises(expected):
                        gcp_campaign.main(args)
                    run.assert_not_called()
                persisted = json.loads((root/'state.json').read_text())
                self.assertTrue(gcp_campaign.is_admission_refusal(persisted))
                self.assertEqual(persisted['instances'], [])
                self.assertEqual(persisted['reservation']['instances'], 6)
                self.assertNotIn('lease_token', persisted)
                self.assertEqual(api.request.call_args_list, [mock.call('GET', 'regions/us-central1')])
                api.request.reset_mock()
                api.request.side_effect = AssertionError('recovery must not call Compute')
                with mock.patch.object(gcp_campaign, 'GoogleApi', return_value=api):
                    gcp_campaign.main(['recover-cleanup', '--config', str(root/'config.json'),
                        '--run-id', owner['run_id'], '--state', str(root/'recovered-state'),
                        '--output-dir', str(root/'recovery')])
                result = json.loads((root/'recovery/cleanup-receipt.json').read_text())
                self.assertEqual(result['result'], 'no lease, no resources by construction')
                api.request.assert_not_called()
                self.assertFalse(any('/package' in key for key in store.data))
                if failure == 'run_cap':
                    self.assertEqual(gcp_lease.owned(store, state(2)), sibling)

    def test_refusal_marker_requires_absent_lease_and_empty_resource_inventories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, store = Path(tmp), CasStore()
            owner = state(1)
            owner['cleanup_verified'] = False
            before = copy.deepcopy(owner)
            held = gcp_lease.acquire(store, owner)
            self.assertFalse(gcp_campaign.record_admission_refusal(store, owner, root/'state'))
            self.assertEqual(owner, before)
            gcp_lease.release(store, *held)
            with mock.patch.object(gcp_lease, 'read', side_effect=ApiError(500, 'registry unavailable')):
                with self.assertRaises(ApiError):
                    gcp_campaign.record_admission_refusal(store, owner, root/'state')
            self.assertEqual(owner, before)
            for key, resources in (('children', {'fleet': {}}), ('instances', [{'id': '42'}]),
                                   ('policy', {'id': '42'})):
                populated = dict(owner, **{key: resources})
                self.assertFalse(gcp_campaign.record_admission_refusal(store, populated, root/'state'))
                self.assertFalse(populated['cleanup_verified'])
            self.assertTrue(gcp_campaign.record_admission_refusal(store, owner, root/'state'))
            self.assertTrue(gcp_campaign.is_admission_refusal(owner))
            with self.assertRaisesRegex(RuntimeError, 'invalid admission refusal'):
                gcp_campaign.is_admission_refusal(dict(owner, children={'fleet': {}}))

    async def test_unmarked_abandoned_state_is_not_retroactively_certified(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, store = Path(tmp), CasStore()
            abandoned = state(1)
            abandoned['cleanup_verified'] = False
            path = gcp_campaign.StateFile(root/'state', store, abandoned['run_id'])
            gcp_campaign.save(path, abandoned)
            execute = mock.AsyncMock(side_effect=AssertionError('no resources may be selected'))
            with self.assertRaisesRegex(RuntimeError, 'lease identity mismatch or missing'):
                await manual_campaign.recover_cleanup(CONFIG, run_id=abandoned['run_id'],
                    state_path=root/'recover', execute=execute, artifacts=store)
            remote = json.loads(store.get(path.key))
            self.assertFalse(remote['cleanup_verified'])
            self.assertNotIn('cleanup_reason', remote)
            execute.assert_not_called()


    def test_abandoned_unleased_state_does_not_count_against_new_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            store, abandoned = CasStore(), state(1)
            abandoned['cleanup_verified'] = False
            path = gcp_campaign.StateFile(Path(tmp)/'abandoned', store, abandoned['run_id'])
            gcp_campaign.save(path, abandoned)
            before = store.data[path.key]
            with mock.patch.object(store, 'list', side_effect=AssertionError('admission must not scan run states')):
                held = gcp_lease.acquire(store, state(2), max_runs=1, ceiling_cents=4000, quotas=QUOTAS)
            self.assertEqual(gcp_lease.owned(store, state(2)), held)
            self.assertEqual(store.data[path.key], before)
            report = gcp_budget.reserve(store, state(2)['run_id'], 4000, 4000)
            self.assertEqual(report['weekly'], 4000)
            self.assertNotIn(abandoned['run_id'], json.loads(store.get(gcp_budget.KEY))['runs'])


class CreateRejectionTests(unittest.IsolatedAsyncioTestCase):
    async def attempt(self, outcome):
        code = outcome.removeprefix('operation:')
        with tempfile.TemporaryDirectory() as tmp:
            root, store = Path(tmp), CasStore()
            parent = state(1)
            parent['cleanup_verified'] = False
            parent_path = gcp_campaign.StateFile(root/'parent', store, parent['run_id'])
            gcp_campaign.save(parent_path, parent)
            lease = gcp_lease.acquire(store, parent)
            parent['lease_token'] = lease[0]['token']
            child, _ = manual_campaign._new_child(parent, parent_path, 'fleet', 3600)
            child['host_count'] = 2
            child_path = root/'child-api-state'
            api = FakeMcp(child, CONFIG, child_path)
            query_mode = outcome.removeprefix('query:') if outcome.startswith('query:') else None
            query_calls = []

            async def execute(argv):
                # FakeMcp checks the persisted deletion intent before deleting.
                child_path.write_text(json.dumps(child))
                if query_mode and argv[:3] == ['compute', 'instances', 'describe']:
                    query_calls.append(argv)
                    if query_mode == 'exists':
                        return {'stdout': json.dumps({'name': argv[3], 'id': '200'}), 'exit_code': 0}
                    target = 'projects/mysten-research/zones/us-central1-a/instances/' + argv[3]
                    return {'exit_code': 1, 'stdout': '',
                            'stderr': "ERROR: (gcloud.compute.instances.describe) Could not fetch resource:\n"
                                      " - The resource '" + target + "' was not found\n"}
                if query_mode and argv[:3] == ['compute', 'operations', 'list']:
                    query_calls.append(argv)
                    round_index = sum(call[:3] == argv[:3] for call in query_calls)
                    if query_mode == 'error' and round_index == 2:
                        return {'exit_code': 1, 'stderr': 'permission denied'}
                    operation = {'name': 'operation-123', 'operationType': 'insert',
                                 'zone': 'zones/us-central1-a',
                                 'targetLink': 'https://www.googleapis.com/compute/v1/projects/mysten-research/zones/us-central1-a/instances/'
                                               + child['run_id'] + '-0'}
                    self.assertIn('--filter=targetLink=' + operation['targetLink'] + ' AND operationType=insert', argv)
                    if query_mode == 'operation' and round_index == 2:
                        api.operation_name = operation['name']
                        api.operation_target = operation['targetLink']
                        return {'stdout': json.dumps([operation]), 'exit_code': 0}
                    if query_mode == 'ambiguous':
                        return {'stdout': json.dumps([operation, operation]), 'exit_code': 0}
                    return {'stdout': '[]', 'exit_code': 0}
                if query_mode == 'operation' and argv[:3] == ['compute', 'operations', 'describe']:
                    response = await api(argv)
                    operation = json.loads(response['text'])
                    operation['error'] = {'errors': [{'code': 'UNKNOWN_FAILURE'}]}
                    return {'stdout': json.dumps(operation), 'exit_code': 0}
                if argv[:3] == ['compute', 'operations', 'describe'] and outcome.startswith('operation:'):
                    response = await api(argv)
                    operation = json.loads(response['text'])
                    operation['error'] = {'errors': [{'code': code}]}
                    return {'text': json.dumps(operation)}
                if argv[:3] == ['compute', 'instances', 'create']:
                    if outcome.startswith('operation:'):
                        api.operation_name = 'operation-123'
                        api.operation_target = ('projects/mysten-research/zones/us-central1-a/instances/'
                                                + child['run_id'] + '-0')
                        return {'text': json.dumps({'name': api.operation_name})}
                    if outcome == 'timeout':
                        raise TimeoutError('lost create response')
                    if outcome == 'free_text' or query_mode:
                        return {'exit_code': 1, 'stdout': '', 'stderr': 'ERROR: QUOTA_EXCEEDED'}
                    if outcome == 'invalid_json':
                        return {'exit_code': 1, 'stdout': '{not json'}
                    errors = [{'code': outcome}]
                    if outcome == 'mixed':
                        errors = [{'code': 'QUOTA_EXCEEDED'}, {'code': 'UNKNOWN_FAILURE'}]
                    return {'exit_code': 1, 'stdout': json.dumps({'error': {'errors': errors}})}
                return await api(argv)

            compute = Compute(CONFIG, child, child_path, execute)
            await compute.create_policy()
            with mock.patch('gcp_manual_compute.asyncio.sleep', new_callable=mock.AsyncMock) as sleep, \
                 mock.patch.object(compute, '_ready_vm', new_callable=mock.AsyncMock,
                                   side_effect=ComputeError('bounded reconciliation found no VM')):
                known = code in ('QUOTA_EXCEEDED', 'ZONE_RESOURCE_POOL_EXHAUSTED',
                                 'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')
                expected = CreateRejected if known and not outcome.startswith('operation:') else ComputeError
                with self.assertRaises(expected):
                    await compute.create_vm(0, 'projects/p/global/images/i', child['deadline'], 'startup')
            gcp_campaign.save(parent_path, parent)
            if query_mode:
                lists = [call for call in query_calls if call[:3] == ['compute', 'operations', 'list']]
                expected_rounds = {'absent': 3, 'operation': 2, 'error': 2, 'exists': 0, 'ambiguous': 1}
                self.assertEqual(len(lists), expected_rounds[query_mode])
                for call in lists:
                    self.assertEqual(call, [
                        'compute', 'operations', 'list', '--zones=us-central1-a',
                        '--filter=targetLink=https://www.googleapis.com/compute/v1/projects/mysten-research/zones/us-central1-a/instances/'
                        + child['run_id'] + '-0 AND operationType=insert',
                        '--project=mysten-research', '--quiet', '--format=json', '--verbosity=error'])
                if query_mode == 'absent':
                    self.assertEqual(sleep.await_count, 2)
                    self.assertTrue(child['instances'][0]['create_absence_verified'])
                if query_mode == 'operation':
                    self.assertEqual(child['instances'][0]['operation_name'], 'operation-123')
                    self.assertTrue(child['instances'][0]['operation_done'])
                    self.assertTrue(any(call[:3] == ['compute', 'operations', 'describe'] for call in api.calls))
            if known or outcome.startswith('operation:') or query_mode in ('absent', 'operation'):
                self.assertFalse(child['instances'][0]['uncertain'])
                if known and not outcome.startswith('operation:'):
                    self.assertEqual(child['instances'][0]['create_rejection'], code)
                await manual_campaign._cleanup(CONFIG, parent, parent_path, store, execute)
                self.assertTrue(parent['cleanup_verified'])
                self.assertIsNone(gcp_lease.read(store, parent['run_id']))
                self.assertTrue(child['instances'][0]['deleted'])
                self.assertTrue(child['instances'][0]['disk_deleted'])
                self.assertTrue(child['policy']['deleted'])
                self.assertFalse(any(api.resources.values()))
            else:
                self.assertTrue(child['instances'][0]['uncertain'])
                with self.assertRaisesRegex(RuntimeError, 'cleanup incomplete'):
                    await manual_campaign._cleanup(CONFIG, parent, parent_path, store, execute)
                self.assertFalse(parent['cleanup_verified'])
                self.assertEqual(gcp_lease.owned(store, parent), lease)
                self.assertTrue(api.resources['policy'])

    async def test_structured_quota_and_stockout_rejections_verify_cleanup_and_release(self):
        for code in ('QUOTA_EXCEEDED', 'ZONE_RESOURCE_POOL_EXHAUSTED',
                     'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS'):
            for outcome in (code, 'operation:'+code):
                with self.subTest(outcome=outcome):
                    await self.attempt(outcome)

    async def test_timeout_unknown_and_unstructured_errors_retain_lease(self):
        for outcome in ('timeout', 'UNKNOWN_FAILURE', 'free_text', 'invalid_json', 'mixed'):
            with self.subTest(outcome=outcome):
                await self.attempt(outcome)

    async def test_terminal_unknown_operation_error_verifies_cleanup_and_releases(self):
        await self.attempt('operation:UNKNOWN_FAILURE')

    async def test_prose_failure_absence_queries_verify_cleanup_and_release(self):
        await self.attempt('query:absent')

    async def test_prose_failure_adopts_operation_on_second_round_and_polls(self):
        await self.attempt('query:operation')

    async def test_prose_failure_query_error_retains_lease(self):
        await self.attempt('query:error')

    async def test_prose_failure_existing_instance_retains_uncertainty(self):
        await self.attempt('query:exists')

    async def test_prose_failure_ambiguous_operations_retain_lease(self):
        await self.attempt('query:ambiguous')


if __name__ == '__main__':
    unittest.main()
