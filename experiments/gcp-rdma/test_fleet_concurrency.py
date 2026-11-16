import copy
import hashlib
import json
import multiprocessing
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import gcp_budget
import gcp_campaign
import gcp_lease
import gcp_manual_preflight
import manual_campaign as manual
from gcp_api import ApiError
from test_gcp_lease import CasStore
from test_gcp_lifecycle import FakeApi
from test_manual_campaign import CONFIG, QUOTAS


def state(index, cents=4000):
    return {'run_id': 'bench-' + format(index, '016x'), 'project_id': 'mysten-research',
            'source_sha256': 'a' * 64, 'deadline': '2099-01-01T00:00:00Z',
            'instances': [], 'children': {},
            'reservation': {'cents': cents, 'region': 'us-central1', 'vcpus': 32,
                            'instances': 2, 'local_ssds': 2, 'local_ssd_gib': 28000}}


class RacingStore(CasStore):
    def __init__(self, parties):
        super().__init__()
        self.lock = threading.RLock()
        self.barrier = threading.Barrier(parties)
        self.attempted = set()

    def head(self, *args, **kwargs):
        with self.lock:
            return super().head(*args, **kwargs)

    def get(self, *args, **kwargs):
        with self.lock:
            return super().get(*args, **kwargs)

    def put(self, key, data, **kwargs):
        ident = threading.get_ident()
        if key == gcp_lease.REGISTRY_KEY and ident not in self.attempted:
            self.attempted.add(ident)
            self.barrier.wait(timeout=10)
        with self.lock:
            return super().put(key, data, **kwargs)


def upload_worker(path, entered, release, events):
    class UploadStore:
        def upload(self, key, source, **kwargs):
            events.put(('enter', time.monotonic()))
            entered.set()
            if release is not None and not release.wait(10):
                raise TimeoutError('test upload was not released')
            data = Path(source).read_bytes()
            events.put(('leave', time.monotonic()))
            return {'generation': '1', 'metadata': {'sha256': hashlib.sha256(data).hexdigest()}}
    gcp_campaign.upload_file(UploadStore(), path, hashlib.sha256(b'package').hexdigest(), 'package')


class FleetAdmissionTests(unittest.TestCase):
    def test_concurrent_admission_under_and_over_cap(self):
        for contenders, cap in ((3, 3), (4, 3), (2, 1)):
            with self.subTest(contenders=contenders, cap=cap):
                store = RacingStore(contenders)
                def admit(index):
                    try:
                        return gcp_lease.acquire(store, state(index), max_runs=cap,
                                                 ceiling_cents=14500, quotas=QUOTAS)
                    except RuntimeError as exc:
                        self.assertIn('active lease capacity', str(exc))
                        return None
                with ThreadPoolExecutor(max_workers=contenders) as executor:
                    leases = list(executor.map(admit, range(contenders)))
                self.assertEqual(sum(item is not None for item in leases), min(cap, contenders))
                self.assertEqual(len(json.loads(store.get(gcp_lease.REGISTRY_KEY))['runs']), min(cap, contenders))

    def test_budget_sum_and_independent_weekly_ledger(self):
        store = CasStore()
        first = gcp_lease.acquire(store, state(1, 4834), ceiling_cents=14500, quotas=QUOTAS)
        with self.assertRaisesRegex(ValueError, 'aggregate active run budget'):
            gcp_lease.acquire(store, state(2, 9668), ceiling_cents=14500, quotas=QUOTAS)
        second = gcp_lease.acquire(store, state(2, 4834), ceiling_cents=14500, quotas=QUOTAS)
        gcp_budget.reserve(store, first[0]['run_id'], 4834, 10000)
        report = gcp_budget.reserve(store, second[0]['run_id'], 4834, 10000)
        self.assertEqual(report['weekly'], 9668)
        with self.assertRaisesRegex(ValueError, 'weekly budget'):
            gcp_budget.reserve(store, state(3)['run_id'], 4000, 10000)
        self.assertEqual(gcp_lease.owned(store, state(1, 4834)), first)

    def test_concurrent_budget_sum_cannot_oversubscribe(self):
        store = RacingStore(2)
        def admit(index):
            try:
                return gcp_lease.acquire(store, state(index, 8000), ceiling_cents=14500, quotas=QUOTAS)
            except ValueError as exc:
                self.assertIn('aggregate active run budget', str(exc))
                return None
        with ThreadPoolExecutor(max_workers=2) as executor:
            self.assertEqual(sum(x is not None for x in executor.map(admit, (1, 2))), 1)

    def test_old_lease_counts_without_mutating_or_releasing_it(self):
        store = CasStore()
        old = state(1, 4834)
        old.pop('reservation')
        old['budget'] = {'ceiling_cents': 4834, 'compute_cents': 3945}
        old['children'] = {'fleet': {'host_count': 2}}
        legacy = {key: old[key] for key in ('run_id', 'project_id', 'source_sha256', 'deadline')}
        legacy.update(version=1, token='legacy-token', state_key='runs/' + old['run_id'] + '/lifecycle/state.json')
        store.put(legacy['state_key'], json.dumps(old).encode())
        store.put(gcp_lease.KEY, json.dumps(legacy).encode())
        before = store.data[gcp_lease.KEY]
        with self.assertRaisesRegex(ValueError, 'aggregate active run budget'):
            gcp_lease.acquire(store, state(2, 9668), ceiling_cents=14500, quotas=QUOTAS)
        lease = gcp_lease.acquire(store, state(2, 4834), max_runs=2, ceiling_cents=14500, quotas=QUOTAS)
        with self.assertRaisesRegex(RuntimeError, 'active lease capacity'):
            gcp_lease.acquire(store, state(3), max_runs=2, ceiling_cents=14500, quotas=QUOTAS)
        gcp_lease.release(store, *lease)
        self.assertEqual(store.data[gcp_lease.KEY], before)
        self.assertEqual(gcp_lease.owned(store, old), (legacy, str(before[1])))
        gcp_lease.release(store, legacy, str(before[1]))
        self.assertNotIn(gcp_lease.KEY, store.data)

    def test_expiration_and_missing_legacy_state_never_authorize_takeover(self):
        store = CasStore()
        expired = state(1)
        expired['deadline'] = '2000-01-01T00:00:00Z'
        held = gcp_lease.acquire(store, expired, max_runs=1)
        with self.assertRaisesRegex(RuntimeError, 'active lease capacity'):
            gcp_lease.acquire(store, state(2), max_runs=1)
        self.assertEqual(gcp_lease.owned(store, expired), held)
        gcp_lease.release(store, *held)
        legacy = dict(held[0], version=1)
        store.put(gcp_lease.KEY, json.dumps(legacy).encode())
        with self.assertRaisesRegex(RuntimeError, 'legacy active lease state missing'):
            gcp_lease.acquire(store, state(2), ceiling_cents=14500, quotas=QUOTAS)

    def test_quota_sums_active_pending_fleets_and_observed_usage(self):
        for metric, limit in (('CPUS', 67), ('LOCAL_SSD_TOTAL_GB', 55999), ('INSTANCES', 6)):
            store = CasStore()
            quotas = copy.deepcopy(QUOTAS)
            next(row for row in quotas if row['metric'] == metric)['limit'] = limit
            first = gcp_lease.acquire(store, state(1), ceiling_cents=14500, quotas=quotas)
            with self.assertRaisesRegex(ValueError, metric):
                gcp_lease.acquire(store, state(2), ceiling_cents=14500, quotas=quotas)
            self.assertEqual(gcp_lease.owned(store, state(1)), first)
        quotas = copy.deepcopy(QUOTAS)
        quotas[0].update(limit=64, usage=33)
        with self.assertRaisesRegex(ValueError, 'CPUS'):
            gcp_lease.acquire(CasStore(), state(1), ceiling_cents=14500, quotas=quotas)
        with self.assertRaisesRegex(ValueError, 'missing or ambiguous'):
            gcp_lease.acquire(CasStore(), state(1), ceiling_cents=14500, quotas=[])

    def test_observed_metrics_are_required_and_family_metrics_are_optional(self):
        reservation = state(1)['reservation']
        gcp_lease.check_quota(QUOTAS, [reservation])
        for metric in ('CPUS', 'LOCAL_SSD_TOTAL_GB', 'INSTANCES'):
            with self.subTest(metric=metric):
                without = [row for row in QUOTAS if row['metric'] != metric]
                with self.assertRaisesRegex(ValueError, 'missing or ambiguous: ' + metric):
                    gcp_lease.check_quota(without, [reservation])
                duplicated = [*QUOTAS, next(row for row in QUOTAS if row['metric'] == metric)]
                with self.assertRaisesRegex(ValueError, 'missing or ambiguous: ' + metric):
                    gcp_lease.check_quota(duplicated, [reservation])
        for metric, needed in (('Z4M_CPUS', 32), ('LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY', 28000)):
            with self.subTest(metric=metric):
                family = {'metric': metric, 'limit': needed, 'usage': 0}
                gcp_lease.check_quota([*QUOTAS, family], [reservation])
                with self.assertRaisesRegex(ValueError, 'quota exceeded: ' + metric):
                    gcp_lease.check_quota([*QUOTAS, dict(family, limit=needed-1)], [reservation])
                with self.assertRaisesRegex(ValueError, 'missing or ambiguous: ' + metric):
                    gcp_lease.check_quota([*QUOTAS, family, family], [reservation])
        # Neither launch path allocates external addresses.
        without_addresses = [row for row in QUOTAS if row['metric'] != 'IN_USE_ADDRESSES']
        gcp_lease.check_quota(without_addresses, [reservation])
        gcp_lease.check_quota([*without_addresses,
                              {'metric': 'IN_USE_ADDRESSES', 'limit': 0, 'usage': 0}], [reservation])

    def test_old_reservations_without_vm_count_remain_conservative(self):
        old = state(1)
        old['reservation'].pop('instances')
        old['reservation']['local_ssds'] = 4
        self.assertEqual(gcp_lease.reservation(old)['instances'], 4)
        old['children']['fleet'] = {'host_count': 2}
        self.assertEqual(gcp_lease.reservation(old)['instances'], 2)

    def test_release_preserves_sibling_and_rejects_changed_token(self):
        store = CasStore()
        first = gcp_lease.acquire(store, state(1))
        second = gcp_lease.acquire(store, state(2))
        with self.assertRaisesRegex(RuntimeError, 'changed before release'):
            gcp_lease.release(store, *first[:1], 'foreign')
        gcp_lease.release(store, *first)
        self.assertIsNone(gcp_lease.read(store, state(1)['run_id']))
        self.assertEqual(gcp_lease.owned(store, state(2)), second)

    def test_vm_disk_policy_and_object_namespace_are_per_run(self):
        store = CasStore()
        api = FakeApi()
        api.objects = lambda bucket: store
        states = [state(1), state(2)]
        leases = []
        config = dict(CONFIG, image='projects/p/global/images/i')
        candidate = CONFIG['candidates'][0]
        for index, item in enumerate(states, 1):
            leases.append(gcp_lease.acquire(store, item))
            item['policy'] = {'name': item['run_id'] + '-placement', 'region': 'us-central1',
                              'id': str(index + 100), 'description': json.dumps({'run': item['run_id']})}
            rec = {'name': item['run_id'] + '-0', 'id': str(index), 'zone': 'us-central1-a',
                   'disk_token': format(index, '032x'), 'disk_id': str(index + 200)}
            item['instances'] = [rec]
            vm = gcp_campaign.instance_body(config, candidate, item, 0, 'startup', rec['disk_token'])
            vm['id'] = str(index)
            vm['disks'][0]['source'] = 'projects/mysten-research/zones/us-central1-a/disks/' + rec['name']
            api.resources['zones/us-central1-a/instances/' + rec['name']] = vm
            api.resources['zones/us-central1-a/disks/' + rec['name']] = {
                'name': rec['name'], 'id': rec['disk_id'],
                'description': vm['disks'][0]['initializeParams']['description']}
            api.resources['beta/regions/us-central1/resourcePolicies/' + item['policy']['name']] = dict(item['policy'])
            store.put('runs/' + item['run_id'] + '/private-live-rdma.json', b'owned-prefix')
            self.assertEqual(vm['scheduling']['terminationTime'], item['deadline'])
            self.assertNotIn('tags', vm)  # No shared firewall tag is created or mutated.
            self.assertEqual({x['value'] for x in vm['metadata']['items'] if x['key'] == 'bench-run'}, {item['run_id']})
        second_paths = {path for path in api.resources if states[1]['run_id'] in path}
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(gcp_campaign, 'launch', side_effect=ValueError('stop test workload')), \
             mock.patch.object(gcp_campaign, 'collect_results', return_value=[]):
            with self.assertRaisesRegex(ValueError, 'stop test workload'):
                gcp_campaign.run_campaign(api, {'bucket': 'b'}, {}, states[0], Path(tmp) / 'state', {}, {}, tmp, leases[0])
        self.assertTrue(states[0]['cleanup_verified'])
        self.assertNotIn('cleanup_verified', states[1])
        self.assertTrue(second_paths <= set(api.resources))
        self.assertFalse(any(states[1]['run_id'] in path for method, path, _ in api.calls if method == 'DELETE'))
        self.assertEqual(gcp_lease.owned(store, states[1]), leases[1])
        self.assertEqual(store.get('runs/' + states[1]['run_id'] + '/private-live-rdma.json'), b'owned-prefix')
        # A forged resource record cannot use a sibling name with this run's state.
        states[0]['instances'] = states[1]['instances']
        with tempfile.TemporaryDirectory() as tmp, self.assertRaisesRegex(RuntimeError, 'invalid instance record identity'):
            gcp_campaign.cleanup(api, states[0], Path(tmp) / 'state')
        self.assertTrue(second_paths <= set(api.resources))

    def test_registry_outage_does_not_skip_owned_vm_teardown(self):
        store, owner = CasStore(), state(1)
        lease = gcp_lease.acquire(store, owner)
        name = owner['run_id'] + '-0'
        rec = {'name': name, 'id': '42', 'zone': 'us-central1-a', 'disk_token': 'a'*32}
        owner['policy'] = {'name': owner['run_id'] + '-placement'}
        vm = gcp_campaign.instance_body(dict(CONFIG, image='projects/p/global/images/i'),
            CONFIG['candidates'][0], owner, 0, 'startup', rec['disk_token'])
        vm['id'] = '42'
        vm['disks'][0]['source'] = 'projects/mysten-research/zones/us-central1-a/disks/' + name
        owner.update(policy=None, instances=[rec])
        vm_path = 'zones/us-central1-a/instances/' + name
        api = FakeApi({vm_path: vm})
        api.objects = lambda bucket: store
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(gcp_campaign, 'launch', side_effect=ValueError('workload failed')), \
             mock.patch.object(gcp_campaign, 'collect_results', return_value=[]), \
             mock.patch.object(store, 'head', side_effect=ApiError(500, 'registry unavailable')):
            with self.assertRaisesRegex(ApiError, 'registry unavailable'):
                gcp_campaign.run_campaign(api, {'bucket': 'b'}, {}, owner,
                    Path(tmp)/'state', {}, {}, tmp, lease)
        self.assertNotIn(vm_path, api.resources)
        self.assertNotIn(vm_path.replace('/instances/', '/disks/'), api.resources)
        self.assertTrue(owner['cleanup_verified'])
        self.assertEqual(gcp_lease.owned(store, owner), lease)

    def test_manual_run_ids_nonces_and_capsule_cleanup_are_independent(self):
        store = CasStore()
        with tempfile.TemporaryDirectory() as tmp:
            children = []
            for index in (1, 2):
                owner = state(index)
                child, _ = manual._new_child(owner, Path(tmp)/str(index), 'fleet', 10800)
                child['package'] = {'manifest_sha256': 'f'*64}
                children.append(child)
                capsule = {'identity': {'run_id': child['run_id'],
                    'source_sha256': child['source_sha256'], 'deadline_utc': child['deadline'],
                    'manifest_sha256': 'f'*64}}
                store.put('runs/' + child['run_id'] + '/private-live-rdma.json', json.dumps(capsule).encode())
            self.assertNotEqual(children[0]['run_id'], children[1]['run_id'])
            self.assertNotEqual(children[0]['owner_nonce'], children[1]['owner_nonce'])
            sibling_key = 'runs/' + children[1]['run_id'] + '/private-live-rdma.json'
            sibling_before = store.data[sibling_key]
            manual._cleanup_live_capsules(store, children[0])
            self.assertNotIn('runs/' + children[0]['run_id'] + '/private-live-rdma.json', store.data)
            self.assertEqual(store.data[sibling_key], sibling_before)

    def test_upload_serializes_across_processes(self):
        ctx = multiprocessing.get_context('fork')
        first_entered, second_entered, release = ctx.Event(), ctx.Event(), ctx.Event()
        first_events, second_events = ctx.Queue(), ctx.Queue()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'package'
            path.write_bytes(b'package')
            first = ctx.Process(target=upload_worker, args=(path, first_entered, release, first_events))
            second = ctx.Process(target=upload_worker, args=(path, second_entered, None, second_events))
            try:
                first.start()
                self.assertTrue(first_entered.wait(10))
                second.start()
                self.assertFalse(second_entered.wait(0.3))
                release.set()
                first.join(10)
                second.join(10)
                self.assertEqual((first.exitcode, second.exitcode), (0, 0))
                first_times = [first_events.get(timeout=2) for _ in range(2)]
                second_times = [second_events.get(timeout=2) for _ in range(2)]
                self.assertLessEqual(first_times[-1][1], second_times[0][1])
            finally:
                release.set()
                for process in (first, second):
                    if process.pid is not None:
                        if process.is_alive():
                            process.terminate()
                        process.join(10)
                first_events.close()
                second_events.close()


class ReservationWindowTests(unittest.IsolatedAsyncioTestCase):
    def test_default_and_three_hour_quote(self):
        spec = copy.deepcopy(manual.REVIEWED_SPECS['nvmeof_f7_highmem32'])
        self.assertEqual(manual.reservation_quote(spec, 'z4m-highmem-32'), (21600, 7890, 9668))
        spec['reservation_hours'] = 3
        self.assertEqual(manual.reservation_quote(spec, 'z4m-highmem-32'), (10800, 3945, 4834))
        self.assertEqual(len(manual._reviewed_jobs(spec)), 8)
        manual._check_config(dict(CONFIG, machine_type='z4m-highmem-32'), spec, 889)
        with self.assertRaisesRegex(ValueError, 'non-compute quote'):
            manual._check_config(dict(CONFIG, machine_type='z4m-highmem-32'), spec, 890)
        self.assertEqual(4834 + 4834, 9668)
        for bad in (1, 7, True, 2.5, '3'):
            with self.subTest(hours=bad), self.assertRaisesRegex(ValueError, 'reservation_hours'):
                manual._reviewed_jobs(dict(spec, reservation_hours=bad))

    async def test_window_controls_parent_child_guest_and_workload_deadlines(self):
        spec = dict(manual.REVIEWED_SPECS['nvmeof_f7_highmem32'], reservation_hours=3)
        image = 'projects/mysten-research/global/images/base-image'
        receipt = dict(source_revision='b' * 40, source_sha256='a' * 64,
                       manifest_sha256='c' * 64, sha256='d' * 64)
        store = CasStore()
        captured = {}
        class StopCompute:
            def __init__(self, config, child, path, execute):
                captured['child'] = child
            async def create_policy(self):
                raise RuntimeError('stop before allocation')
            async def cleanup(self):
                pass
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'spec.json').write_text(json.dumps(spec))
            before = time.time()
            with mock.patch.object(manual, '_admit_package', return_value=(receipt, {'tree_sha256': 'f'*64, 'dirty': False})), \
                 mock.patch.object(manual, '_qualify_observer', side_effect=lambda package, value: captured.update(spec=copy.deepcopy(value)) or {}), \
                 mock.patch.object(manual, '_check_snapshot'), \
                 mock.patch.object(gcp_manual_preflight, 'permission_preflight'), \
                 mock.patch.object(gcp_manual_preflight, 'infrastructure_preflight', new_callable=mock.AsyncMock,
                                   return_value=dict(id='42', status='READY', selfLink='https://example/' + image)), \
                 mock.patch.object(gcp_manual_preflight, 'quota_preflight', new_callable=mock.AsyncMock, return_value=QUOTAS), \
                 mock.patch.object(manual, '_stage', return_value=({}, 'e'*64)), \
                 mock.patch('gcp_manual_compute.Compute', StopCompute):
                with self.assertRaisesRegex(RuntimeError, 'stop before allocation'):
                    await manual.run(dict(CONFIG, machine_type='z4m-highmem-32'), package_dir=root,
                        revision='b'*40, spec=spec, wheels=[], base_image=image, base_image_id='42',
                        rdma_device='irdma0', output=root/'out', execute=mock.AsyncMock(),
                        artifacts=store, surcharge_cents=889)
            parent = json.loads((root/'out/state.json').read_text())
            expiry = gcp_campaign.parse_time(parent['deadline']).timestamp()
            self.assertTrue(before + 10800 - 1 <= expiry <= time.time() + 10800)
            self.assertEqual(parent['deadline'], captured['child']['deadline'])
            self.assertEqual(captured['spec']['hard_deadline_seconds'], 10800)
            self.assertEqual(parent['budget']['reservation_hours'], 3)
            self.assertEqual(parent['budget']['ceiling_cents'], 4834)
            child = captured['child']
            child['policy'] = {'name': child['run_id'] + '-placement'}
            body = gcp_campaign.instance_body(dict(CONFIG, machine_type='z4m-highmem-32', image=image),
                CONFIG['candidates'][0], child, 0, manual._gate(CONFIG['bucket'], child), 'a'*32)
            self.assertEqual(body['scheduling']['terminationTime'], parent['deadline'])
            self.assertIn(parent['deadline'], body['metadata']['items'][0]['value'])
            self.assertEqual(spec['hard_deadline_seconds'], 21600)  # Caller input is not mutated.

    async def test_regional_quota_query_is_scoped_and_read_only(self):
        execute = mock.AsyncMock(return_value=json.dumps({'name': 'us-central1', 'quotas': QUOTAS}))
        self.assertEqual(await gcp_manual_preflight.quota_preflight(CONFIG, execute), QUOTAS)
        self.assertEqual(execute.call_args.args[0], ['compute', 'regions', 'describe', 'us-central1',
            '--project=mysten-research', '--quiet', '--format=json'])
        execute.return_value = json.dumps({'name': 'us-central1', 'quotas': QUOTAS,
                                          'quotaStatusWarning': {'code': 'UNREACHABLE'}})
        with self.assertRaisesRegex(ValueError, 'quota fetch returned a warning'):
            await gcp_manual_preflight.quota_preflight(CONFIG, execute)

    async def test_recovery_of_cleaned_run_does_not_read_sibling_as_owner(self):
        store = CasStore()
        first, second = state(1), state(2)
        first['cleanup_verified'] = True
        held = gcp_lease.acquire(store, second)
        with tempfile.TemporaryDirectory() as tmp:
            gcp_campaign.save(gcp_campaign.StateFile(Path(tmp)/'saved-state', store, first['run_id']), first)
            execute = mock.AsyncMock(side_effect=AssertionError('cleaned run must not issue cloud calls'))
            recovered = await manual.recover_cleanup(CONFIG, run_id=first['run_id'],
                state_path=Path(tmp)/'state', execute=execute, artifacts=store)
            self.assertEqual(recovered, first)
            self.assertEqual(gcp_lease.owned(store, second), held)


if __name__ == '__main__':
    unittest.main()
