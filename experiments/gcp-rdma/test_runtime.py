import base64
import json
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path

import gcp_runtime as runtime
import gcp_worker as worker


class Error(Exception):
    def __init__(self, status):
        self.status = status


class Store:
    def __init__(self):
        self.values = {}

    def put(self, key, data, **kwargs):
        if kwargs.get('if_generation_match') == 0 and key in self.values:
            raise Error(412)
        self.values[key] = (str(len(self.values) + 1), data)
        return self.head(key)

    def head(self, key):
        if key not in self.values:
            raise Error(404)
        return {'name': key, 'generation': self.values[key][0]}

    def get(self, key, *, generation=None, max_bytes=0):
        meta = self.head(key)
        if generation is not None and str(generation) != meta['generation']:
            raise Error(404)
        return self.values[key][1]


class Api:
    def __init__(self):
        self.store = Store()

    def objects(self, bucket):
        return self.store


def fleet():
    return [{'index': i, 'role': 'coordinator' if i == 0 else 'owner', 'name': f'run-{i}',
             'id': str(100 + i), 'zone': 'us-central1-a', 'internal_ip': f'10.0.0.{i+1}',
             'rdma_ip': f'10.1.0.{i+1}'} for i in range(6)]


def deadline():
    return (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()


class SelectedBackendRuntimeTests(unittest.TestCase):
    def test_focused_package_profile_is_selection_specific(self):
        for selection, expected, rejected in (
                ('valkey_rdma_reads', 'focused_valkey', 'focused'),
                ('nvmeof_f7_highmem32', 'focused', 'focused_valkey'),
                ('nvmeof_reads_geometry', 'focused', 'focused_valkey'),
                ('nvmeof_reads_geometry_rest', 'focused', 'focused_valkey'),
                ('nvmeof_reads_geometry_rest2', 'focused', 'focused_valkey')):
            spec = {'mode': 'focused', 'selection': selection}
            with self.subTest(selection=selection):
                runtime.admit_package_profile(spec, {'build_profile': expected})
                with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'package build profile'):
                    runtime.admit_package_profile(spec, {'build_profile': rejected})
                with self.assertRaises(runtime.RuntimeSafetyError):
                    runtime.admit_package_profile(spec, {})
        runtime.admit_package_profile({'mode': 'standard'}, {})
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.admit_package_profile({'mode': 'standard'}, {'build_profile': 'focused_valkey'})

    def test_three_hour_f7_controller_omits_launch_only_reservation(self):
        import cloud_campaign
        import cloud_package
        import manual_campaign
        spec = dict(manual_campaign.REVIEWED_SPECS['nvmeof_f7_highmem32'],
                    reservation_hours=3, hard_deadline_seconds=10800)
        manifest = dict(revision='b'*40, source_sha256='a'*64,
                        binary_sha256='c'*64, binary_blake2b512='d'*128,
                        build_profile='focused', features=cloud_package.FOCUSED_FEATURES,
                        files={'source/experiments/kv-comparison/nvme_campaign_owner.py': {}})
        expiry = deadline()
        config = dict(package_root='/package', package={'source_revision': 'b'*40},
                      source_sha256='a'*64, spec=spec, fleet=fleet()[:2],
                      run_id='bench-'+'1'*16, machine_type='z4m-highmem-32',
                      bucket='bucket', project_number='1', deadline_utc=expiry)
        with patch.object(Path, 'read_text', return_value=json.dumps(manifest)), \
                patch('prepare.admit_selected_features') as valkey_admission:
            controller = runtime._controller_config(config)
        valkey_admission.assert_not_called()
        self.assertNotIn('reservation_hours', controller['spec'])
        self.assertNotIn('suite', controller['spec'])
        self.assertEqual(controller['spec']['hard_deadline_seconds'], 10800)
        self.assertEqual(controller['deadline_utc'], expiry)
        self.assertEqual(len(cloud_campaign.make_matrix(controller['spec'])), 8)
        self.assertEqual(config['spec']['reservation_hours'], 3)
        # The guest must accept the launcher's projection (F7 attempt 4 failed here).
        with patch.object(runtime, 'validate_runtime'), patch('cloud_layout.validate_fleet'), \
                patch('cloud_controller.validate_read_tier_memory'):
            self.assertIs(runtime.validate_controller_config(controller), controller)

    def test_f7_preparation_requires_only_tidehunter_features(self):
        import cloud_package
        import manual_campaign
        import prepare
        spec = manual_campaign.REVIEWED_SPECS['nvmeof_f7_highmem32']
        manifest = {'build_profile': 'focused', 'features': cloud_package.FOCUSED_FEATURES,
                    'files': {'source/experiments/kv-comparison/nvme_campaign_owner.py': {}}}
        prepare.admit_selected_features(spec, manifest)
        with self.assertRaisesRegex(ValueError, 'exact selected build features'):
            prepare.admit_selected_features(spec, {
                **manifest, 'build_profile': 'focused_valkey',
                'features': cloud_package.FOCUSED_VALKEY_FEATURES})

    def test_f7_controller_rejects_valkey_build_features(self):
        import cloud_package
        import manual_campaign
        config = {'package_root': '/package', 'source_sha256': 'a' * 64,
                  'package': {'source_revision': 'b' * 40},
                  'spec': manual_campaign.REVIEWED_SPECS['nvmeof_f7_highmem32']}
        for profile, features in (
                ('focused_valkey', cloud_package.FOCUSED_VALKEY_FEATURES),
                ('focused', cloud_package.FOCUSED_VALKEY_FEATURES)):
            manifest = {'source_sha256': 'a' * 64, 'revision': 'b' * 40,
                        'build_profile': profile, 'features': features}
            with self.subTest(profile=profile), \
                    patch.object(Path, 'read_text', return_value=json.dumps(manifest)):
                with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'exact TideHunter build features'):
                    runtime._controller_config(config)

    def test_controller_rejects_each_missing_selected_store_feature(self):
        backends = ['tikv', 'cassandra', 'foundationdb', 'garnet', 'kvrocks']
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {'package_root': directory, 'source_sha256': 'a' * 64,
                      'package': {'source_revision': 'b' * 40},
                      'spec': {'backends': backends}}
            for missing in backends:
                manifest = {'source_sha256': config['source_sha256'], 'revision': 'b' * 40,
                            'features': [name for name in backends if name != missing]}
                (root / 'manifest.json').write_text(json.dumps(manifest))
                with self.subTest(missing=missing), self.assertRaisesRegex(
                        runtime.RuntimeSafetyError, 'lacks selected industry'):
                    runtime._controller_config(config)


class NvmetFixRuntimeTests(unittest.TestCase):
    def test_selected_candidate_requires_matching_manifest_guest_and_loaded_identity(self):
        import cloud_package
        candidate = {'candidate_srcversion': 'ABC', 'candidate_build_id': '123', 'module_sha256': 'a' * 64}
        loaded = {**candidate, 'guest': {'instance_name': 'run-1', 'instance_id': '101', 'run_id': 'run'},
                  'loaded_nvmet': {'srcversion': 'ABC', 'build_id': '123'}}
        helper = SimpleNamespace(install=lambda prefix: loaded)
        config = {'run_id': 'run', 'package_root': '/package', 'package': {'manifest_sha256': 'b' * 64}}
        admission = {'manifest_sha256': 'b' * 64, 'nvmet_fix': candidate}
        host = {'name': 'run-1', 'id': '101'}
        with patch.object(cloud_package, 'nvmet_fix_module', return_value=helper):
            self.assertEqual(runtime.admit_nvmet_fix(config, host, admission)['loaded'], loaded)
            with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'manifest'):
                runtime.admit_nvmet_fix(config, host, {**admission, 'manifest_sha256': 'c' * 64})
            loaded['loaded_nvmet']['srcversion'] = 'stock'
            with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'loaded nvmet identity'):
                runtime.admit_nvmet_fix(config, host, admission)
        self.assertIsNone(runtime.admit_nvmet_fix(config, host, {}))


class RemoteOwnerTraceEnvelopeTests(unittest.TestCase):
    def test_maximum_serialized_owner_trace_and_cpu_fit_unchanged_envelope(self):
        import cloud_controller
        import owner_ftrace

        def encoded(value):
            return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()

        self.assertEqual(cloud_controller.MAX_JSON_BYTES, 32 * 1024**2)
        self.assertEqual(owner_ftrace.TRACE_MAX_SERIALIZED_BYTES + owner_ftrace.CPU_MAX_BYTES
                         + owner_ftrace.OWNER_TRACE_JSON_MARGIN_BYTES, cloud_controller.MAX_JSON_BYTES)
        line = 'wal"\\\tλ-123 [000] .... 1.000000000: nvmet_req_complete: status=0\n'
        line_bytes = len(encoded(line)) - 2
        event_count, padding = divmod(owner_ftrace.TRACE_MAX_SERIALIZED_BYTES, line_bytes)
        trace = line * (event_count - 1) + line[:-1] + 'x' * padding + '\n'
        self.assertEqual(len(encoded(trace)) - 2, owner_ftrace.TRACE_MAX_SERIALIZED_BYTES)
        self.assertLess(len(trace.encode()), owner_ftrace.TRACE_MAX_SERIALIZED_BYTES)
        self.assertTrue(trace.endswith('\n'))

        # Fill the conservative CPU wire budget with real-shaped per-CPU samples,
        # not an opaque padding string. Both tick rows represent 100 total ticks.
        cpu_count = min(256, owner_ftrace.CPU_MAX_CPUS)
        idle = {'ticks': [1, 0, 0, 99, 0, 0, 0, 0], 'busy_pct': 1.0, 'softirq_pct': 0.0}
        busy = {'ticks': [10, 0, 0, 90, 0, 0, 0, 0], 'busy_pct': 10.0, 'softirq_pct': 0.0}
        cpus = {f'cpu{i}': idle for i in range(cpu_count)}
        sample = {'elapsed_seconds': 1, 'cpus': cpus}
        cpu = {'available': False, 'source': '/proc/stat', 'interval_seconds': 1,
               'scope': 'owner trace start to stop; includes the final partial interval',
               'samples': [], 'truncated': True, 'reason': 'owner CPU sampling deadline expired'}
        empty_bytes, sample_bytes = len(encoded(cpu)), len(encoded(sample))
        sample_count = (owner_ftrace.CPU_MAX_BYTES - empty_bytes + 1) // (sample_bytes + 1)
        remaining = owner_ftrace.CPU_MAX_BYTES - (empty_bytes + sample_count * (sample_bytes + 1) - 1)
        if remaining == 1:
            sample_count -= 1
            remaining += sample_bytes + 1
        odd = remaining % 2
        remaining -= 3 * odd  # A plausible 1.25-second interval adds three bytes.
        scaled_rows = remaining // 2  # The busy row is exactly two bytes longer.
        self.assertGreater(sample_count, 1)
        self.assertLessEqual(sample_count, owner_ftrace.CPU_MAX_SAMPLES)
        self.assertGreaterEqual(remaining, 0)
        self.assertLessEqual(scaled_rows, sample_count * cpu_count)
        for index in range(sample_count):
            count = min(scaled_rows, cpu_count)
            cpu['samples'].append({
                'elapsed_seconds': 1.25 if odd and index == 0 else 1,
                'cpus': {name: busy if number < count else idle
                         for number, name in enumerate(cpus)} if count else cpus})
            scaled_rows -= count
        self.assertEqual(scaled_rows, 0)
        self.assertEqual(len(encoded(cpu)), owner_ftrace.CPU_MAX_BYTES)

        coverage = {'event_count': event_count, 'first_timestamp_ns': 1_000_000_000,
                    'last_timestamp_ns': 1_000_000_000, 'duration_ns': 0,
                    'scope': 'captured_complete_trace_events', 'clock': 'mono'}
        result = {'trace': trace, 'cpu_samples': cpu, 'trace_coverage': coverage,
                  'events_enabled': list(owner_ftrace.EVENTS), 'events_missing': [],
                  'truncated': True, 'buffer_overruns': 7,
                  'errors': ['output: bounded diagnostic "\\\tλ']}
        request = {'action': 'owner_ftrace_stop', 'case': 'envelope', 'index': 0}
        envelope = {'request': request, 'instance_id': '101', 'status': 'completed', 'result': result}
        metadata = {**envelope, 'result': {**result, 'trace': '', 'cpu_samples': {}}}
        metadata_bytes = len(encoded(metadata)) + 1 - len(encoded({}))
        expected_bytes = (owner_ftrace.TRACE_MAX_SERIALIZED_BYTES
                          + owner_ftrace.CPU_MAX_BYTES + metadata_bytes)
        self.assertLess(metadata_bytes, owner_ftrace.OWNER_TRACE_JSON_MARGIN_BYTES)

        class EnvelopeStore(Store):
            def __init__(self):
                super().__init__()
                self.metadata = {}
                self.reads = []

            def put(self, key, data, **kwargs):
                self.metadata[key] = kwargs.get('metadata', {})
                return super().put(key, data, **kwargs)

            def head(self, key):
                return {**super().head(key), 'size': len(self.values[key][1]),
                        'metadata': self.metadata[key]}

            def get(self, key, *, generation=None, max_bytes=0):
                self.reads.append((key, generation, max_bytes))
                raw = super().get(key, generation=generation, max_bytes=max_bytes)
                if len(raw) > max_bytes:
                    raise ValueError('object exceeds retrieval bound')
                return raw

        store = EnvelopeStore()
        config = {'prefix': 'run', 'deadline_utc': deadline(), 'repository_root': '/sealed/source'}
        clients = {'objects': store}
        exports = []

        class Mailbox:
            def submit(self, index, argv, *, operation_id, timeout_seconds):
                payload = json.loads(base64.b64decode(argv[argv.index('--request-base64') + 1]))
                receipt = {**envelope, 'request': payload['request']}
                exports.append(cloud_controller.persist_json(clients, config, payload['key'], receipt))
                return payload['key']

            def wait(self, reference, *, timeout_seconds):
                return {'exit_code': 0, 'stderr': ''}

        clients['mailbox'] = Mailbox()
        controller = SimpleNamespace(config=config, clients=clients, ids=['100', '101'],
                                     remaining=lambda **kwargs: 180, guard=lambda: None,
                                     sequence_lock=threading.Lock(), sequence=0)
        for size in (expected_bytes, cloud_controller.MAX_JSON_BYTES):
            with self.subTest(object_bytes=size):
                received = runtime.request_remote(controller, 0, request, case='envelope')
                exported = exports[-1]
                raw = store.values[exported['key']][1]
                self.assertEqual(raw, encoded(envelope) + b'\n')
                self.assertEqual(len(raw), size)
                self.assertEqual(exported['bytes'], size)
                self.assertEqual(store.reads[-1], (exported['key'], exported['generation'],
                                                  cloud_controller.MAX_JSON_BYTES))
                self.assertEqual(received, result)
                self.assertEqual(received['trace_coverage'], coverage)
                self.assertNotIn('cpu_summary', received)
                if size == expected_bytes:
                    # Consume only the reserved metadata headroom, leaving both
                    # maximum-sized evidence payloads unchanged.
                    result['errors'][0] += 'x' * (cloud_controller.MAX_JSON_BYTES - size)
                else:
                    result['errors'][0] += 'x'

        self.assertEqual(len(encoded(envelope)) + 1, cloud_controller.MAX_JSON_BYTES + 1)
        with self.assertRaisesRegex(cloud_controller.SafetyError, 'bounded metadata budget'):
            cloud_controller.persist_json(clients, config, 'run/results/oversized.json', envelope)
        self.assertNotIn('run/results/oversized.json', store.values)
        self.assertEqual(cloud_controller.MAX_JSON_BYTES, 32 * 1024**2)


class MailboxTests(unittest.TestCase):
    def test_wait_retries_credentials_then_receives_terminal(self):
        api = Api()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, deadline())
        ref = mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/a.py'], operation_id='id', timeout_seconds=1)
        key = ref['key'].replace('/requests/', '/responses/')
        api.store.put(key, json.dumps({'request_sha256': ref['request_sha256'], 'instance_id': ref['instance_id'],
                                      'operation_id': 'id', 'source_sha256': 'a' * 64, 'exit_code': 0,
                                      'stdout': '', 'stderr': ''}).encode())
        clock, sleeps = [0], []
        original = api.store.get
        failures = [2]
        def get(*args, **kwargs):
            if failures[0]:
                failures[0] -= 1
                raise runtime.CredentialUnavailable('secret')
            return original(*args, **kwargs)
        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds
        with patch.object(api.store, 'get', side_effect=get), \
                patch.object(runtime.time, 'time', side_effect=lambda: clock[0]), \
                patch.object(runtime.time, 'sleep', side_effect=sleep), \
                patch('sys.stderr') as stderr:
            self.assertEqual(mailbox.wait(ref, timeout_seconds=100)['exit_code'], 0)
        self.assertEqual(sleeps, [5, 10])
        self.assertNotIn('secret', str(stderr.write.call_args_list))

    def test_wait_credentials_stop_at_deadline_with_capped_backoff(self):
        api = Api()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, deadline())
        ref = mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/a.py'], operation_id='id', timeout_seconds=1)
        clock, sleeps = [0], []
        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds
        with patch.object(api.store, 'get', side_effect=runtime.CredentialUnavailable('secret')), \
                patch.object(runtime.time, 'time', side_effect=lambda: clock[0]), \
                patch.object(runtime.time, 'sleep', side_effect=sleep), patch('sys.stderr'):
            with self.assertRaisesRegex(TimeoutError, 'terminal response missing before deadline'):
                mailbox.wait(ref, timeout_seconds=220)
        self.assertEqual(clock[0], 220)
        self.assertEqual(sleeps, [5, 10, 20, 40, 60, 60, 25])

    def test_real_subprocess_round_trip_and_replay_gate(self):
        api = Api()
        expires = deadline()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, expires)
        with tempfile.TemporaryDirectory() as temporary, patch.object(runtime, 'GUEST_PYTHON', sys.executable):
            root = Path(temporary)
            source = root / 'package/source'
            source.mkdir(parents=True)
            script = source / 'command.py'
            script.write_text('import pathlib,sys\np=pathlib.Path(sys.argv[1]); p.write_text(p.read_text()+"x" if p.exists() else "x"); print("done")\n')
            counter = root / 'counter'
            reference = mailbox.submit(1, [runtime.GUEST_PYTHON, str(script), str(counter)], operation_id='one', timeout_seconds=10)
            config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'deadline_utc': expires, 'package_root': str(root / 'package')}
            gate = root / 'gate'; gate.mkdir()
            for _ in range(2):
                worker.run_request(config, fleet()[1], api.store, reference['key'], reference['generation'], gate)
            receipt = mailbox.wait(reference, timeout_seconds=1)
            self.assertEqual(counter.read_text(), 'x')
            self.assertEqual(receipt['exit_code'], 0)
            self.assertEqual(receipt['stdout'], 'done\n')

    def test_collision_and_missing_response_fail_closed(self):
        api = Api()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, deadline())
        ref = mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/a.py'], operation_id='id', timeout_seconds=1)
        with self.assertRaises(runtime.RuntimeSafetyError):
            mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/b.py'], operation_id='id', timeout_seconds=1)
        with self.assertRaises(TimeoutError):
            mailbox.wait(ref, timeout_seconds=0.01)

    def test_reject_wrong_receipt_source(self):
        api = Api()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, deadline())
        ref = mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/a.py'], operation_id='id', timeout_seconds=1)
        key = ref['key'].replace('/requests/', '/responses/')
        api.store.put(key, json.dumps({'request_sha256': ref['request_sha256'], 'instance_id': ref['instance_id'],
                                      'operation_id': 'id', 'source_sha256': 'b' * 64, 'exit_code': 0,
                                      'stdout': '', 'stderr': ''}).encode())
        with self.assertRaises(runtime.RuntimeSafetyError):
            mailbox.wait(ref, timeout_seconds=1)

    def test_deadline_and_fleet_identity_are_enforced(self):
        api = Api()
        expired = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, expired)
        with self.assertRaises(runtime.RuntimeSafetyError):
            mailbox.submit(0, ['x'], operation_id='id', timeout_seconds=1)
        bad = fleet(); bad[1]['id'] = bad[0]['id']
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.RemoteMailbox(api, 'bucket', 'run', bad, 'a' * 64, deadline())

    def test_python_command_requires_admitted_source_and_interpreter(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'source'
            source.mkdir()
            script = source / 'command.py'
            script.write_text('pass\n')
            config = {'package_root': temporary, 'run_id': 'run'}
            request = {'argv': [runtime.GUEST_PYTHON, str(script)], 'cwd': None, 'env': {}}
            self.assertEqual(worker.admit_command(config, request)[0], request['argv'])
            for argv in ([runtime.GUEST_PYTHON, '/tmp/untrusted.py'], ['/usr/bin/python3', str(script)]):
                with self.subTest(argv=argv), self.assertRaises(runtime.RuntimeSafetyError):
                    worker.admit_command(config, {**request, 'argv': argv})
            for key in ('PATH', 'LD_PRELOAD', 'PYTHONPATH'):
                with self.subTest(environment=key), self.assertRaises(runtime.RuntimeSafetyError):
                    worker.admit_command(config, {**request, 'env': {key: '/tmp/untrusted'}})

    def test_live_fleet_rejects_changed_expiry_and_identity(self):
        expires = deadline()
        hosts = fleet()
        config = {'platform': {'runtime': {'fleet': hosts, 'run_id': 'run', 'source_sha256': 'a' * 64,
                                             'deadline_utc': expires, 'machine_type': 'z4m-highmem-16'}}}
        rows = {}
        for host in hosts:
            rows[host['name']] = {'id': host['id'], 'name': host['name'], 'status': 'RUNNING',
                'machineType': 'zones/us-central1-a/machineTypes/z4m-highmem-16',
                'labels': {'bench-run': 'run', 'bench-source': 'a' * 63},
                'metadata': {'items': [{'key': 'bench-run', 'value': 'run'},
                                       {'key': 'bench-source', 'value': 'a' * 64},
                                       {'key': 'bench-expiry', 'value': expires}]},
                'scheduling': {'instanceTerminationAction': 'DELETE', 'terminationTime': expires},
                'networkInterfaces': [{'networkIP': host['internal_ip'], 'nicType': 'GVNIC'},
                                      {'networkIP': host['rdma_ip'], 'nicType': 'IRDMA'}],
                'disks': [{'type': 'SCRATCH', 'interface': 'NVME'}],
                'resourcePolicies': ['regions/us-central1/resourcePolicies/run-placement']}
        class Compute:
            def request(self, method, path):
                return rows[path.rsplit('/', 1)[-1]]
        clients = {'platform': Compute()}
        self.assertEqual(len(runtime.verify_fleet(config, clients)), 6)
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'STOP'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'DELETE'
        rows['run-1']['id'] = '999'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        rows['run-1']['id'] = '101'
        for row in rows.values():
            row['resourcePolicies'] = ['regions/us-central1/resourcePolicies/reused-placement']
        config['platform']['runtime']['placement_policy'] = {'name': 'reused-placement', 'id': '123'}
        policy = {'id': '123', 'groupPlacementPolicy': {'collocation': 'COLLOCATED', 'maxDistance': 2}}
        class ReusedCompute:
            def request(self, method, path):
                if '/resourcePolicies/' in path:
                    self.path = path
                    return policy
                return rows[path.rsplit('/', 1)[-1]]
        compute = ReusedCompute()
        self.assertEqual(len(runtime.verify_fleet(config, {'platform': compute})), 6)
        self.assertEqual(compute.path, 'beta/regions/us-central1/resourcePolicies/reused-placement')
        policy['id'] = '124'
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'placement policy'):
            runtime.verify_fleet(config, {'platform': compute})

    def test_h4d_fleet_requires_all_ten_nvme_scratch_disks(self):
        expires = deadline()
        hosts = fleet()
        shape = 'h4d-highmem-192-lssd'
        config = {'platform': {'runtime': {'fleet': hosts, 'run_id': 'run', 'source_sha256': 'a' * 64,
                                           'deadline_utc': expires, 'machine_type': shape}}}
        rows = {}
        for host in hosts:
            rows[host['name']] = {'id': host['id'], 'name': host['name'], 'status': 'RUNNING',
                'machineType': 'zones/us-central1-a/machineTypes/' + shape,
                'labels': {'bench-run': 'run', 'bench-source': 'a' * 63},
                'metadata': {'items': [{'key': 'bench-run', 'value': 'run'},
                                       {'key': 'bench-source', 'value': 'a' * 64},
                                       {'key': 'bench-expiry', 'value': expires}]},
                'scheduling': {'instanceTerminationAction': 'DELETE', 'terminationTime': expires},
                'networkInterfaces': [{'networkIP': host['internal_ip'], 'nicType': 'GVNIC'},
                                      {'networkIP': host['rdma_ip'], 'nicType': 'IRDMA'}],
                'disks': [{'type': 'PERSISTENT', 'interface': 'NVME', 'boot': True}] +
                         [{'type': 'SCRATCH', 'interface': 'NVME'} for _ in range(10)],
                'resourcePolicies': ['regions/us-central1/resourcePolicies/run-placement']}
        class Compute:
            def request(self, method, path):
                return rows[path.rsplit('/', 1)[-1]]
        clients = {'platform': Compute()}
        result = runtime.verify_fleet(config, clients)
        self.assertEqual(result['run-0']['selected_local_ssd'], 'google-local-nvme-ssd-0')
        self.assertEqual(result['run-0']['unused_local_ssd_count'], 9)
        disks = rows['run-1']['disks']
        disks.pop()
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        disks.append({'type': 'SCRATCH', 'interface': 'SCSI'})
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        disks[-1]['interface'] = 'NVME'
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'STOP'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'DELETE'
        config['platform']['runtime']['machine_type'] = 'h4d-standard-192'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)

    def test_rdma_requires_admitted_network_binding(self):
        config = {'spec': {'backends': ['tidehunter_nvme_rdma']},
                  'platform': {'runtime': {'fleet': fleet()}}}
        addresses = json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.2'}]}])
        with patch('shutil.which', return_value='/usr/bin/nvme'), \
             patch.object(Path, 'is_dir', return_value=True), \
             patch.object(Path, 'is_char_device', return_value=True), \
             patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='configfs')), \
             patch('subprocess.check_output', side_effect=['link rdma0/1 state ACTIVE netdev eth1', addresses]):
            proof = runtime.admit_fabrics(config, 0, {}, '/tmp')
        self.assertEqual(proof['network_interface'], 'eth1')
        with patch('shutil.which', return_value='/usr/bin/nvme'), \
             patch.object(Path, 'is_dir', return_value=True), \
             patch.object(Path, 'is_char_device', return_value=True), \
             patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='configfs')), \
             patch('subprocess.check_output', side_effect=['link rdma0/1 state ACTIVE netdev eth0', addresses]):
            with self.assertRaises(runtime.RuntimeSafetyError):
                runtime.admit_fabrics(config, 0, {}, '/tmp')
        with patch('shutil.which', return_value='/usr/bin/nvme'), \
             patch.object(Path, 'is_dir', return_value=True), \
             patch.object(Path, 'is_char_device', return_value=True), \
             patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='configfs')), \
             patch('subprocess.check_output', side_effect=['link rdma0/1 state DOWN netdev eth1', addresses]):
            with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'active RDMA'):
                runtime.admit_fabrics(config, 0, {}, '/tmp')

    def test_live_rdma_also_loads_tcp_for_private_commissioning(self):
        config = {'spec': {'mode': 'standard-with-live-nvmeof',
                           'backends': ['tidehunter_nvme_rdma']},
                  'platform': {'runtime': {'fleet': fleet()}}}
        addresses = json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.2'}]}])
        with patch('shutil.which', return_value='/usr/bin/nvme'), \
             patch.object(Path, 'is_dir', return_value=True), \
             patch.object(Path, 'is_char_device', return_value=True), \
             patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='configfs')) as run, \
             patch('subprocess.check_output', side_effect=['link rdma0/1 state ACTIVE netdev eth1', addresses]):
            runtime.admit_fabrics(config, 0, {}, '/tmp')
        commands = [call.args[0] for call in run.call_args_list]
        self.assertIn(['modprobe', 'nvme_tcp'], commands)
        self.assertIn(['modprobe', 'nvmet_tcp'], commands)

    def test_nvme_prerequisites_fail_closed(self):
        config = {'spec': {'backends': ['tidehunter_nvme_tcp']}}
        with patch('shutil.which', return_value=None), self.assertRaisesRegex(runtime.RuntimeSafetyError, 'nvme-cli'):
            runtime.admit_fabrics(config, 0, {}, '/tmp')
        with patch('shutil.which', return_value='/usr/bin/nvme'), \
             patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='tmpfs')), \
             self.assertRaisesRegex(runtime.RuntimeSafetyError, 'configfs'):
            runtime.admit_fabrics(config, 0, {}, '/tmp')
        with patch('shutil.which', return_value='/usr/bin/nvme'), \
             patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='configfs')), \
             patch.object(Path, 'is_dir', return_value=False), \
             self.assertRaisesRegex(runtime.RuntimeSafetyError, 'configfs or initiator'):
            runtime.admit_fabrics(config, 0, {}, '/tmp')

    def test_verbs_binds_each_host_without_mutating_case_defaults(self):
        transport = {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'}
        config = {'fleet': {'coordinator': {'rdma_address': '10.1.0.1'},
                            'nodes': [{'address': f'10.0.0.{i+2}', 'rdma_address': f'10.1.0.{i+2}'} for i in range(5)]}}
        prepared = runtime.prepare_transport(config, {'transport': transport})
        self.assertEqual(prepared['transport']['fabric']['source_address'], '10.1.0.1')
        self.assertEqual([value['fabric']['source_address'] for value in prepared['owner_transports']],
                         [f'10.1.0.{i+2}' for i in range(5)])
        single = runtime.prepare_transport(config, {'transport': transport, 'addresses': ['10.0.0.2:9400']})
        self.assertEqual([value['fabric']['source_address'] for value in single['owner_transports']],
                         ['10.1.0.2'])
        colocated = runtime.prepare_transport(config, {'transport': transport,
            'addresses': ['10.0.0.2:' + str(9400 + i) for i in range(16)]})
        self.assertEqual(len(colocated['owner_transports']), 16)
        self.assertEqual({owner['fabric']['source_address'] for owner in colocated['owner_transports']}, {'10.1.0.2'})
        self.assertEqual(transport, {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'})
        self.assertEqual(runtime.prepare_transport(config, {'transport': {'kind': 'tcp'}}),
                         {'transport': {'kind': 'tcp'}})

    def test_sui_only_verbs_admits_the_active_rdma_interface(self):
        config = {'spec': {'backends': ['native']},
                  'platform': {'runtime': {'fleet': fleet(),
                              'spec': {'suite': 'sui', 'backends': ['native'],
                                       'sui_transports': ['tidehunter_verbs_native']}}}}
        addresses = json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.2'}]}])
        with patch('subprocess.check_output',
                   side_effect=['link rdma0/1 state ACTIVE netdev eth1', addresses]):
            self.assertTrue(runtime.admit_fabrics(config, 0, {}, '/tmp')['rdma_requested'])
        with patch('subprocess.check_output',
                   side_effect=['link rdma0/1 state DOWN netdev eth1', addresses]):
            with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'active RDMA'):
                runtime.admit_fabrics(config, 0, {}, '/tmp')

    def test_valkey_rdma_requires_cm_and_its_admitted_nic(self):
        config = {'spec': {'backends': ['valkey_tcp', 'valkey_rdma']},
                  'platform': {'runtime': {'fleet': fleet()}}}
        addresses = json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.2'}]}])
        with patch('subprocess.run'), patch.object(Path, 'is_char_device', return_value=False):
            with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'RDMA-CM'):
                runtime.admit_fabrics(config, 0, {}, '/tmp')
        with patch('subprocess.run'), patch.object(Path, 'is_char_device', return_value=True), \
             patch('subprocess.check_output', side_effect=['link rdma0/1 state ACTIVE netdev eth1', addresses]):
            proof = runtime.admit_fabrics(config, 0, {}, '/tmp')
        self.assertTrue(proof['valkey_rdma_requested'])
        self.assertFalse(proof['nvme_rdma_requested'])
        self.assertEqual(proof['address'], '10.1.0.2')

    def test_runtime_accepts_real_comparison_backends_but_not_efa(self):
        expires = deadline()
        config = {'project_id': 'project', 'project_number': '123', 'bucket': 'bucket',
                  'machine_type': 'z4m-highmem-16', 'run_id': 'run', 'fleet': fleet(),
                  'source_sha256': 'a' * 64, 'deadline_utc': expires,
                  'campaign_deadline_utc': (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
                  'package_root': '/mnt/tidehunter/run/package',
                  'package': {'source_sha256': 'a' * 64, 'source_revision': 'b' * 40,
                              'object_key': 'runs/run/package.tar', 'generation': '1', 'sha256': 'c' * 64},
                  'spec': {'backends': ['native', 'tidehunter_tcp', 'tidehunter_verbs_native',
                                        'tikv', 'cassandra', 'foundationdb', 'valkey_tcp', 'valkey_rdma',
                                        'garnet', 'kvrocks']}}
        self.assertIs(runtime.validate_runtime(config), config)
        config['spec']['backends'].append('tidehunter_efa')
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.validate_runtime(config)
        config['spec'] = {'backends': ['native', 'tidehunter_nvme_rdma'], 'mode': 'standard'}
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'NVMe-oF mode'):
            runtime.validate_runtime(config)
        config['spec']['mode'] = 'nvmeof-focused'
        self.assertIs(runtime.validate_runtime(config), config)
        config['spec']['mode'] = 'standard-with-nvmeof'
        self.assertIs(runtime.validate_runtime(config), config)
        config['spec']['mode'] = 'standard-with-live-nvmeof'
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'live pair qualification'):
            runtime.validate_runtime(config)
        config['live_qualification'] = {
            'image': 'projects/project/global/images/qualified',
            'image_qualification': {'bucket': 'bucket',
                'object': 'runs/bench-aaaaaaaaaaaaaaaa/qualification/image-receipt.json',
                'generation': '1', 'sha256': 'd' * 64},
            'pair_hosts': [
                {'name': 'bench-aaaaaaaaaaaaaaaa-0', 'id': '200', 'zone': 'us-central1-a',
                 'internal_ip': '10.2.0.1', 'rdma_ip': '10.3.0.1'},
                {'name': 'bench-aaaaaaaaaaaaaaaa-1', 'id': '201', 'zone': 'us-central1-a',
                 'internal_ip': '10.2.0.2', 'rdma_ip': '10.3.0.2'}],
            'rdma_device': 'irdma0', 'service_account': 'guest@example.test'}
        self.assertIs(runtime.validate_runtime(config), config)
        config['package']['manifest_sha256'] = 'e' * 64
        hosts = config['live_qualification']['pair_hosts']
        records = [{'instance_id': host['id'], 'peer_instance_id': hosts[1-i]['id'],
                    'local_rdma': host['rdma_ip'], 'peer': hosts[1-i]['rdma_ip']}
                   for i, host in enumerate(hosts)]
        import gcp_api
        import gcp_image
        import manual_campaign
        api = SimpleNamespace(request=lambda *args: {'id': '1'})
        qualified = {'runtime_sha256': 'e' * 64, '_records': records}
        with patch.object(gcp_api, 'GoogleApi', return_value=api), \
             patch.object(gcp_image, 'admit_image', return_value=qualified), \
             patch.object(gcp_image, 'admit_nvme_transports'), \
             patch.object(manual_campaign, '_admit_live_pair'):
            runtime.admit_live_qualification(config)
            records[0]['peer_instance_id'] = '999'
            with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'host identity mismatch'):
                runtime.admit_live_qualification(config)
        config['spec']['mode'] = 'unexpected'
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'invalid matrix mode'):
            runtime.validate_runtime(config)

    def test_compact_live_requires_pair_bound_to_fleet(self):
        import manual_campaign
        run = 'bench-aaaaaaaaaaaaaaaa'
        hosts = [{**host, 'name': f'{run}-{i}'} for i, host in enumerate(fleet()[:2])]
        config = {'project_id': 'project', 'project_number': '123', 'bucket': 'bucket',
                  'machine_type': 'z4m-highmem-16', 'run_id': run, 'fleet': hosts,
                  'source_sha256': 'a' * 64, 'deadline_utc': deadline(),
                  'campaign_deadline_utc': (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
                  'package_root': f'/mnt/tidehunter/{run}/package',
                  'package': {'source_sha256': 'a' * 64, 'source_revision': 'b' * 40,
                              'object_key': f'runs/{run}/package.tar', 'generation': '1', 'sha256': 'c' * 64},
                  'spec': manual_campaign.REVIEWED_SPECS['leased_ab_compact']}
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'live pair qualification'):
            runtime.validate_runtime(config)
        config['live_qualification'] = {
            'image': 'projects/project/global/images/qualified',
            'image_qualification': {'bucket': 'bucket', 'object': f'runs/{run}/qualification/image-receipt.json',
                                    'generation': '1', 'sha256': 'd' * 64},
            'pair_hosts': [{k: h[k] for k in ('name', 'id', 'zone', 'internal_ip', 'rdma_ip')} for h in hosts],
            'rdma_device': 'irdma0', 'service_account': 'guest@example.test'}
        self.assertIs(runtime.validate_runtime(config), config)
        config['live_qualification']['pair_hosts'][0]['id'] = '999'
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'match the campaign fleet'):
            runtime.validate_runtime(config)
        config['fleet'] = fleet()
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'fleet size'):
            runtime.validate_runtime(config)

    def test_nvmeof_path_requires_live_qualification(self):
        import workflow_input
        for selection in workflow_input.cloud_campaign.NVMEOF_PATH_SELECTIONS:
            with self.subTest(selection=selection):
                self.assertTrue(runtime.live_mode({'mode': 'focused', 'selection': selection}))
                self.assertFalse(runtime.live_mode({'mode': 'standard', 'selection': selection}))
        self.assertFalse(runtime.live_mode({'mode': 'focused', 'selection': 'fabric_ab_compact'}))

    def test_industry_stores_do_not_request_rdma(self):
        config = {'spec': {'backends': ['garnet', 'kvrocks']}}
        with patch('subprocess.run') as run, patch('subprocess.check_output') as output:
            proof = runtime.admit_fabrics(config, 0, {}, '/tmp')
        self.assertFalse(proof['rdma_requested'])
        self.assertFalse(proof['valkey_rdma_requested'])
        self.assertFalse(proof['nvme_rdma_requested'])
        run.assert_not_called()
        output.assert_not_called()

    def test_focused_guest_bootstrap_skips_external_store_image_inspection(self):
        import cloud_bootstrap
        import cloud_controller
        import cloud_package
        import local
        config = {'spec': {'mode': 'focused'}, 'platform': {'runtime': {'package_root': '/package'}},
                  'source_sha256': 'a' * 64, 'binary_sha256': 'b' * 64,
                  'fleet': {'data_root': '/tmp'}, 'prefix': 'run', 'repository_root': '/source',
                  'deadline_utc': deadline()}
        package = {'source_sha256': 'a' * 64, 'binary_sha256': 'b' * 64, 'build_profile': 'focused'}
        with patch.object(runtime, 'verify_local_identity', return_value={'name': 'host', 'instance_id': '1'}), \
                patch.object(runtime, 'admit_guest_runtime', return_value={}), \
                patch.object(runtime, 'verify_fleet', return_value=[]), \
                patch.object(runtime, 'admit_nvmet_fix', return_value=None), \
                patch.object(runtime, 'admit_guest_unit', return_value={}), \
                patch.object(runtime, 'admit_fabrics', return_value={}), \
                patch.object(cloud_package, 'verify_directory', return_value=package), \
                patch.object(cloud_controller, 'snapshot', return_value={}), \
                patch.object(cloud_bootstrap, 'cache_monitor_images', return_value={'complete': True}), \
                patch('subprocess.run'), patch('subprocess.check_output', return_value='[]') as inspect:
            focused = runtime.bootstrap_guest(config, 0, {})
            self.assertEqual(focused['images'], {})
            inspect.assert_not_called()
            config['spec']['mode'] = 'standard-with-live-nvmeof'
            package.pop('build_profile')
            full = runtime.bootstrap_guest(config, 0, {})
            self.assertEqual(set(full['images']), set(local.IMAGES))
            self.assertEqual(inspect.call_count, len(local.IMAGES))

    def test_guest_unit_rejects_unsafe_process_limits_and_identity(self):
        import cloud_controller
        config = {'prefix': 'bench-0123456789abcdef',
                  'repository_root': '/tmp/source',
                  'fleet': {'client_cpus': 1, 'server_cpus': 1,
                            'client_memory_mib': 512, 'server_memory_mib': 512}}
        valid = {'executable': runtime.GUEST_PYTHON, 'yaml': '6.0.2',
                 'yaml_check': {'x': 1}, 'nofile': [524288, 524288]}
        status = {'LimitNOFILE': '524288', 'LimitNOFILESoft': '524288',
                  'ActiveState': 'active', 'SubState': 'exited', 'ExecMainStatus': '0', 'Result': 'success'}
        def execute(argv, **kwargs):
            if argv[0] == 'systemd-run':
                output = next(arg.split('file:', 1)[1] for arg in argv if arg.startswith('--property=StandardOutput=file:'))
                Path(output).write_text(json.dumps(valid))
            return unittest.mock.Mock()
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(cloud_controller, 'environment', return_value={}), \
             patch.object(cloud_controller, 'unit_status', return_value=status), \
             patch('subprocess.run', side_effect=execute) as run:
            config['fleet']['data_root'] = directory
            for field, value, error in (('nofile', [1024, 524288], 'effective NOFILE'),
                                        ('executable', '/usr/bin/python3', 'guest Python')):
                invalid = dict(valid)
                invalid[field] = value
                with self.subTest(field=field), patch.object(Path, 'read_text', return_value=json.dumps(invalid)):
                    with self.assertRaisesRegex(runtime.RuntimeSafetyError, error):
                        runtime.admit_guest_unit(config, -1)
                self.assertEqual(run.call_args.args[0], ['systemctl', 'stop',
                    cloud_controller.unit_name(config, 'startup-runtime-admission', 'client')])




class ReviewedSuiRuntimeTests(unittest.TestCase):
    def config(self):
        import copy
        import manual_sui
        run = 'bench-aaaaaaaaaaaaaaaa'
        hosts = [{**host, 'name': f'{run}-{i}'} for i, host in enumerate(fleet()[:2])]
        spec = copy.deepcopy(manual_sui.REVIEWED_SUI_SPECS['sui_nvmeof_compact'])
        spec['sui_fixture'].update(key='fixture', generation='1')
        spec['sui_verification'] = dict(key='proof', generation='2', sha256='f'*64)
        return dict(project_id='project', project_number='123', bucket='bucket',
            machine_type='z4m-highmem-16', run_id=run, fleet=hosts,
            source_sha256='a'*64, deadline_utc=deadline(),
            campaign_deadline_utc=(datetime.now(timezone.utc)+timedelta(minutes=2)).isoformat(),
            package_root=f'/mnt/tidehunter/{run}/package',
            package=dict(source_sha256='a'*64, source_revision='b'*40,
                object_key=f'runs/{run}/package.tar', generation='1', sha256='c'*64), spec=spec,
            live_qualification=dict(image='projects/project/global/images/qualified',
                image_qualification=dict(bucket='bucket', object=f'runs/{run}/qualification/image-receipt.json',
                                         generation='1', sha256='d'*64),
                pair_hosts=[{k: h[k] for k in ('name','id','zone','internal_ip','rdma_ip')} for h in hosts],
                rdma_device='irdma0', service_account='guest@example.test'))

    def test_reviewed_compact_sui_requires_live_pair_bound_to_fleet(self):
        config = self.config()
        self.assertTrue(runtime.live_mode(config['spec']))
        self.assertIs(runtime.validate_runtime(config), config)
        config['live_qualification']['pair_hosts'][0]['id'] = '999'
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'match the campaign fleet'):
            runtime.validate_runtime(config)
        config = self.config()
        config.pop('live_qualification')
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'live pair qualification'):
            runtime.validate_runtime(config)
        config = self.config()
        config['spec']['sui_cells'][0]['owner_tier'] = 'synchronous'
        with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'compact fleet'):
            runtime.validate_runtime(config)

    def test_sui_worker_admits_nvme_cells_and_live_proof_before_storage(self):
        import gcp_worker
        config = self.config()
        host = config['fleet'][1]
        with patch.object(runtime, 'validate_runtime'), \
             patch.object(gcp_worker, 'identify', return_value=host), \
             patch.object(gcp_worker.cloud_package, 'verify_directory', return_value={'source_sha256': 'a'*64}), \
             patch.object(runtime, 'verify_fleet'), patch.object(runtime, 'admit_nvmet_fix'), \
             patch.object(runtime, 'admit_fabrics') as fabrics, \
             patch.object(runtime, 'admit_live_qualification') as proof, \
             patch.object(gcp_worker, 'prepare_storage', side_effect=RuntimeError('storage boundary')):
            with self.assertRaisesRegex(RuntimeError, 'storage boundary'):
                gcp_worker.serve(config, api=object())
        self.assertEqual(fabrics.call_args.args[0]['spec']['backends'], ['tidehunter_nvme_rdma'])
        proof.assert_called_once_with(config)

    def test_sui_guest_revalidates_focused_live_image_receipt(self):
        import gcp_api
        import gcp_image
        import manual_campaign
        config = self.config()
        config['package']['manifest_sha256'] = 'e'*64
        fixture = json.loads((Path(__file__).parent/'fixtures/sui-library-abi.json').read_text())
        config['package']['abi'] = fixture['records'][0]['outputs']['package']['abi']
        config['package']['sui_abi'] = fixture['sui_abi']
        hosts = config['live_qualification']['pair_hosts']
        records = [dict(instance_id=host['id'], peer_instance_id=hosts[1-i]['id'],
                        local_rdma=host['rdma_ip'], peer=hosts[1-i]['rdma_ip'])
                   for i, host in enumerate(hosts)]
        for record in records:
            record['outputs'] = {'package': {'sui_library_linkage': {'ld_library_path': None}}}
        qualification = dict(runtime_sha256='e'*64, _records=records)
        api = SimpleNamespace(request=lambda *args: {'id': '1'})
        with patch.object(gcp_api, 'GoogleApi', return_value=api), \
             patch.object(gcp_image, 'admit_image', return_value=qualification) as image, \
             patch.object(gcp_image, 'admit_nvme_transports'), \
             patch.object(manual_campaign, '_admit_live_pair'), \
             patch('gcp_worker.identify', return_value={'index':0}), patch.dict('os.environ', {}, clear=True):
            runtime.admit_live_qualification(config)
            with patch.dict('os.environ', {'LD_LIBRARY_PATH':'/changed'}):
                with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'search path differs'):
                    runtime.admit_live_qualification(config)
        self.assertEqual(image.call_args.args[1]['qualification_profile'], 'focused')


class ReviewedCombinedRuntimeTests(ReviewedSuiRuntimeTests):
    spec_name = 'sui_nvmeof_with_kv'

    def config(self):
        import copy
        import manual_sui
        config=super().config()
        config['spec']=copy.deepcopy(manual_sui.REVIEWED_SUI_SPECS[self.spec_name])
        config['spec']['sui_fixture'].update(key='fixture',generation='1')
        config['spec']['sui_verification']=dict(key='proof',generation='2',sha256='f'*64)
        return config

    def test_controller_gets_only_the_exact_focused_kv_spec(self):
        import manual_sui
        config=self.config()
        import cloud_package
        manifest=dict(revision='b'*40,source_sha256=config['source_sha256'],
                      binary_sha256='c'*64,binary_blake2b512='d'*128,build_profile='focused',
                      features=cloud_package.FOCUSED_FEATURES,
                      files={'source/experiments/kv-comparison/nvme_campaign_owner.py':{}})
        with patch.object(Path,'read_text',return_value=json.dumps(manifest)):
            controller=runtime._controller_config(config)
        expected=manual_sui.kv_spec(config['spec'])
        expected.pop('suite')
        self.assertEqual(controller['spec'],expected)
        self.assertFalse(any(key.startswith('sui_') for key in controller['spec']))


    def test_kv_report_continues_to_sui_but_kv_exception_does_not(self):
        import gcp_sui
        import cloud_controller
        from unittest.mock import Mock
        config=self.config()
        api=Mock()
        for failed in (False, True):
            controller=Mock()
            if failed:
                controller.run.side_effect=RuntimeError('KV exception')
            else:
                controller.run.return_value=dict(complete=False,planned_cases=config['spec']['job_count'],
                                                 completed_cases=config['spec']['job_count']-1)
            with self.subTest(failed=failed), patch.object(runtime,'validate_runtime'), \
                 patch('gcp_worker.identify',return_value={'index':0}), \
                 patch.object(runtime,'RemoteMailbox'), patch.object(runtime,'_controller_config',return_value={}), \
                 patch.object(runtime,'make_clients'), patch.object(runtime,'verify_fleet',return_value=[]), \
                 patch.object(runtime,'admit_live_qualification'), patch.object(runtime,'_put_once'), \
                 patch.object(cloud_controller,'Controller',return_value=controller), \
                 patch.object(gcp_sui,'run_suite',return_value={'complete':True}) as sui:
                if failed:
                    with self.assertRaisesRegex(RuntimeError,'KV exception'):
                        runtime.execute_campaign(config,api=api)
                    sui.assert_not_called()
                else:
                    result=runtime.execute_campaign(config,api=api)
                    self.assertFalse(result['kv']['complete'])
                    self.assertTrue(result['sui']['complete'])
                    sui.assert_called_once()


class ReviewedContractsRuntimeTests(ReviewedCombinedRuntimeTests):
    spec_name = 'sui_nvmeof_with_contracts'


class CrossingSuiTests(ReviewedCombinedRuntimeTests):
    spec_name = 'sui_nvmeof_with_contracts_crossing'


class Crossing2SuiTests(ReviewedCombinedRuntimeTests):
    spec_name = 'sui_nvmeof_with_contracts_crossing2'


if __name__ == '__main__':
    unittest.main()
