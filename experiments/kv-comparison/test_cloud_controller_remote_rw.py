import base64
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import cloud_controller as controller
import nvme_campaign_native


class CompactReadLifecycleTests(unittest.TestCase):
    def test_nvmeof_path_reviewed_cases_pass_compact_admission(self):
        from test_cloud_controller import config_fixture, clients_fixture
        for selection in controller.cloud_campaign.NVMEOF_PATH_SELECTIONS:
            with self.subTest(selection=selection):
                config = config_fixture()
                config['fleet']['nodes'] = config['fleet']['nodes'][:1]
                config['spec'].update(mode='focused', selection=selection)
                runtime = controller.Controller(config, clients_fixture(config))
                runtime.run_floor_case = MagicMock(return_value='floor')
                runtime.run_fio_case = MagicMock(return_value='fio')
                jobs = controller.cloud_campaign.make_focused_matrix(selection)
                with tempfile.TemporaryDirectory() as tmp:
                    runtime.root = Path(tmp)
                    for job in jobs:
                        with self.subTest(case=job['case_id']):
                            if job['case_kind'] in ('net_floor', 'ssd_floor', 'ssd_write_floor'):
                                self.assertEqual(runtime.run_case(job), 'floor')
                            elif job['case_kind'] == 'fio_floor':
                                self.assertEqual(runtime.run_case(job), 'fio')
                            else:
                                with patch.object(controller, 'guarded', side_effect=RuntimeError('admitted')):
                                    with self.assertRaisesRegex(RuntimeError, 'admitted'):
                                        runtime.run_case(job)
                    remote = next(j for j in jobs if j.get('nvmeof_route') == 'live')
                    for mutated in (dict(remote, selection='leased_ab_compact'),
                                    dict(remote, owner_count=5), dict(remote, history_persistence='unexpected')):
                        with self.subTest(mutated=mutated), self.assertRaises(ValueError):
                            runtime.run_case(mutated)

    def test_live_compact_preparation_boundaries_and_cleanup(self):
        import threading
        import yaml
        jobs = controller.cloud_campaign.make_focused_matrix('leased_ab_compact')
        cases = [next(j for j in jobs if j.get('live_write_mode') == mode
                      and j['durability_tier'] == tier and j['group'] == 'G17W')
                 for mode in ('reserved', 'leased') for tier in ('native', 'synchronous')]
        cases += [next(j for j in jobs if j.get('live_inline_reads') is inline)
                  for inline in (False, True)]
        cases.append(next(j for j in controller.cloud_campaign.make_focused_matrix('nvmeof_contracts')
                          if j['measurement_operations'] == 200000 and j['durability_tier'] == 'synchronous'))
        nqn = 'nqn.2014-08.org.nvmexpress:uuid:01234567-89ab-4def-8123-456789abcdef'
        for job in cases:
            with self.subTest(case=job['case_id']), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                runtime = controller.Controller.__new__(controller.Controller)
                runtime.root = root
                runtime.config = {'prefix': 'test', 'binary': '/unused',
                    'fleet': {'data_root': str(root), 'nodes': [{'address': '10.0.0.2'}],
                              'server_memory_mib': 8192},
                    'spec': {'mode': 'focused', 'selection': job['selection'],
                             'backends': ['tidehunter_verbs_native', 'tidehunter_nvme_rdma'],
                             'case_deadline_seconds': 600}}
                runtime.clients = {}
                runtime.sequence_lock = threading.Lock()
                runtime.active_tide = runtime.active_nvme = runtime.shared_tide = None
                runtime.tide_cleanup_forbidden = False
                runtime.remaining = MagicMock(return_value=600)
                runtime.cleanup_deadline = MagicMock(return_value=100)
                runtime.cleanup_timeout = MagicMock(return_value=30)
                runtime.request = MagicMock(return_value={'complete': True})
                case_dir = root / job['case_id']
                case_dir.mkdir()
                def fake_command(argv, **kwargs):
                    prep = json.loads(Path(argv[2]).read_text())
                    out = Path(argv[-1])
                    out.mkdir()
                    live = prep['live_nvmeof']
                    client = {key: live[key] for key in ('max_inflight_requests', 'max_inflight_bytes',
                              'connect_timeout_ms', 'io_timeout_ms', 'write_mode', 'inline_reads',
                              'lease_ahead', 'polled_writes')}
                    client.update(queue_geometry=live['queue_geometry'], export_mode=live['export_mode'])
                    client.update(host_nqn=nqn, host_id=controller.live_host_identity(nqn),
                                  leased_durability=prep['durability_tier'] if live['write_mode'] == 'leased' else None)
                    controller.dump(out / 'remote.json', {'tls': {}, 'live_nvme': client})
                    controller.dump(out / 'external_owner_read_config.json', [])
                    node = out / 'node-0'
                    node.mkdir()
                    target = controller.live_target_for_owner(
                        runtime.config, job['case_id'], 0, 'rdma', nqn, export_mode=live['export_mode'])
                    fragment, wal_file = ((134217728, 10737418240)
                                          if prep['owner_wal_geometry'] == 'native_default'
                                          else (4194304, 1073741824))
                    (node / 'launch.yaml').write_text(yaml.safe_dump({
                        'tls': {}, 'history_persistence': prep['history_persistence'],
                        'node': {'live_target': target,
                                 'partition': {'fragment_bytes': fragment, 'wal_file_bytes': wal_file}}}))
                    return ''
                with patch.object(controller, 'command', side_effect=fake_command), \
                     patch.object(controller, 'platform_module', return_value=None), \
                     patch.object(controller, 'environment', return_value={}), \
                     patch.object(controller, 'upload_object'), \
                     patch.object(controller, 'validate_launch'), \
                     patch.object(controller, 'confirm_owner_durability', return_value={'partition': 0}):
                    options = runtime.prepare_tide(job, case_dir)
                    prep = json.loads((case_dir / 'prepare-input.json').read_text())
                    self.assertEqual(prep['addresses'], ['10.0.0.2:9400'])
                    self.assertEqual(prep['live_nvmeof']['addresses'], ['10.0.0.2'])
                    self.assertEqual(prep['live_nvmeof']['write_mode'], job['live_write_mode'])
                    self.assertEqual(prep['live_nvmeof']['inline_reads'], job.get('live_inline_reads', True))
                    self.assertEqual(prep['live_nvmeof']['export_mode'], job.get('live_export_mode', 'on_demand'))
                    self.assertEqual(prep['durability_tier'], job['durability_tier'])
                    self.assertEqual(prep['history_persistence'], job.get('history_persistence', 'on_disk'))
                    self.assertTrue(prep['live_nvmeof']['lease_ahead'])
                    self.assertFalse(prep['live_nvmeof']['polled_writes'])
                    self.assertEqual(options['owners'], 1)
                    rows = runtime.live_boundaries(job['case_id'], 'begin', case_dir,
                                                   {'host_nqn': nqn, 'transport': 'rdma'})
                    self.assertEqual([row['index'] for row in rows], [-1, 0])
                    runtime.stop_tide()
                    calls = [(c.args[0], c.args[1]['action']) for c in runtime.request.call_args_list]
                    self.assertIn((0, 'tide_start'), calls)
                    self.assertIn((0, 'tide_stop'), calls)
                    self.assertEqual({index for index, action in calls if action == 'live_case_snapshot'}, {-1, 0})
                    self.assertIsNone(runtime.active_tide)

    def test_reviewed_read_arms_and_floors_only_pass_compact_admission(self):
        from test_cloud_controller import config_fixture, clients_fixture
        config = config_fixture()
        config['fleet']['nodes'] = config['fleet']['nodes'][:1]
        config['spec'].update(mode='focused', selection='remote_rw_compact')
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.run_floor_case = MagicMock(return_value='floor')
        runtime.run_fio_case = MagicMock()
        jobs = controller.cloud_campaign.make_focused_matrix('remote_rw_compact')
        reads = [next(j for j in jobs if j['group'] == group and j['backend'] == backend)
                 for group in ('G15C', 'G15M')
                 for backend in ('native', 'tidehunter_tcp', 'tidehunter_verbs_native')]
        floors = [next(j for j in jobs if j['group'] == group) for group in ('G11N', 'G11S', 'G15F')]
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            for job in reads:
                with (self.subTest(group=job['group'], backend=job['backend']),
                      patch.object(controller, 'guarded', side_effect=RuntimeError('admitted'))):
                    with self.assertRaisesRegex(RuntimeError, 'admitted'):
                        runtime.run_case(job)
            for job in floors:
                self.assertEqual(runtime.run_case(job), 'floor')
            runtime.run_floor_case.reset_mock()
            rejected = [{**reads[1], 'nvmeof_route': 'sealed_read'},
                        {**reads[1], 'backend': 'tidehunter_nvme_rdma'},
                        *[{**reads[1], 'case_kind': kind} for kind in ('hybrid_put', 'interference', 'fio_floor', 'block_trace')]]
            for job in rejected:
                with self.subTest(job=job), self.assertRaises(ValueError):
                    runtime.run_case(job)
            runtime.run_floor_case.assert_not_called()
            runtime.run_fio_case.assert_not_called()
            self.assertEqual(list(runtime.root.iterdir()), [])

    def test_fake_fleet_prepares_gates_qualifies_and_cleans(self):
        jobs = controller.cloud_campaign.make_focused_matrix('remote_rw_compact')
        cases = [next(j for j in jobs if j['group'] == group and j['backend'] == backend)
                 for group, backend in (('G15C', 'tidehunter_tcp'),
                                        ('G15M', 'tidehunter_tcp'), ('G15C', 'native'))]
        fabric = controller.cloud_campaign.make_focused_matrix('fabric_ab_compact')
        cases += [next(j for j in fabric if j['backend'] == backend and j.get('nvmeof_target') == target)
                  for backend, target in (('tidehunter_nvme_rdma', 'kernel'),
                                          ('tidehunter_nvme_rdma', 'spdk'),
                                          ('tidehunter_nvme_tcp', 'kernel'))]
        for job in cases:
            with self.subTest(group=job['group'], backend=job['backend']), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                runtime = controller.Controller.__new__(controller.Controller)
                runtime.root = root
                runtime.config = {'prefix': 'test', 'binary': '/unused', 'binary_blake2b512': 'a' * 128,
                    'source_revision': 'b' * 40, 'repository_root': '/unused',
                    'fleet': {'data_root': str(root), 'nodes': [{'address': '10.0.0.2'}],
                              'client_cpus': 8, 'client_memory_mib': 8192, 'server_memory_mib': 8192},
                    'spec': {'mode': 'focused', 'selection': job.get('selection', 'remote_rw_compact'), 'case_deadline_seconds': 600}}
                runtime.clients = {}
                runtime.sequence_lock = __import__('threading').Lock()
                runtime.active_backend = runtime.active_tide = runtime.active_nvme = runtime.shared_tide = None
                runtime.shared_native = {}
                runtime.tide_cleanup_forbidden = False
                runtime.local_mode = MagicMock()
                runtime.local_mode.snapshot_client.return_value = {}
                runtime.local_mode.client_logs.return_value = ''
                runtime.monitoring_complete = True
                runtime.monitoring_attempted = False
                runtime.monitoring_events = []
                runtime.select_monitoring = MagicMock()
                runtime.finish_monitoring = MagicMock()
                runtime.guard = MagicMock()
                runtime.remaining = MagicMock(return_value=600)
                runtime.clock = MagicMock(return_value=1)
                runtime.cleanup_deadline = MagicMock(return_value=100)
                runtime.cleanup_timeout = MagicMock(return_value=30)
                runtime.observe = lambda operation, action: action()
                runtime.observe_resource_interval = MagicMock(return_value={})
                runtime.provider_facts = MagicMock(return_value={})
                runtime.stage_nvme_manifests = MagicMock(return_value=[])
                runtime.refresh_owner_receipts = MagicMock()
                nvme = job['backend'] in controller.cloud_campaign.NVME_BACKENDS
                transport = 'tcp' if job['backend'] == 'tidehunter_nvme_tcp' else 'rdma'
                implementation = 'spdk' if job.get('nvmeof_target') == 'spdk' else 'kernel_direct'
                policy = {'selected_transport': transport, 'target_cache_verified': True,
                          'target_cache': ('direct_file_bdev_no_OS_pagecache' if implementation == 'spdk'
                                           else 'direct_file_no_OS_pagecache'),
                          'target_implementation': implementation, 'direct_io_verified': True,
                          'read_only_verified': True}
                targets = [{'target': {'exports': [{'target_receipt': policy}]}}]
                counters = {'eligible_hits': 10, 'ineligible_fallbacks': 0, 'io_submissions': 10,
                            'io_completions': 10, 'io_errors': 0, 'io_timeouts': 0}
                interval = {'observed': {'nvmeof': {'available': True, 'readers': {'0': {
                    'identity': {'transport': transport}, 'counters': {'available': True, 'counters': counters}}}},
                    'deltas': {'coordinator': {'available': True, 'counters': {'read_rpc_count': 0, 'hits': 0}}}}}
                def connect(selected, case_directory):
                    runtime.active_nvme = {'transport': transport, 'targets': targets}
                    controller.dump(case_directory / 'nvme-targets.json', targets)
                    return [{'binding': {'transport': transport}}]
                def disconnect(*, drained):
                    self.assertTrue(drained)
                    runtime.active_nvme = None
                if nvme:
                    runtime.connect_nvme = MagicMock(side_effect=connect)
                    runtime.disconnect_nvme = MagicMock(side_effect=disconnect)
                    runtime.spdk_boundary = MagicMock(return_value={'path': 'fake', 'sha256': 'a' * 64})
                calls = []
                def request(index, request, **kwargs):
                    calls.append((index, request['action']))
                    if request['action'] == 'nvme_owner_export':
                        prepared = request['prepared']
                        return {'complete': True, 'ready': {**{key: prepared[key] for key in (
                            'namespace', 'seed', 'keys', 'value_bytes', 'entropy', 'source_epoch',
                            'preload_recipe', 'preload_batch_max_members')}, 'partition': 0,
                            'members': job['keys'], 'exports': [{'members': job['keys']}]}}
                    return {'complete': True}
                runtime.request = MagicMock(side_effect=request)
                frames = []
                def all_hosts(request, **kwargs):
                    frames.append(request)
                    end = len(frames) > 1
                    hosts = []
                    for index in (-1, 0):
                        hosts.append({'nvme': {'host_identity': {'boot_id': f'boot-{index}'}},
                            'media_device': {'owner_index': index, 'major': 259, 'minor': index + 1,
                                             'read_ops': 19 if end else 10, 'read_sectors': 0,
                                             'write_ops': 0, 'write_sectors': 0},
                            'resource_accounting': {}, 'scoped_file_eviction': True})
                    return hosts
                runtime.all_hosts = MagicMock(side_effect=all_hosts)
                def fake_command(argv, **kwargs):
                    if '--prepare-external-case' in argv:
                        directory = Path(argv[-1]); directory.mkdir()
                        (directory / 'remote.json').write_text(json.dumps({'tls': {}}))
                        (directory / 'external_owner_read_config.json').write_text('[]')
                        node = directory / 'node-0'; node.mkdir()
                        (node / 'launch.yaml').write_text('tls: {}\nnode:\n  partition:\n    fragment_bytes: 4194304\n    wal_file_bytes: 1073741824\n')
                    elif 'prepare-nvmeof-dataset' in argv:
                        cfg = json.loads(Path(argv[2]).read_text())
                        data = {'schema_version': 1, 'keys': job['keys'], 'value_bytes': job['value_size'],
                            'seed': cfg['seed'], 'namespace': list(bytes.fromhex(cfg['key_namespace_hex'])),
                            'entropy': 1.0, 'source_epoch': 1, 'database': 'db', 'partitions': [0],
                            'preload_recipe': 'kv-research-native-sealed-batch-v2', 'preload_batch_max_members': 32}
                        controller.dump(Path(argv[3]), data)
                    return ''
                directory = root / 'cases' / job['case_id']
                def start(client, executable, budget, seconds):
                    result = directory / 'result'; result.mkdir()
                    controller.dump(result / 'manifest.json', {'schema_version': 1, 'status': 'completed',
                        'backend_info': {}, 'raw_samples_complete': True})
                    controller.dump(result / 'summary.json', {'status': 'completed', 'backend': {}})
                    (result / 'samples.csv').write_text('samples')
                    (result / 'metrics.prom').write_text('')
                    if job.get('evict_mapped_wal_before_measurement'):
                        controller.dump(result / 'pre-measurement-wal-eviction.json',
                                        {'requested': True, 'available': True, 'evidence': {}})
                    if job['backend'] != 'native':
                        evidence = directory / 'read-evidence'; evidence.mkdir()
                        (evidence / 'read-evidence.json').write_text('{}')
                    (directory / 'measurement.ready').touch()
                    (directory / 'measurement.complete').touch()
                runtime.local_mode.start_client.side_effect = start
                statuses = [{'SubState': 'running', 'ActiveState': 'active'},
                            {'SubState': 'exited', 'ActiveState': 'inactive', 'Result': 'success', 'ExecMainStatus': '0'},
                            {'LoadState': 'loaded', 'ActiveState': 'inactive', 'MainPID': '0'}]
                def counts(result, admitted, **kwargs):
                    return {'preload': 0, 'warmup': job['warmup_operations'], 'aging': 0,
                            'measurement': job['measurement_operations'], 'verify_existing_dataset': job['keys']}
                def native_prepare(binary, config, path, timeout, environment):
                    path.mkdir(parents=True, exist_ok=True)
                    database = path / 'database'; database.mkdir()
                    return {'database_path': str(database)}
                with ExitStack() as stack:
                    def mock(obj, name, **kwargs):
                        return stack.enter_context(patch.object(obj, name, **kwargs))
                    mock(controller, 'command', side_effect=fake_command)
                    mock(controller, 'environment', return_value={})
                    mock(controller, 'platform_module', return_value=None)
                    mock(controller, 'upload_object')
                    mock(controller, 'validate_launch')
                    mock(controller, 'confirm_owner_durability', side_effect=lambda proof, tier, index, nonce: {'partition': index})
                    mock(controller, 'unit_status', side_effect=statuses)
                    mock(controller, 'verify_counter_evidence')
                    mock(controller, 'host_cpu_interval', return_value={})
                    mock(controller, 'bind_native_dataset_config', side_effect=lambda receipt, cfg, job: cfg)
                    mock(controller, 'cleanup_native_media_dataset', return_value=None)
                    mock(controller.shutil, 'disk_usage', return_value=type('Disk', (), {'free': 10**13})())
                    mock(nvme_campaign_native, 'prepare_native', side_effect=native_prepare)
                    mock(controller.campaign, 'admit_config', side_effect=lambda binary, path, *a, **k: json.loads(Path(path).read_text()))
                    mock(controller.campaign, 'verify_run_identity')
                    mock(controller.campaign, 'verify_timing_result', side_effect=counts)
                    mock(controller.analyze, 'verify_normalization')
                    mock(controller.analyze, 'read_samples', return_value=iter([{'operation': 'read', 'outcome': 'success'}] * 10))
                    mock(controller.resource_accounting, 'interval', return_value={})
                    for name in ('admit_write_diagnostics', 'admit_read_diagnostics',
                                 'admit_valkey_diagnostics', 'admit_write_timeline',
                                 'admit_spdk_target_measurement'):
                        mock(controller.cloud_campaign, name, return_value=None)
                    mock(controller.cloud_campaign, 'admit_measurement_read_evidence',
                         return_value=interval if nvme else None)
                    receipt = runtime.run_case(job)
                    if job['backend'] != 'native':
                        runtime.stop_tide()
                self.assertEqual(receipt['status'], 'completed', receipt.get('error'))
                self.assertTrue((directory / 'measurement.release').exists())
                self.assertTrue((directory / 'measurement.observed').exists())
                self.assertEqual(frames[0]['drop'], job.get('drop_os_caches', False))
                config = json.loads((directory / 'config.json').read_text())
                self.assertEqual(config['phases']['evict_mapped_wal_before_measurement'],
                                 job.get('evict_mapped_wal_before_measurement', False))
                if job['group'] == 'G15C':
                    self.assertEqual(receipt['wal_eviction_evidence']['observed']['available'], True)
                    self.assertEqual(receipt['measurement_evidence']['mapped_wal_eviction'], 'completed_before_gate')
                    media = json.loads((directory / 'media_evidence.json').read_text())
                    self.assertEqual(media['status'], 'qualified')
                    self.assertEqual(media['read_ops_per_success'], 0.9)
                    devices = media['client_devices' if job['backend'] == 'native' else 'owner_devices']
                    self.assertEqual(len(devices), 1)
                    self.assertEqual(devices[0].get('client_index', devices[0].get('owner_index')),
                                     -1 if job['backend'] == 'native' else 0)
                if nvme:
                    self.assertTrue(receipt['nvmeof_measurement_qualification']['qualified'])
                    self.assertEqual(receipt['nvmeof_measurement_qualification']['expected_transport'], transport)
                    self.assertEqual(receipt['owner_control_backend'], 'tidehunter_verbs_native')
                    self.assertEqual(receipt['media_evidence']['path'], 'media_evidence.json')
                    self.assertGreaterEqual(sum(frame['action'] == 'snapshot' for frame in frames), 2)
                    media = json.loads((directory / 'media_evidence.json').read_text())
                    self.assertEqual(media['status'], 'qualified')
                    runtime.disconnect_nvme.assert_called_once_with(drained=True)
                    if implementation == 'spdk':
                        self.assertEqual(runtime.spdk_boundary.call_count, 2)
                if job['backend'] != 'native':
                    self.assertEqual(len(receipt['owner_durability_receipts']), 1)
                    self.assertEqual(Path(receipt['owner_durability_receipts'][0]['path']).name, 'node-0.durability.json')
                    self.assertEqual({index for index, _ in calls}, {0})
                    self.assertIn((0, 'tide_start'), calls)
                    self.assertIn((0, 'nvme_owner_export'), calls)
                    self.assertIn((0, 'nvme_owner_restart'), calls)
                    self.assertIn((0, 'tide_stop'), calls)
                    self.assertIsNone(runtime.active_tide)
                else:
                    self.assertEqual(calls, [])
                    self.assertEqual(receipt['dataset_source']['physical_scope'], 'native local dataset')
                    self.assertEqual(receipt['native_dataset_cleanup'], 'completed')

    def test_cold_device_ratio_requires_nine_reads_per_ten_calls(self):
        before = [{'owner_index': 0, 'major': 259, 'minor': 1, 'read_ops': 10,
                   'read_sectors': 0, 'write_ops': 0, 'write_sectors': 0}]
        after = [{**before[0], 'read_ops': 18}]
        result = controller.qualify_media_evidence(
            'case', {'expected': 'cold', 'history': 'normal'}, before, after, 10, True)
        self.assertEqual(result['status'], 'unqualified')
        self.assertIn('cold_read_ratio_below_threshold', result['reason_codes'])

    def test_client_ssd_floor_binds_guest_receipt_and_artifact(self):
        from test_floors import FloorTests
        job = next(j for j in controller.cloud_campaign.make_focused_matrix('remote_rw_compact')
                   if j['group'] == 'G15F' and j['ssd']['queue_depth'] == 1 and j['ssd']['block_bytes'] == 4096)
        raw = json.dumps({'jobs': [{'read': {'total_ios': 1, 'io_bytes': 4096, 'runtime': 1000,
            'clat_ns': {'percentile': {'50.000000': 1, '99.000000': 2, '99.900000': 3}}},
            'write': {'total_ios': 0}, 'error': 0, 'iodepth_level': {'1': 100}}]}).encode()
        log = b'1, 2, 0, 4096, 0, 0\n'
        for reported in (-1, 0):
            with self.subTest(reported=reported), tempfile.TemporaryDirectory() as tmp:
                runtime = FloorTests().controller(tmp)
                runtime.request = MagicMock(return_value={'raw_base64': base64.b64encode(raw).decode(),
                    'lat_base64': base64.b64encode(log).decode(), 'argv': ['fio', '--readonly'],
                    'device_identity': '259:0', 'opened_read_only': True, 'actual_direct': True,
                    'host_index': reported})
                if reported == 0:
                    with self.assertRaises(controller.SafetyError):
                        runtime.run_floor_case(job)
                    continue
                receipt = runtime.run_floor_case(job)
                self.assertEqual(runtime.request.call_args.args[0], -1)
                self.assertEqual(receipt['status'], 'completed')
                artifact = json.loads((Path(tmp) / 'cases' / job['case_id'] / 'ssd_floor_result.json').read_text())
                self.assertEqual((artifact['host_index'], receipt['host_index']), (-1, -1))
                controller.analyze._validate_focused_artifact('ssd_floor_result', artifact, job, receipt)
                for altered in ({**artifact, 'host_index': 0}, {**artifact, 'host_index': True}):
                    with self.assertRaises(ValueError):
                        controller.analyze._validate_focused_artifact('ssd_floor_result', altered, job, receipt)
                with self.assertRaises(ValueError):
                    controller.analyze._validate_focused_artifact('ssd_floor_result', artifact, job,
                                                                 {**receipt, 'host_index': 0})

    def test_client_ssd_guest_rejects_wrong_host_before_scratch(self):
        job = next(j for j in controller.cloud_campaign.make_focused_matrix('remote_rw_compact')
                   if j['group'] == 'G15F')
        with tempfile.TemporaryDirectory() as tmp:
            config = {'fleet': {'data_root': tmp, 'nodes': [{'index': 0}]},
                      'spec': {'mode': 'focused'}, 'deadline_utc': '2099-01-01T00:00:00Z'}
            request = {'index': 0, 'action': 'ssd_floor', 'case_id': job['case_id'],
                       'job': job, 'quiescent': True, 'timeout': 60}
            with (patch.object(controller, 'verify_local_identity', return_value={}),
                  patch.object(controller, 'guarded', side_effect=lambda path, *a, **kw: Path(path)),
                  patch.object(controller, 'provision_fio_scratch') as provision):
                with self.assertRaises(controller.SafetyError):
                    controller.guest_operation(config, request, None)
                provision.assert_not_called()
            request['index'] = -1
            with (patch.object(controller, 'verify_local_identity', return_value={}),
                  patch.object(controller, 'guarded', side_effect=lambda path, *a, **kw: Path(path)),
                  patch.object(controller, 'media_device_snapshot', side_effect=controller.FioFailure('fake stop')) as media,
                  patch.object(controller, 'provision_fio_scratch') as provision):
                result = controller.guest_operation(config, request, None)
                self.assertEqual(result['fio_failed'], 'fake stop')
                self.assertEqual(media.call_args.args[1], -1)
                provision.assert_not_called()
