import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import analyze
import cloud_controller as controller
import nvme_campaign_native
from test_nvme_campaign_native import config as native_config


class StageControllerTests(unittest.TestCase):
    def test_g18_native_and_remote_device_counter_sources(self):
        client = {'owner_index': -1, 'major': 259, 'minor': 3, 'read_ops': 1,
                  'read_sectors': 2, 'write_ops': 3, 'write_sectors': 4}
        owner = {**client, 'owner_index': 0}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for backend, expected in [('native', client), ('tidehunter_verbs_native', owner)]:
                job = {'backend': backend, 'device_counters': True}
                frames = [{'media_device': client}, {'media_device': owner}]
                controller.store_measurement_device_evidence(root, {}, job, None, frames, frames, 1)
                self.assertEqual(json.loads((root / 'device_counters.json').read_text())['before'], [expected])
            with self.assertRaisesRegex(controller.SafetyError, 'client device counters unavailable'):
                controller.store_measurement_device_evidence(root, {}, {'backend': 'native', 'device_counters': True},
                                                             None, [{}, {'media_device': owner}], frames, 1)

    def test_g18_live_measurement_device_qualification(self):
        counter = {'owner_index': 0, 'major': 259, 'minor': 3, 'read_ops': 11214,
                   'read_sectors': 99872, 'write_ops': 2032957, 'write_sectors': 221232720}
        def frame(row):
            return [{}, {'nvme': {'host_identity': {'boot_id': '72305c5b-2565-467c-8d6b-6919294e8aaa'}},
                         'media_device': row}]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for expected, calls, delta, status in [('cold', 2048, 1900, 'qualified'),
                                                    ('cold', 2048, 1700, 'unqualified'),
                                                    ('memory', 8192, 0, 'qualified'),
                                                    ('memory', 8192, 10, 'unqualified')]:
                job = {'case_id': 'case', 'case_kind': 'live_read', 'media': {'expected': expected}}
                first, last = frame(counter), frame({**counter, 'read_ops': counter['read_ops'] + delta})
                samples = [{'operation': 'read', 'outcome': 'success'}] * calls
                media = controller.measurement_media_evidence(job, samples, 32, first, last, 1, 1, True, None)
                self.assertEqual(media['status'], status)
                controller.store_measurement_device_evidence(root, {}, job, media, first, last, 1)
                self.assertEqual(json.loads((root / 'media_evidence.json').read_text())['status'], status)
                if status == 'unqualified':
                    with self.assertRaisesRegex(controller.SafetyError, 'required cold canary is not device-served'):
                        controller.store_measurement_device_evidence(root, {}, {**job, 'require_media_qualified': True},
                                                                     media, first, last, 1)
                    self.assertEqual(json.loads((root / 'media_evidence.json').read_text())['status'], status)
        batch_job = {'case_id': 'batch', 'case_kind': 'native_media_read', 'media': {'expected': 'cold'}}
        client = {**counter, 'owner_index': -1}
        client_frame = [{'nvme': {'host_identity': {'boot_id': 'client-boot'}}, 'media_device': client}, {}]
        client_end = copy.deepcopy(client_frame)
        client_end[0]['media_device']['read_ops'] += 8000
        result = controller.measurement_media_evidence(
            batch_job, [{'operation': 'get_many', 'outcome': 'success'}] * 256, 32,
            client_frame, client_end, 1, 1, True, None)
        self.assertEqual(result['successful_reads'], 8192)
        self.assertEqual(result['status'], 'qualified')

    def test_g18_snapshot_selects_native_or_owner_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            native = root / 'results/cases/case/database'
            native.mkdir(parents=True)
            config = {'fleet': {'data_root': str(root), 'nodes': [{}]}}
            frame = {'nvme': {'host_identity': {'boot_id': 'boot'}}, 'diskstats': 'diskstats'}
            native_job = {'case_id': 'case', 'case_kind': 'native_op', 'backend': 'native', 'device_counters': True}
            remote = {'case_id': 'case', 'case_kind': 'remote_put', 'backend': 'tidehunter_verbs_native',
                      'device_counters': True, 'owner_count': 1}
            live = {**remote, 'case_kind': 'live_read', 'device_counters': False, 'media': {'expected': 'cold'}}
            def guarded(path, root, must_exist=False):
                path = Path(path)
                if must_exist and not path.exists():
                    raise controller.SafetyError("required owned path missing")
                return path
            with patch.object(controller, 'verify_local_identity', return_value={}), \
                 patch.object(controller, 'guarded', side_effect=guarded), \
                 patch.object(controller, 'snapshot', side_effect=lambda *a, **k: copy.deepcopy(frame)), \
                 patch.object(controller, 'owned_monitor_processes', return_value={'processes': [], 'missing': []}), \
                 patch.object(controller.resource_accounting, 'capture', return_value={'processes': []}), \
                 patch.object(controller.rdma_diagnostics, 'capture', return_value={}), \
                 patch.object(controller, 'media_device_snapshot', return_value={'read_ops': 1}) as snapshot:
                result = controller.guest_operation(config, {'index': -1, 'action': 'snapshot', 'resource_job': native_job}, None)
                self.assertEqual(result['media_device'], {'read_ops': 1})
                self.assertEqual(snapshot.call_args.args[0], native)
                snapshot.reset_mock()
                controller.guest_operation(config, {'index': 0, 'action': 'snapshot', 'resource_job': native_job}, None)
                snapshot.assert_not_called()
                with self.assertRaisesRegex(controller.SafetyError, 'required owned path missing'):
                    controller.guest_operation(config, {'index': 0, 'action': 'snapshot', 'resource_job': remote}, None)
                (root / 'cells/case/node-0').mkdir(parents=True)
                result = controller.guest_operation(config, {'index': 0, 'action': 'snapshot', 'resource_job': live}, None)
                self.assertEqual(result['media_device'], {'read_ops': 1})
                self.assertEqual(snapshot.call_args.args[0], root / 'cells/case/node-0')
                batch_job = next(job for job in controller.cloud_campaign.make_focused_matrix('nvmeof_path_core')
                                 if job['backend'] == 'native' and job['case_kind'] == 'native_media_read'
                                 and job['mix']['get_many'])
                batch_path = root / 'results/cases' / batch_job['case_id'] / 'database'
                batch_path.mkdir(parents=True)
                controller.guest_operation(config, {'index': -1, 'action': 'snapshot', 'resource_job': batch_job}, None)
                self.assertEqual(snapshot.call_args.args[0], batch_path)

    def test_prepared_geometry_mismatch_fails(self):
        from test_cloud_controller import config_fixture
        runtime = controller.Controller.__new__(controller.Controller)
        runtime.config = config_fixture()
        runtime.remaining = MagicMock(return_value=60)
        job = {'case_id': 'case', 'backend': 'tidehunter_tcp', 'owner_count': 1,
               'keys': 1, 'warmup_operations': 0, 'measurement_operations': 1,
               'coordinator_cache_bytes': 0}
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            def prepare(argv, **kwargs):
                node = directory / 'prepared' / 'node-0'
                node.mkdir(parents=True)
                (node / 'launch.yaml').write_text('node: {partition: {fragment_bytes: 1, wal_file_bytes: 1}}')
            with patch.object(controller, 'command', side_effect=prepare), \
                 patch.object(controller, 'platform_module', return_value=None), \
                 patch.object(controller, 'environment', return_value={}):
                with self.assertRaisesRegex(controller.SafetyError, 'prepared owner WAL geometry differs from the planned case'):
                    runtime.prepare_tide(job, directory)

    def test_prepared_history_persistence_matches_planned_default(self):
        from test_cloud_controller import config_fixture
        for planned, launched in (('memory_only', None), ('memory_only', 'on_disk'),
                                  (None, 'memory_only'), ('on_disk', 'memory_only'),
                                  (None, None), ('on_disk', None), (None, 'on_disk'),
                                  ('memory_only', 'memory_only')):
            with self.subTest(planned=planned, launched=launched), tempfile.TemporaryDirectory() as tmp:
                runtime = controller.Controller.__new__(controller.Controller)
                runtime.config = config_fixture()
                runtime.remaining = MagicMock(return_value=60)
                job = {'case_id': 'case', 'backend': 'tidehunter_tcp', 'owner_count': 1,
                       'keys': 1, 'warmup_operations': 0, 'measurement_operations': 1,
                       'coordinator_cache_bytes': 0}
                if planned is not None:
                    job['history_persistence'] = planned
                directory = Path(tmp)
                def prepare(argv, **kwargs):
                    import yaml
                    launch = {'node': {'partition': {'fragment_bytes': 4194304,
                                                     'wal_file_bytes': 1073741824}}}
                    if launched is not None:
                        launch['history_persistence'] = launched
                    node = directory / 'prepared' / 'node-0'
                    node.mkdir(parents=True)
                    (node / 'launch.yaml').write_text(yaml.safe_dump(launch))
                with patch.object(controller, 'command', side_effect=prepare), \
                     patch.object(controller, 'platform_module', return_value=None), \
                     patch.object(controller, 'environment', return_value={}):
                    if (planned or 'on_disk') != (launched or 'on_disk'):
                        with self.assertRaisesRegex(controller.SafetyError,
                                'prepared owner history persistence differs from the planned case'):
                            runtime.prepare_tide(job, directory)
                    else:
                        # Reaching the absent remote.json proves history admission
                        # passed before any owner was started.
                        with self.assertRaises(FileNotFoundError):
                            runtime.prepare_tide(job, directory)
                        self.assertEqual(runtime.active_tide, (job, directory, []))

    def test_owner_history_diagnostics_require_all_epochs_and_owners(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            path = directory / 'owner-durability' / 'node-0.history-maintenance.json'
            evidence = {'schema_version': 1, 'epoch_stats': [[0, {'waits': 0}], [1, {'waits': 0}]]}
            controller.dump(path, evidence)
            controller.require_zero_owner_history_waits(directory, 1)
            with self.assertRaisesRegex(RuntimeError, 'owner history diagnostics missing or invalid'):
                controller.require_zero_owner_history_waits(directory, 2)
            evidence['epoch_stats'][1][1]['waits'] = 1
            controller.dump(path, evidence)
            with self.assertRaisesRegex(RuntimeError, 'memory-only owner waited for history'):
                controller.require_zero_owner_history_waits(directory, 1)
            for epochs in ([], {}, [[0, {}]], [[0, {'waits': False}]], [[0, {'waits': -1}]]):
                controller.dump(path, {'schema_version': 1, 'epoch_stats': epochs})
                with self.subTest(epochs=epochs), self.assertRaisesRegex(
                        RuntimeError, 'owner history diagnostics missing or invalid'):
                    controller.require_zero_owner_history_waits(directory, 1)

    def test_nvmeof_path_live_admission_is_selection_bound(self):
        jobs = [{'case_id': 'live', 'nvmeof_route': 'live'}, {'case_id': 'sealed', 'nvmeof_route': 'sealed_read'}]
        for selection in controller.cloud_campaign.NVMEOF_PATH_SELECTIONS:
            with self.subTest(selection=selection):
                spec = {'mode': 'focused', 'selection': selection}
                with patch.object(controller.cloud_campaign, 'make_focused_matrix', return_value=jobs) as matrix:
                    self.assertTrue(controller.admitted_live_campaign({'spec': spec}, 'live'))
                    self.assertFalse(controller.admitted_live_campaign({'spec': spec}, 'sealed'))
                    self.assertFalse(controller.admitted_live_campaign({'spec': spec}, 'unknown'))
                    matrix.assert_called_with(selection)

    def test_get_many_successful_key_count(self):
        samples = [{'operation': 'get_many', 'outcome': 'success'}] * 4
        config = {'workload': {'batch_size': 32}}
        self.assertEqual(analyze.successful_read_keys(samples, 32), 128)
        controller.cloud_campaign.validate_media_sample_count({'successful_reads': 128}, samples, config)
        with self.assertRaisesRegex(ValueError, 'media successful reads differ from raw samples'):
            controller.cloud_campaign.validate_media_sample_count({'successful_reads': 4}, samples, config)

    def test_required_canary_persists_before_failure(self):
        media = {'status': 'unqualified'}
        frames = [{}, {'media_device': {'read_ops': 1, 'read_sectors': 2, 'write_ops': 3, 'write_sectors': 4}}]
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            receipt = {}
            job = {'device_counters': True, 'require_media_qualified': True}
            with self.assertRaisesRegex(controller.SafetyError, 'required cold canary is not device-served'):
                controller.store_measurement_device_evidence(directory, receipt, job, media, frames, frames, 1)
            self.assertEqual(json.loads((directory / 'media_evidence.json').read_text()), media)
            self.assertEqual(json.loads((directory / 'device_counters.json').read_text())['before'], [frames[1]['media_device']])
            self.assertEqual(set(receipt), {'media_evidence', 'device_counters'})
            controller.store_measurement_device_evidence(directory, {}, {'device_counters': True}, media, frames, frames, 1)

    def test_v2_media_counters_and_validator(self):
        before = {'owner_index': 0, 'major': 259, 'minor': 1, 'read_ops': 10,
                  'read_sectors': 20, 'write_ops': 3, 'write_sectors': 4}
        after = {**before, 'read_ops': 1910, 'read_sectors': 15220}
        media = {'expected': 'cold'}
        result = controller.qualify_media_evidence('case', media, [before], [after], 2048, True)
        self.assertEqual(result['status'], 'qualified')
        self.assertAlmostEqual(result['read_ops_per_success'], 1900 / 2048)
        self.assertEqual(result['device_bytes_per_read'], 4096)
        job = {'case_id': 'case', 'media': media}
        receipt = {'measurement_evidence': {'gate_released': True, 'completion_gate_observed': True}}
        analyze._validate_focused_artifact('media_evidence', result, job, receipt)
        with self.assertRaisesRegex(ValueError, 'media bytes per read differs'):
            analyze._validate_focused_artifact('media_evidence', {**result, 'device_bytes_per_read': 1}, job, receipt)
        after['read_ops'] = 1710
        result = controller.qualify_media_evidence('case', media, [before], [after], 2048, True)
        self.assertIn('cold_read_ratio_below_threshold', result['reason_codes'])

    def test_memory_only_read_admission(self):
        runtime = controller.Controller.__new__(controller.Controller)
        runtime.config = {'fleet': {'data_root': '/tmp'}}
        for job in ({'backend': 'tidehunter_verbs_native'}, {'nvmeof_route': 'live'}):
            with self.assertRaisesRegex(controller.SafetyError, 'memory-only history admits reads only with permanent live export'):
                runtime.prepare_tide({**job, 'case_id': 'case', 'history_persistence': 'memory_only',
                                      'mix': {'get_many': 1}}, Path('/tmp'))

    def test_direct_hybrid_route_is_admitted_without_widening_reads(self):
        jobs = controller.cloud_campaign.make_focused_matrix('stages')
        for job in jobs:
            if job.get('case_kind') == 'hybrid_put' and job.get('target_io') == 'direct':
                controller.require_supported_nvme_route(job)
                cfg = controller.cloud_campaign.make_run_config(job, Path('/tmp/case'), {}, 'a' * 40, 600)
                self.assertTrue(cfg['phases']['preload'])
                self.assertFalse(cfg['phases']['verify_existing_dataset'])
                self.assertIsNone(cfg.get('key_namespace_hex'))
                self.assertGreaterEqual(job['read_lanes'], 1)
                invalid = {**job, 'mix': controller.cloud_campaign.mix(read=100)}
                with self.assertRaises(controller.SafetyError):
                    controller.require_supported_nvme_route(invalid)

    def test_focused_run_config_preserves_policy_and_diagnostics(self):
        jobs = controller.cloud_campaign.make_focused_matrix('stages')
        for job in jobs:
            if job['backend'] != 'native' or not (job.get('put_key_policy') or job.get('diagnostics')):
                continue
            options = {'path': '/tmp/native', 'key_count': 1048576, 'durability': job['native_durability']}
            cfg = controller.cloud_campaign.make_run_config(job, Path('/tmp/case'), options, 'a' * 40, 600)
            if 'put_key_policy' in job:
                self.assertEqual(cfg['workload']['put_key_policy'], job['put_key_policy'])
            if 'diagnostics' in job:
                self.assertEqual(cfg['diagnostics'], job['diagnostics'])

    def test_profile_and_new_policy_are_bound_to_manifest(self):
        from test_evidence import EvidenceAdmission
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, receipt, manifest, summary = EvidenceAdmission().binding(root)
            config.update(workload={'put_key_policy': 'new'}, diagnostics={'write_profile_limit': 1})
            normalized = {**config}
            (root / 'normalized-config.json').write_text(json.dumps(normalized))
            receipt['normalized_config_sha256'] = hashlib.sha256((root / 'normalized-config.json').read_bytes()).hexdigest()
            receipt['job'] = {'selection': 'stages', 'put_key_policy': 'new'}
            manifest['run']['config'] = normalized
            manifest['run']['data_policy'] = {'put_key_policy': 'new'}
            path = root / 'result/manifest.json'
            path.write_text(json.dumps(manifest))
            receipt['artifacts_sha256']['manifest.json'] = hashlib.sha256(path.read_bytes()).hexdigest()
            profile = {'schema_version': 1}
            path = root / 'result/write-diagnostics.json'
            path.write_text(json.dumps(profile))
            receipt['write_diagnostics'] = {'path': 'result/write-diagnostics.json',
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'observed': profile}
            analyze.verify_binding(root, config, receipt, manifest, summary)
            manifest['run']['data_policy']['put_key_policy'] = 'overwrite'
            with self.assertRaises(ValueError):
                analyze.verify_binding(root, config, receipt, manifest, summary)
            manifest['run']['data_policy']['put_key_policy'] = 'new'
            path.write_text('{}')
            with self.assertRaises(ValueError):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_native_dataset_reopens_with_planned_durability(self):
        for durability in ('page_cache', 'value_wal_barrier', 'value_wal_group_barrier'):
            with self.subTest(durability=durability):
                config = native_config()
                config['backend']['options']['durability'] = durability
                prepared = copy.deepcopy(config)
                prepared['backend']['options']['durability'] = 'page_cache'
                receipt = {'status': 'completed', 'identity': nvme_campaign_native._identity(config),
                           'database_path': '/shared/database', 'config': prepared}
                result = controller.bind_native_dataset_config(receipt, config, {'native_durability': durability})
                self.assertEqual(result['backend']['options']['durability'], durability)
                self.assertEqual(result['backend']['options']['path'], '/shared/database')
                self.assertFalse(result['phases']['preload'])
                self.assertTrue(result['phases']['verify_existing_dataset'])
                self.assertEqual(prepared['backend']['options']['durability'], 'page_cache')
                receipt['identity']['seed'] += 1
                with self.assertRaises(ValueError):
                    controller.bind_native_dataset_config(receipt, config, {'native_durability': durability})

    def test_group_barrier_job_binds_native_harness_options(self):
        job = next(j for j in controller.cloud_campaign.make_focused_matrix('fair_native')
                   if j['backend'] == 'native' and j['native_durability'] == 'value_wal_group_barrier')
        self.assertEqual(job['durability_tier'], 'value_wal_group_barrier')
        options = {'path': '/tmp/native', 'key_count': 1048576,
                   'durability': 'value_wal_group_barrier'}
        config = controller.cloud_campaign.make_run_config(job, Path('/tmp/case'), options, 'a' * 40, 600)
        self.assertEqual(config['backend']['options']['durability'], 'value_wal_group_barrier')
        with self.assertRaisesRegex(ValueError, 'durability differs'):
            controller.cloud_campaign.make_run_config(
                job, Path('/tmp/case'), {**options, 'durability': 'value_wal_barrier'}, 'a' * 40, 600)

    def test_group_barrier_metrics_are_bound_as_raw_evidence(self):
        from test_evidence import EvidenceAdmission
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, receipt, manifest, summary = EvidenceAdmission().binding(root)
            path = root / 'result/metrics.prom'
            path.write_text(
                'kv_research_native_group_barrier_runs_total 2\n'
                'kv_research_native_group_barrier_callers_total 3\n'
                'kv_research_native_group_barrier_errors_total 0\n'
                'kv_research_native_group_barrier_group_size_count 2\n'
                'kv_research_native_group_barrier_group_size_sum 3\n')
            receipt['artifacts_sha256']['metrics.prom'] = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertIn('metrics.prom', controller.RAW_FILES)
            analyze.verify_binding(root, config, receipt, manifest, summary)
            path.write_text(path.read_text() + 'kv_research_native_group_barrier_runs_total 9\n')
            with self.assertRaises(ValueError):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_native_snapshot_uses_client_dataset_directory(self):
        job = next(j for j in controller.cloud_campaign.make_focused_matrix('stages')
                   if j['case_kind'] == 'native_media_read')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = {'fleet': {'data_root': str(root), 'nodes': [{} for _ in range(5)]}}
            request = {'index': -1, 'action': 'snapshot', 'resource_job': job}
            frame = {'nvme': {'host_identity': {'boot_id': 'boot'}}, 'diskstats': 'diskstats'}
            with patch.object(controller, 'verify_local_identity', return_value={}), \
                 patch.object(controller, 'guarded', side_effect=lambda path, *a, **k: Path(path)), \
                 patch.object(controller, 'snapshot', return_value=frame), \
                 patch.object(controller, 'owned_monitor_processes', return_value={'processes': [], 'missing': []}), \
                 patch.object(controller.resource_accounting, 'capture', return_value={'processes': []}), \
                 patch.object(controller.rdma_diagnostics, 'capture', return_value={}), \
                 patch.object(controller, 'media_device_snapshot', return_value={'owner_index': -1}) as media:
                result = controller.guest_operation(config, request, None)
            self.assertEqual(result['media_device'], {'owner_index': -1})
            self.assertEqual(media.call_args.args[0],
                             root / 'results' / 'datasets' / (job['sealed_dataset']['source_id'] + '-native') / 'database')
            self.assertEqual(media.call_args.args[1], -1)

    def test_native_media_ratio_and_owner_schema(self):
        media = {'expected': 'cold', 'history': 'normal'}
        first = [{'owner_index': -1, 'major': 259, 'minor': 4, 'read_ops': 10,
                  'read_sectors': 0, 'write_ops': 0, 'write_sectors': 0}]
        end = [{**first[0], 'read_ops': 19}]
        result = controller.qualify_media_evidence('case', media, first, end, 10, True, client=True)
        self.assertEqual(result['status'], 'qualified')
        self.assertEqual(result['read_ops_per_success'], .9)
        self.assertEqual(result['client_devices'][0]['client_index'], -1)
        self.assertNotIn('owner_devices', result)
        self.assertEqual(controller.qualify_media_evidence('case', media, first, end, 11, True, client=True)['status'], 'unqualified')
        self.assertEqual(controller.qualify_media_evidence('case', media, first, end, 10, True)['status'], 'unqualified')
        self.assertEqual(controller.qualify_media_evidence('case', media, first + first, end + end, 10, True, client=True)['status'], 'unqualified')

    def test_client_media_snapshot_binding_checks_boot_and_device(self):
        media = {'expected': 'cold', 'history': 'normal'}
        job = {'case_kind': 'native_media_read', 'case_id': 'case', 'media': media}
        result = controller.qualify_media_evidence('case', media,
            [{'owner_index': -1, 'major': 259, 'minor': 4, 'read_ops': 10, 'read_sectors': 0, 'write_ops': 0, 'write_sectors': 0}],
            [{'owner_index': -1, 'major': 259, 'minor': 4, 'read_ops': 19, 'read_sectors': 0, 'write_ops': 0, 'write_sectors': 0}], 10, True, client=True)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gate = {'gate_released': True, 'completion_gate_observed': True}
            for field, reads in (('before', 10), ('end', 19)):
                host = {'nvme': {'host_identity': {'boot_id': 'boot'}},
                        'diskstats': f'259 4 nvme0n1 {reads} 0 0 0 0 0 0',
                        'media_device': {'owner_index': -1, 'major': 259, 'minor': 4, 'read_ops': reads,
                                         'read_sectors': 0, 'write_ops': 0, 'write_sectors': 0}}
                path = root / (field + '.json')
                path.write_text(json.dumps({'hosts': [host]}))
                gate[field], gate[field + '_sha256'] = path.name, hashlib.sha256(path.read_bytes()).hexdigest()
            receipt = {'measurement_evidence': gate}
            analyze._validate_focused_artifact('media_evidence', result, job, receipt)
            analyze._verify_media_snapshots(root, receipt, result)
            path = root / 'end.json'
            data = json.loads(path.read_text())
            data['hosts'][0]['nvme']['host_identity']['boot_id'] = 'other'
            path.write_text(json.dumps(data))
            gate['end_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaises(ValueError):
                analyze._verify_media_snapshots(root, receipt, result)

    def test_hybrid_put_uses_boundary_readers_without_read_io(self):
        job = {'measurement_operations': 2, 'nvmeof_target': 'kernel'}
        backend = {'configuration': {'write_backend': 'owner_record_protocol'}}
        identity = {'database_uuid': [1] * 16, 'epoch': 1, 'partition': 0, 'export_uuid': [2] * 16}
        reader = {'binding': {'transport': 'rdma'}, 'manifest': identity}
        observed = {'nvmeof': {'available': True, 'readers': {'id': {'available': True,
                    'identity': {**identity, 'transport': 'rdma'},
                    'counters': {'available': True, 'counters': {'logical_requests': 0, 'io_submissions': 0}}}}}}
        policy = {'target_implementation': 'kernel_direct', 'target_cache': 'direct_file_no_OS_pagecache',
                  'target_cache_verified': True, 'selected_transport': 'rdma'}
        targets = [{'target': {'exports': [{'export_uuid': identity['export_uuid'], 'target_receipt': policy}]}}]
        samples = [{'operation': 'put', 'outcome': 'success'} for _ in range(2)]
        qualify = lambda b=backend, e=observed, t=targets, r=[reader], s=samples, d=True, c={'phases': {'preload': True}}: analyze.qualify_hybrid_put(job, b, e, t, r, s, d, c)
        self.assertTrue(qualify()['qualified'])
        self.assertEqual(qualify()['namespace_origin'], 'runner_fresh')
        self.assertFalse(qualify(c={'phases': {'preload': False}})['qualified'])
        self.assertFalse(qualify(c={'phases': {'preload': True}, 'key_namespace_hex': '00' * 16})['qualified'])
        self.assertFalse(qualify(b={'configuration': {'write_backend': 'other'}})['qualified'])
        self.assertFalse(qualify(e={'nvmeof': {'available': False}})['qualified'])
        self.assertFalse(qualify(d=False)['qualified'])
        for field in ('logical_requests', 'io_submissions'):
            changed = copy.deepcopy(observed)
            changed['nvmeof']['readers']['id']['counters']['counters'][field] = 1
            self.assertIn('reader_' + field + '_nonzero', qualify(e=changed)['reasons'])
        self.assertFalse(qualify(s=samples[:1])['qualified'])
        altered = copy.deepcopy(targets)
        altered[0]['target']['exports'][0]['export_uuid'] = [3] * 16
        self.assertFalse(qualify(t=altered)['qualified'])
        spdk_job = {**job, 'nvmeof_target': 'spdk'}
        spdk_targets = copy.deepcopy(targets)
        spdk_targets[0]['target']['exports'][0]['target_receipt'] = {
            'target_implementation': 'spdk', 'target_cache': 'direct_file_bdev_no_OS_pagecache',
            'target_cache_verified': True, 'selected_transport': 'rdma',
            'direct_io_verified': True, 'read_only_verified': True}
        cfg = {'phases': {'preload': True}}
        target_io = {'available': True, 'read_ops': 0, 'read_bytes': 0}
        def spdk(io):
            return analyze.qualify_hybrid_put(spdk_job, backend, observed, spdk_targets, [reader],
                                              samples, True, cfg, io)
        self.assertTrue(spdk(target_io)['qualified'])
        self.assertIn('target_reads_observed', spdk({**target_io, 'read_ops': 1})['reasons'])
        self.assertIn('target_reads_observed', spdk({**target_io, 'read_bytes': 4096})['reasons'])
        self.assertIn('target_read_counters_unavailable', spdk(None)['reasons'])

    def test_completed_hybrid_unqualified_keeps_completed_receipt(self):
        from contextlib import ExitStack
        job = next(j for j in controller.cloud_campaign.make_focused_matrix('stages')
                   if j['case_kind'] == 'hybrid_put' and j['nvmeof_target'] == 'kernel')
        with tempfile.TemporaryDirectory() as tmp:
            runtime = controller.Controller.__new__(controller.Controller)
            runtime.root = Path(tmp)
            directory = runtime.root / 'cases' / job['case_id']
            runtime.config = {'prefix': 'test', 'binary': '/unused', 'binary_blake2b512': 'a' * 128,
                'source_revision': 'b' * 40, 'fleet': {'client_cpus': 8, 'client_memory_mib': 1024, 'nodes': [{} for _ in range(5)]},
                'spec': {'case_deadline_seconds': 600}}
            runtime.local_mode = MagicMock()
            runtime.local_mode.snapshot_client.return_value = {}
            runtime.local_mode.client_logs.return_value = ''
            runtime.monitoring_complete = True
            runtime.monitoring_attempted = False
            runtime.monitoring_events = []
            runtime.active_backend = runtime.active_tide = runtime.active_nvme = None
            runtime.tide_cleanup_forbidden = False
            runtime.shared_tide = {'job': job, 'block': runtime.root, 'identity': ['source', 'verbs'],
                'prepared': {'source_epoch': 1, 'preload_recipe': 'recipe',
                             'preload_batch_max_members': 1, 'database': 'db'}}
            runtime.select_monitoring = MagicMock()
            runtime.finish_monitoring = MagicMock()
            runtime.guard = MagicMock()
            runtime.remaining = MagicMock(return_value=600)
            runtime.clock = MagicMock(return_value=1)
            runtime.observe = lambda operation, action: action()
            runtime.observe_resource_interval = MagicMock(return_value={})
            runtime.prepared_tide_options = MagicMock(return_value={})
            runtime.all_hosts = MagicMock(return_value=[{'resource_accounting': {}} for _ in range(6)])
            def connect(job, directory):
                controller.dump(directory / 'nvme-targets.json', [])
                runtime.active_nvme = {'targets': [], 'transport': 'rdma'}
                return []
            runtime.connect_nvme = MagicMock(side_effect=connect)
            runtime.disconnect_nvme = MagicMock(side_effect=lambda **kwargs: setattr(runtime, 'active_nvme', None))
            def start(client, executable, budget, seconds):
                result = directory / 'result'
                result.mkdir()
                manifest = {'schema_version': 1, 'status': 'completed', 'backend_info': {}, 'raw_samples_complete': True}
                controller.dump(result / 'manifest.json', manifest)
                controller.dump(result / 'summary.json', {'status': 'completed', 'backend': {}})
                (result / 'samples.csv').write_text('samples')
                (result / 'metrics.prom').write_text('')
                evidence = directory / 'read-evidence'
                evidence.mkdir()
                (evidence / 'read-evidence.json').write_text('{}')
                (directory / 'measurement.ready').touch()
                (directory / 'measurement.complete').touch()
            runtime.local_mode.start_client.side_effect = start
            statuses = [{'SubState': 'running', 'ActiveState': 'active'},
                        {'SubState': 'exited', 'ActiveState': 'inactive', 'Result': 'success', 'ExecMainStatus': '0'},
                        {'LoadState': 'loaded', 'ActiveState': 'inactive', 'MainPID': '0'}]
            counts = {'preload': job['keys'], 'warmup': job['warmup_operations'], 'aging': 0,
                      'measurement': job['measurement_operations']}
            with ExitStack() as stack:
                def mock(obj, name, **kwargs):
                    return stack.enter_context(patch.object(obj, name, **kwargs))
                mock(controller, 'unit_status', side_effect=statuses)
                mock(controller, 'environment', return_value={})
                mock(controller.campaign, 'admit_config',
                     side_effect=lambda binary, path, *a, **k: json.loads(Path(path).read_text()))
                mock(controller.campaign, 'verify_run_identity')
                mock(controller.campaign, 'verify_timing_result', return_value=counts)
                mock(controller.analyze, 'verify_normalization')
                mock(controller.analyze, 'read_samples', return_value=iter([{'operation': 'put', 'outcome': 'success'}]))
                mock(controller.analyze, 'qualify_hybrid_put', return_value={'qualified': False, 'reasons': ['reader_io_submissions_nonzero']})
                mock(controller, 'verify_counter_evidence')
                mock(controller.resource_accounting, 'interval', return_value={})
                mock(controller, 'host_cpu_interval', return_value={})
                for name in ('admit_wal_eviction_evidence', 'admit_write_diagnostics', 'admit_read_diagnostics',
                             'admit_valkey_diagnostics', 'admit_write_timeline', 'admit_measurement_read_evidence',
                             'admit_spdk_target_measurement'):
                    mock(controller.cloud_campaign, name, return_value=None)
                receipt = runtime.run_case(job)
            self.assertEqual(receipt['status'], 'completed')
            self.assertEqual(receipt['hybrid_put_qualification']['reasons'], ['reader_io_submissions_nonzero'])
            self.assertFalse(receipt['hybrid_put_qualification']['qualified'])
            runtime.disconnect_nvme.assert_called_once_with(drained=True)

    def test_hybrid_unproved_drain_remains_quarantined(self):
        job = {'case_kind': 'hybrid_put', 'case_id': 'hybrid-case'}
        with self.assertRaisesRegex(controller.InfrastructureError, 'hybrid-case.*put run failed.*drain could not be proven'):
            controller.require_hybrid_reader_drain(job, True, False, True)
        controller.require_hybrid_reader_drain(job, True, True, True)
        controller.require_hybrid_reader_drain(job, False, False, False)

    def test_timeline_uses_sealed_runtime_identity(self):
        from test_evidence import EvidenceAdmission
        for target in ('kernel', 'spdk'):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                config, receipt, manifest, summary = EvidenceAdmission().binding(root)
                receipt['job'] = {'backend': 'tidehunter_nvme_rdma', 'nvmeof_route': 'sealed_read',
                                  'nvmeof_target': target}
                with patch.object(analyze, 'admit_write_timeline', return_value=None) as timeline:
                    analyze.verify_binding(root, config, receipt, manifest, summary)
                self.assertEqual(timeline.call_args.kwargs['expected_backend'], 'tidehunter_nvme_rdma_sealed_read')
                self.assertEqual(controller.cloud_campaign.diagnostic_backend(receipt['job']),
                                 'tidehunter_nvme_rdma_sealed_read')

    def test_native_timeout_is_recoverable_after_confirmed_stop(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            proc = MagicMock(pid=1234)
            proc.wait.side_effect = [subprocess.TimeoutExpired('native', 1), -9]
            with patch.object(nvme_campaign_native.subprocess, 'Popen', return_value=proc), \
                 patch.object(nvme_campaign_native.os, 'killpg'), \
                 patch('floors.process_group_alive', return_value=False):
                with self.assertRaises(nvme_campaign_native.PreparationTimeout) as raised:
                    nvme_campaign_native._run(['native'], Path(tmp) / 'run.log',
                                              nvme_campaign_native.time.monotonic() + 30)
            self.assertNotIsInstance(raised.exception, OSError)
            self.assertEqual(proc.wait.call_count, 2)
            proc.wait.side_effect = [subprocess.TimeoutExpired('native', 1), subprocess.TimeoutExpired('native', 10)]
            with patch.object(nvme_campaign_native.subprocess, 'Popen', return_value=proc), \
                 patch.object(nvme_campaign_native.os, 'killpg'):
                with self.assertRaises(nvme_campaign_native.PreparationCleanupUnconfirmed):
                    nvme_campaign_native._run(['native'], Path(tmp) / 'run.log',
                                              nvme_campaign_native.time.monotonic() + 30)

    def test_native_dataset_cleanup_retains_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / 'dataset'
            dataset.mkdir()
            (dataset / 'database').mkdir()
            (dataset / 'receipt.json').write_text('{\"status\":\"completed\"}')
            case = root / 'case'
            case.mkdir()
            binding = controller.cleanup_native_media_dataset(dataset, case)
            self.assertFalse(dataset.exists())
            self.assertEqual(binding['sha256'], hashlib.sha256((case / binding['path']).read_bytes()).hexdigest())

    def test_native_case_timeout_removes_partial_dataset(self):
        job = next(j for j in controller.cloud_campaign.make_focused_matrix('stages')
                   if j['case_kind'] == 'native_media_read' and j['value_size'] == 1024)
        for cleanup_error in (False, True):
            with tempfile.TemporaryDirectory() as tmp:
                runtime = controller.Controller.__new__(controller.Controller)
                runtime.root = Path(tmp)
                runtime.local_mode = None
                runtime.config = {'prefix': 'test', 'binary': '/unused', 'binary_blake2b512': 'a' * 128,
                    'source_revision': 'b' * 40, 'fleet': {'client_cpus': 8, 'client_memory_mib': 1024, 'nodes': [{} for _ in range(5)]},
                    'spec': {'case_deadline_seconds': 600}}
                runtime.active_nvme = runtime.active_tide = runtime.active_backend = runtime.shared_tide = None
                runtime.shared_native = {}
                runtime.tide_cleanup_forbidden = False
                runtime.monitoring_complete = True
                runtime.monitoring_attempted = False
                runtime.monitoring_events = []
                runtime.guard = MagicMock()
                runtime.select_monitoring = MagicMock()
                runtime.finish_monitoring = MagicMock()
                runtime.remaining = MagicMock(return_value=600)
                runtime.observe = lambda operation, action: action()
                def failed_prepare(binary, config, path, timeout, environment):
                    (path / 'database').mkdir()
                    (path / 'receipt.json').write_text('{\"status\":\"failed\"}')
                    raise nvme_campaign_native.PreparationTimeout('preload exceeded cap')
                with patch.object(controller, 'environment', return_value={}), \
                     patch.object(nvme_campaign_native, 'prepare_native', side_effect=failed_prepare):
                    if cleanup_error:
                        with patch.object(controller, 'cleanup_native_media_dataset', side_effect=OSError('unlink failed')):
                            with self.assertRaises(controller.InfrastructureError):
                                runtime.run_case(job)
                    else:
                        receipt = runtime.run_case(job)
                        self.assertEqual(receipt['status'], 'failed')
                        self.assertEqual(receipt['native_dataset_cleanup'], 'completed')
                        self.assertIn('native_preparation_receipt', receipt)
                        self.assertFalse((runtime.root / 'datasets' / (job['sealed_dataset']['source_id'] + '-native')).exists())

    def test_native_preparer_discards_new_policy_for_read_phase(self):
        cfg = native_config()
        cfg['workload']['put_key_policy'] = 'new'
        original = copy.deepcopy(cfg)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'binary'
            binary.write_bytes(b'binary')
            output = root / 'prepared'
            with patch.object(nvme_campaign_native, '_run', side_effect=RuntimeError('stop after config')):
                with self.assertRaises(RuntimeError):
                    nvme_campaign_native.prepare_native(binary, cfg, output, 30)
            prepared = json.loads((output / 'config.json').read_text())
            self.assertNotIn('put_key_policy', prepared['workload'])
            self.assertEqual(prepared['workload']['mix']['read'], 100)
            self.assertEqual(cfg, original)


if __name__ == '__main__':
    unittest.main()
