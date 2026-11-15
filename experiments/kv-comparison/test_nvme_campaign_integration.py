"""Pure controller and matrix boundary tests; no NVMe hardware is exercised."""
import copy
import hashlib
import json
import itertools
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import cloud_campaign
import cloud_controller as controller


HERE = Path(__file__).resolve().parent


class CampaignModeTests(unittest.TestCase):
    def test_owner_history_receipt_defaults_and_freshness(self):
        launch = {'ready_file': '/tmp/ready', 'launch_nonce': 'nonce', 'listen': '127.0.0.1:1',
                  'node': {'partition': {'partition': 0, 'inventory': {'database': [1] * 16}}}}
        receipt = {'partition': 0, 'database': [1] * 16, 'address': '127.0.0.1:1',
                   'launch_nonce': 'nonce', 'pid': 1, 'boot_id': 'boot',
                   'started_unix_ns': 1, 'durability': {'tier': 'native'}}
        cloud_campaign.validate_owner_receipt(receipt, launch)
        explicit = dict(receipt, history_persistence='on_disk')
        cloud_campaign.validate_owner_receipt(explicit, launch)
        cloud_campaign.validate_owner_receipt(receipt, dict(launch, history_persistence='on_disk'))
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(
            receipt, dict(explicit, pid=2, started_unix_ns=2)))
        replay = dict(receipt, history_persistence='wal_replay')
        cloud_campaign.validate_owner_receipt(replay, dict(launch, history_persistence='wal_replay'))
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(
            replay, dict(replay, pid=2, started_unix_ns=2)))
        with self.assertRaises(ValueError):
            cloud_campaign.validate_owner_receipt(receipt, dict(launch, history_persistence='wal_replay'))
        memory_launch = dict(launch, history_persistence='memory_only')
        memory_receipt = dict(receipt, history_persistence='memory_only')
        cloud_campaign.validate_owner_receipt(memory_receipt, memory_launch)
        for actual, planned in ((receipt, memory_launch), (memory_receipt, launch)):
            with self.assertRaises(ValueError):
                cloud_campaign.validate_owner_receipt(actual, planned)
        newer = dict(memory_receipt, pid=2, started_unix_ns=2)
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(memory_receipt, newer))
        self.assertFalse(cloud_campaign.owner_receipt_is_fresh(receipt, newer))

    def test_permanent_export_measurement_and_configuration(self):
        for selection in cloud_campaign.NVMEOF_PATH_SELECTIONS:
            with self.subTest(selection=selection):
                self._permanent_export_measurement_and_configuration(selection)

    def _permanent_export_measurement_and_configuration(self, selection):
        job = {'selection': selection, 'group': 'G18R', 'backend': 'tidehunter_nvme_rdma',
               'nvmeof_route': 'live', 'mix': {'read': 100}, 'concurrency': 1,
               'read_lanes': 1, 'live_inline_reads': False, 'live_write_mode': 'leased',
               'durability_tier': 'native', 'live_export_mode': 'per_owner',
               'live_io_queues': 1, 'live_io_queue_size': 32, 'live_poll_queues': 0}
        names = (*cloud_campaign.LIVE_NVME_COUNTERS, 'reserve_exchanges',
                 'complete_exchanges', 'leased_frames', 'leased_skip_markers', 'fua_writes',
                 'grant_misses', 'controller_connect_ns', 'controller_connect_samples')
        start = dict.fromkeys(names, 0)
        end = dict(start, completed_read_ops=1, completed_read_bytes=1024,
                   namespace_connects=1, metadata_requests=2)
        evidence = {'measurement_attempted': True, 'measurement_drained': True,
                    'start': {'live_nvmeof': start, 'coordinator': {'read_profile_live_inline': 0}},
                    'end': {'live_nvmeof': end, 'coordinator': {'read_profile_live_inline': 0}},
                    'deltas': {'coordinator': {'available': True,
                               'counters': {'read_profile_live_inline': 0}}},
                    'live_nvmeof': {'available': True, 'counters': dict(end)}}
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])
        for name in ('grant_misses', 'controller_connect_ns', 'controller_connect_samples'):
            bad = copy.deepcopy(evidence)
            del bad['live_nvmeof']['counters'][name]
            self.assertIn(f'invalid live NVMe counter {name}',
                          cloud_campaign.qualify_live_nvme_measurement(bad, job)['reasons'])
        for name in ('lease_reserve_ns', 'lease_namespace_sync_ns', 'lease_publish_ns',
                     'lease_profile_samples', 'adopt_owner_ns', 'adopt_close_scan_ns',
                     'adopt_close_scan_bytes', 'adopt_profile_samples',
                     'extent_preleases', 'extent_switch_waits'):
            current = copy.deepcopy(evidence)
            current['start']['live_nvmeof'][name] = 0
            current['end']['live_nvmeof'][name] = 2
            current['live_nvmeof']['counters'][name] = 2
            self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(current, job)['qualified'])
            current['live_nvmeof']['counters'][name] = 1
            self.assertIn(f'invalid live NVMe counter {name}',
                          cloud_campaign.qualify_live_nvme_measurement(current, job)['reasons'])
        for field, value, reason in (
                ('namespace_connects', 2, 'permanent export made more than one controller connect'),
                ('metadata_requests', 3, 'permanent export sent owner requests for reads')):
            bad = copy.deepcopy(evidence)
            bad['end']['live_nvmeof'][field] = value
            bad['live_nvmeof']['counters'][field] = value
            self.assertIn(reason, cloud_campaign.qualify_live_nvme_measurement(bad, job)['reasons'])
        info = {'configuration': {'live_export_mode': 'per_owner',
                'live_queue_geometry': {'io_queues': 1, 'queue_size': 32, 'poll_queues': 0},
                'live_write': {'write_mode': 'leased',
                               'acknowledgement_contract': 'leased extent: page-cache write'}}}
        if job['selection'] in ('nvmeof_parallel_writes', 'nvmeof_reads_geometry', 'nvmeof_reads_geometry_rest'):
            window = job.get('live_leased_write_window')
            info['configuration'].update(live_leased_write_window=window,
                live_leased_write_window_bound=31 if window is None else window)
        cloud_campaign.admit_live_write_contract(job, info)
        for field in ('live_export_mode', 'live_queue_geometry'):
            bad = copy.deepcopy(info)
            bad['configuration'][field] = None
            with self.assertRaisesRegex(ValueError, 'live export mode or queue geometry differs'):
                cloud_campaign.admit_live_write_contract(job, bad)
        for field, value in (('live_lease_ahead', False), ('live_polled_writes', True)):
            bad = copy.deepcopy(info)
            bad['configuration'][field] = value
            with self.assertRaisesRegex(ValueError, 'live export mode or queue geometry differs'):
                cloud_campaign.admit_live_write_contract(job, bad)
        for field, default in (('live_lease_ahead', True), ('live_polled_writes', False)):
            explicit_job = dict(job, **{field: default})
            with self.assertRaisesRegex(ValueError, 'live export mode or queue geometry differs'):
                cloud_campaign.admit_live_write_contract(explicit_job, info)
            explicit_info = copy.deepcopy(info)
            explicit_info['configuration'][field] = default
            cloud_campaign.admit_live_write_contract(explicit_job, explicit_info)

    def test_parallel_c64_run_configuration_admits_all_callers(self):
        jobs = cloud_campaign.make_focused_matrix('nvmeof_parallel_writes')
        cases = [j for j in jobs if j['concurrency'] == 64]
        self.assertEqual(len(cases), 6)
        for job in cases:
            config = cloud_campaign.make_run_config(job, '/tmp/parallel-c64', {}, 'revision', 600)
            self.assertEqual(config['execution']['concurrency'], 64)
            self.assertEqual(config['execution']['mode'], 'closed_loop')
            self.assertGreaterEqual(config['execution']['queue_capacity'], 64)
            self.assertEqual(config['execution']['request_timeout_ms'], 10000)
            self.assertEqual(config['limits']['deadline_seconds'], 600)
            self.assertEqual(config['measurement_gate']['timeout_seconds'], 240)

    def test_parallel_write_window_and_resource_bound_admission(self):
        jobs = cloud_campaign.make_focused_matrix('nvmeof_parallel_writes')
        for window in (None, 1, 4):
            job = next(j for j in jobs if j.get('live_leased_write_window') == window
                       and j.get('live_write_mode') == 'leased' and j['durability_tier'] == 'native')
            configuration = dict(live_export_mode=job['live_export_mode'],
                live_queue_geometry=dict(io_queues=1, queue_size=32, poll_queues=0),
                live_lease_ahead=True, live_polled_writes=False,
                live_leased_write_window=window, live_leased_write_window_bound=31 if window is None else window,
                live_write=dict(write_mode='leased', acknowledgement_contract='leased extent: page-cache write'))
            cloud_campaign.admit_live_write_contract(job, dict(configuration=configuration))
            for field, value in (('live_leased_write_window', 2), ('live_leased_write_window', False),
                                 ('live_leased_write_window_bound', None), ('live_leased_write_window_bound', True),
                                 ('live_leased_write_window_bound', 0)):
                bad = dict(configuration, **{field: value})
                with self.subTest(window=window, field=field, value=value), self.assertRaises(ValueError):
                    cloud_campaign.admit_live_write_contract(job, dict(configuration=bad))
            for field in ('live_leased_write_window', 'live_leased_write_window_bound'):
                bad = dict(configuration)
                del bad[field]
                with self.assertRaises(ValueError):
                    cloud_campaign.admit_live_write_contract(job, dict(configuration=bad))

    def test_prepared_live_client_exact_contract(self):
        host_nqn = 'nqn.2014-08.org.nvmexpress:uuid:01234567-89ab-4def-8123-456789abcdef'
        for mode in ('reserved', 'leased'):
            for tier in ('native', 'synchronous'):
                for inline in (False, True):
                    prep = {'max_inflight_requests': 32, 'max_inflight_bytes': 16777216,
                            'connect_timeout_ms': 30000, 'io_timeout_ms': 30000,
                            'write_mode': mode, 'inline_reads': inline, 'export_mode': 'on_demand',
                            'lease_ahead': True, 'polled_writes': False,
                            'queue_geometry': {'io_queues': 1, 'queue_size': 32, 'poll_queues': 0}}
                    client = {**prep, 'host_nqn': host_nqn, 'host_id': controller.live_host_identity(host_nqn),
                              'leased_durability': tier if mode == 'leased' else None}
                    self.assertEqual(controller.admit_prepared_live_client(client, prep, tier), host_nqn)
                    for field in client:
                        changed = dict(client)
                        changed[field] = None
                        if changed[field] == client[field]:
                            changed[field] = 'changed'
                        with self.assertRaises(controller.SafetyError):
                            controller.admit_prepared_live_client(changed, prep, tier)

    def test_live_compact_admission_is_exact(self):
        jobs = cloud_campaign.make_focused_matrix('leased_ab_compact')
        live = next(j for j in jobs if j.get('nvmeof_route') == 'live')
        owner = next(j for j in jobs if j['backend'] == 'tidehunter_verbs_native')
        config = {'spec': {'mode': 'focused', 'selection': 'leased_ab_compact',
                           'backends': ['tidehunter_nvme_rdma']},
                  'fleet': {'nodes': [{'address': '10.0.0.2'}]}}
        with patch.object(controller, 'platform_module', return_value=None):
            prep = controller.live_nvme_preparation(config, live['case_id'], 'rdma')
            self.assertEqual(prep['addresses'], ['10.0.0.2'])
            with self.assertRaises(controller.SafetyError):
                controller.live_nvme_preparation(config, owner['case_id'], 'rdma')
            config['spec']['selection'] = 'fabric_ab_compact'
            with self.assertRaises(controller.SafetyError):
                controller.live_nvme_preparation(config, live['case_id'], 'rdma')

    def test_leased_measurement_evidence_rejects_counter_changes(self):
        job = next(j for j in cloud_campaign.make_focused_matrix('leased_ab_compact')
                   if j.get('live_write_mode') == 'leased' and j['group'] == 'G17W'
                   and j['durability_tier'] == 'synchronous')
        names = (*cloud_campaign.LIVE_NVME_COUNTERS, 'reserve_exchanges',
                 'complete_exchanges', 'leased_frames', 'leased_skip_markers', 'fua_writes')
        start = {name: 0 for name in names}
        end = dict(start, completed_write_ops=2, completed_write_bytes=2048, leased_frames=2, fua_writes=2)
        evidence = {'measurement_attempted': True, 'measurement_drained': True,
                    'start': {'live_nvmeof': start}, 'end': {'live_nvmeof': end},
                    'live_nvmeof': {'available': True, 'counters': dict(end)}}
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])
        skipped = copy.deepcopy(evidence)
        skipped['end']['live_nvmeof'].update(leased_skip_markers=1, fua_writes=3)
        skipped['live_nvmeof']['counters'].update(leased_skip_markers=1, fua_writes=3)
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(skipped, job)['qualified'])
        skipped['end']['live_nvmeof']['fua_writes'] = 2
        skipped['live_nvmeof']['counters']['fua_writes'] = 2
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(skipped, job)['qualified'])
        for name, value in [('reserve_exchanges', 1), ('complete_exchanges', 1),
                            ('leased_frames', 0), ('fua_writes', 1)]:
            changed = copy.deepcopy(evidence)
            changed['end']['live_nvmeof'][name] = value
            changed['live_nvmeof']['counters'][name] = value
            self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(changed, job)['qualified'], name)
        missing = copy.deepcopy(evidence)
        del missing['live_nvmeof']['counters']['leased_skip_markers']
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(missing, job)['qualified'])
        native = dict(job, durability_tier='native')
        native_evidence = copy.deepcopy(evidence)
        native_evidence['end']['live_nvmeof']['fua_writes'] = 0
        native_evidence['live_nvmeof']['counters']['fua_writes'] = 0
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(native_evidence, native)['qualified'])
        native_evidence['end']['live_nvmeof']['fua_writes'] = 1
        native_evidence['live_nvmeof']['counters']['fua_writes'] = 1
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(native_evidence, native)['qualified'])

    def test_live_contract_and_inline_admission(self):
        jobs = cloud_campaign.make_focused_matrix('leased_ab_compact')
        for inline in (False, True):
            job = next(j for j in jobs if j['group'] == 'G17R'
                       and j.get('live_inline_reads') is inline and j['concurrency'] == 1
                       and j['mix']['read'] == 100)
            info = {'configuration': {'live_write': {'write_mode': 'leased',
                'acknowledgement_contract': 'leased extent: FUA data sync; owner frontier root asynchronous'}}}
            cloud_campaign.admit_live_write_contract(job, info)
            for field in ('write_mode', 'acknowledgement_contract'):
                bad = copy.deepcopy(info)
                bad['configuration']['live_write'][field] = 'wrong'
                with self.assertRaises(ValueError):
                    cloud_campaign.admit_live_write_contract(job, bad)
            names = (*cloud_campaign.LIVE_NVME_COUNTERS, 'reserve_exchanges',
                     'complete_exchanges', 'leased_frames', 'leased_skip_markers', 'fua_writes')
            start = {name: 0 for name in names}
            end = dict(start, completed_read_ops=1, completed_read_bytes=1024)
            n = int(inline)
            evidence = {'measurement_attempted': True, 'measurement_drained': True,
                        'start': {'live_nvmeof': start, 'coordinator': {'read_profile_live_inline': 0}},
                        'end': {'live_nvmeof': end, 'coordinator': {'read_profile_live_inline': n}},
                        'deltas': {'coordinator': {'available': True, 'counters': {'read_profile_live_inline': n}}},
                        'live_nvmeof': {'available': True, 'counters': dict(end)}}
            self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])
            evidence['end']['coordinator']['read_profile_live_inline'] = 1 - n
            evidence['deltas']['coordinator']['counters']['read_profile_live_inline'] = 1 - n
            self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])

    def test_focused_matrix_matches_read_sources_and_excludes_nvme_writes(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        jobs = cloud_campaign.make_matrix(spec)
        self.assertEqual(len(jobs), 125)
        reads = [job for job in jobs if job['cohort'] == 'sealed_read']
        self.assertTrue(reads)
        for size in (64, 256, 1024, 8192, 65536):
            rows = [job for job in reads if job['value_size'] == size]
            self.assertEqual(len({job['sealed_dataset']['data_id'] for job in rows}), 1)
            self.assertEqual({job['backend'] for job in rows}, set(spec['backends']))
            source = {job['backend']: job['sealed_dataset']['source_id'] for job in rows}
            self.assertNotEqual(source['native'], source['tidehunter_tcp'])
            for backend in cloud_campaign.NVME_BACKENDS & source.keys():
                self.assertEqual(source[backend], source['tidehunter_tcp'])
            for backend in source.keys() - {'native', 'tidehunter_tcp'} - cloud_campaign.NVME_BACKENDS:
                self.assertNotEqual(source[backend], source['tidehunter_tcp'])
                self.assertNotEqual(source[backend], source['native'])
        self.assertFalse(any(job['backend'] in cloud_campaign.NVME_BACKENDS and
                             job['cohort'] == 'native_write_regression' for job in jobs))

    def test_selected_backend_order_cannot_reenter_physical_source(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        # Exercise the portable matrix, including both optional NVMe transports.
        backends = ['native', 'tidehunter_tcp', 'tidehunter_verbs_native',
                    'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma']
        for order in itertools.permutations(backends):
            with self.subTest(order=order):
                jobs = cloud_campaign.make_matrix(dict(spec, backends=list(order), max_cases=2000))
                reads = [job for job in jobs if job['cohort'] == 'sealed_read']
                completed = set()
                previous = None
                for job in reads:
                    source = job['sealed_dataset']['source_id']
                    if source != previous:
                        self.assertNotIn(source, completed)
                        if previous is not None:
                            completed.add(previous)
                        previous = source

    def test_nvme_backend_is_rejected_outside_focused_mode(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        spec['mode'] = 'standard'
        with self.assertRaises(ValueError):
            cloud_campaign.make_matrix(spec)


class HostCpuIntervalTests(unittest.TestCase):
    def test_kernel_work_is_host_scoped_and_reboots_are_unavailable(self):
        def snapshot(boot, counters):
            return {'nvme': {'host_identity': {'boot_id': boot}}, 'host_cpu_hz': 100,
                    'host_cpu_stat': 'cpu ' + ' '.join(map(str, counters))}
        start = [snapshot(str(i), [0] * 8) for i in range(6)]
        end = [snapshot(str(i), [100, 0, 200, 300, 0, 50, 25, 0]) for i in range(6)]
        end[3]['nvme']['host_identity']['boot_id'] = 'rebooted'
        result = controller.host_cpu_interval(start, end)
        self.assertEqual(result['hosts'][0]['system_seconds'], 2.75)
        self.assertFalse(result['hosts'][3]['available'])
        self.assertIn('rebooted', result['hosts'][3]['reason'])
        self.assertIn('not attribution', result['scope'])


class ControllerBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.c = controller.Controller.__new__(controller.Controller)
        self.c.local_mode = None
        self.c.config = {'prefix': 'run', 'repository_root': str(self.root),
                         'spec': {'case_deadline_seconds': 600}, 'deadline_utc': '2099-01-01T00:00:00Z',
                         'fleet': {'nodes': [{'address': f'10.0.0.{i + 1}'} for i in range(5)]}}
        self.c.root = self.root
        self.c.clients = {'s3': Mock()}
        self.c.active_nvme = None
        self.c.active_tide = None
        self.c.tide_cleanup_forbidden = False
        self.c.shared_tide = None
        self.c.remaining = Mock(return_value=300)
        self.c.cleanup_deadline = Mock(return_value=100)
        self.c.cleanup_timeout = Mock(return_value=60)
        self.c.request = Mock(return_value={'complete': True})

    def mirror_fixtures(self, base, uuid, manifest):
        self.c.config['fleet']['data_root'] = str(base / 'fleet')
        roots, targets = [], []
        for index in range(5):
            root = base / f'mirror-{index}'
            root.mkdir()
            path = root / 'manifest.json'
            path.write_text(json.dumps(manifest))
            remote = base / 'fleet' / 'cells' / 'source-case' / f'node-{index}' / 'nvmeof' / 'export'
            roots.append(str(root))
            targets.append({'target': {'transport': 'tcp', 'export_root': str(remote), 'exports': [{
                'export_uuid': uuid, 'manifest': str(remote / 'manifest.json'),
                'manifest_sha256': controller.digest(path),
                'target_receipt': {'selected_transport': 'tcp', 'target_cache': 'buffered_file_page_cache',
                                   'target_cache_verified': True, 'buffered_io': 1}}]}})
        return roots, targets

    def test_staging_requires_exact_object_and_preserves_verified_bytes(self):
        raw = b'{\"catalog\":[{}],\"nqn\":\"test\"}'
        checksum = hashlib.sha256(raw).hexdigest()
        self.c.config.update(bucket='bucket', account='account')
        self.c.config['fleet']['data_root'] = str(self.root / 'fleet')
        uuid = [1] * 16
        work = self.root / 'fleet/cells/case/node-0/nvmeof'
        key = f'run/results/cells/case/storage/0/nvme-manifests/{bytes(uuid).hex()}.json'
        entry = {'manifest_path': str(work / 'export/partition/manifest.json'),
                 'export_uuid': uuid, 'manifest_key': key, 'manifest_sha256': checksum,
                 'manifest_bytes': len(raw)}
        exports = [{'work_dir': str(work), 'ready': {'exports': [entry]}}]
        s3 = self.c.clients['s3']
        s3.head_object.return_value = {'ContentLength': len(raw), 'Metadata': {'sha256': checksum}}
        s3.download_file.side_effect = lambda bucket, obj, dest, **kw: Path(dest).write_bytes(raw)
        real_path = Path
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        with patch.object(controller, 'Path', side_effect=mapped_path):
            mirrors = self.c.stage_nvme_manifests('case', exports)
        self.assertEqual((real_path(mirrors[0]) / 'partition/manifest.json').read_bytes(), raw)
        s3.head_object.assert_called_once_with(Bucket='bucket', Key=key, ExpectedBucketOwner='account')
        self.assertEqual(s3.download_file.call_args.args[:2], ('bucket', key))
        self.assertEqual(s3.download_file.call_args.kwargs['ExtraArgs'], {'ExpectedBucketOwner': 'account'})

    def test_owner_batch_requires_every_exact_owner_to_confirm(self):
        self.c.request.side_effect = lambda index, *a, **kw: {'complete': index != 3}
        with self.assertRaises(controller.InfrastructureError):
            self.c.owner_batch('nvme_owner_stop', 'case')
        self.assertEqual({call.args[0] for call in self.c.request.call_args_list}, set(range(5)))

    def test_guest_owner_cleanup_accepts_only_exact_s3_proof_location(self):
        import nvme_campaign_owner
        config = {'prefix': 'run', 'spec': {'mode': 'nvmeof-focused'},
                  'fleet': {'data_root': '/unused', 'nodes': [
                      {'index': i, 'instance_id': f'i-owner-{i}', 'address': f'10.0.0.{i + 2}'}
                      for i in range(5)]}}
        request = {'index': 2, 'action': 'nvme_owner_target_cleanup', 'case_id': 'case',
                   'generation': 'gen', 'host_disconnected_key': 'run/results/cells/case/storage/2/nvme-disconnect/gen.json'}
        clients = {'s3': Mock()}
        proof = {'phase': 'disconnected'}
        with patch.object(controller, 'verify_local_identity'), \
             patch.object(controller, 'guarded', return_value=self.root), \
             patch.object(controller, 'read_s3_json', return_value=proof) as read_proof, \
             patch.object(nvme_campaign_owner, 'execute', return_value={'complete': True}) as execute:
            self.assertEqual(controller.guest_operation(config, request, clients), {'complete': True})
            self.assertIs(execute.call_args.args[1]['host_disconnected'], proof)
            execute.reset_mock()
            read_proof.reset_mock()
            forged = dict(request, host_disconnected_key='run/results/cells/case/storage/1/nvme-disconnect/gen.json',
                          host_disconnected=proof)
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, forged, clients)
            read_proof.assert_not_called()
            execute.assert_not_called()
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(dict(config, spec={'mode': 'standard'}), request, clients)
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, dict(request, index=-1), clients)
            execute.assert_not_called()

    def test_setup_addresses_are_taken_from_owned_fleet(self):
        self.c.owner_batch('nvme_owner_target_setup', 'case', generation='generation')
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['address'], self.c.config['fleet']['nodes'][index]['address'])
            self.assertEqual(request['generation'], 'generation')

    def test_rdma_target_uses_owned_gcp_rdma_interface(self):
        for i, node in enumerate(self.c.config['fleet']['nodes']):
            node['rdma_address'] = f'10.1.0.{i + 1}'
        with patch.object(controller, 'platform_module', return_value=object()):
            self.c.owner_batch('nvme_owner_target_setup', 'case', generation='g', transport='rdma')
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['address'], self.c.config['fleet']['nodes'][index]['rdma_address'])

    def test_shared_tcp_source_reuses_prepared_owner_dataset(self):
        options = {'external_remote_config': {'read_lanes': 1}, 'evidence': 'old'}
        self.c.shared_tide = {'identity': ('source', 'tcp', 'synchronous', 'adaptive'), 'options': copy.deepcopy(options)}
        job = {'backend': 'tidehunter_nvme_tcp', 'sealed_dataset': {'source_id': 'source'}, 'read_lanes': 4,
               'durability_tier': 'synchronous'}
        self.c.prepare_tide = Mock(side_effect=AssertionError('must reuse prepared dataset'))
        actual = self.c.prepared_tide_options(job, self.root / 'case')
        self.assertEqual(actual['external_remote_config']['read_lanes'], 4)
        self.assertEqual(actual['evidence'], str(self.root / 'case' / 'read-evidence'))
        self.assertEqual(self.c.shared_tide['options'], options)
        self.c.prepare_tide.assert_not_called()

    def test_a_different_durability_tier_never_reuses_the_prepared_owners(self):
        # Owners run one tier for their whole life; a case at another tier needs fresh owners.
        self.c.shared_tide = {'identity': ('source', 'tcp', 'native'), 'options': {}}
        job = {'backend': 'tidehunter_nvme_tcp', 'sealed_dataset': {'source_id': 'source'},
               'durability_tier': 'synchronous', 'read_lanes': 1}
        self.c.stop_tide = Mock()
        self.c.prepare_tide = Mock(side_effect=RuntimeError('fresh owners requested'))
        with self.assertRaisesRegex(RuntimeError, 'fresh owners requested'):
            self.c.prepared_tide_options(job, self.root / 'case')
        self.c.stop_tide.assert_called_once_with()

    def test_outstanding_namespaces_prevent_source_replacement_and_owner_stop(self):
        self.c.shared_tide = {'identity': ('old', 'tcp', 'synchronous')}
        self.c.active_nvme = {'generation': 'g', 'case': 'old-case', 'hosts': []}
        job = {'backend': 'tidehunter_tcp', 'sealed_dataset': {'source_id': 'new'}}
        with self.assertRaises(controller.SafetyError):
            self.c.prepared_tide_options(job, self.root)
        self.c.active_tide = ({'case_id': 'old-case'}, self.root, [0])
        with self.assertRaises((controller.SafetyError, controller.InfrastructureError)):
            self.c.stop_tide()
        self.c.request.assert_not_called()
        self.assertIsNotNone(self.c.active_tide)

    def test_unconfirmed_client_quiescence_forbids_owner_cleanup(self):
        self.c.active_tide = ({'case_id': 'case'}, self.root, [0])
        self.c.tide_cleanup_forbidden = True
        with self.assertRaises(controller.InfrastructureError):
            self.c.stop_tide()
        self.c.request.assert_not_called()
        self.assertIsNotNone(self.c.active_tide)

    def test_failed_owner_cleanup_quarantines_remaining_owners(self):
        active = ({'case_id': 'case'}, self.root, [0])
        self.c.active_tide = active
        self.c.request.side_effect = RuntimeError('owner shutdown unconfirmed')
        with self.assertRaises(controller.InfrastructureError):
            self.c.stop_tide()
        self.assertIs(self.c.active_tide, active)
        self.assertTrue(self.c.tide_cleanup_forbidden)
        self.c.request.reset_mock()
        with self.assertRaises(controller.InfrastructureError):
            self.c.stop_tide()
        self.c.request.assert_not_called()

    def test_undrained_or_incomplete_host_set_is_quarantined(self):
        for drained, count in ((False, 5), (True, 4)):
            with self.subTest(drained=drained, hosts=count):
                active = {'generation': 'g', 'case': 'case',
                          'hosts': [{'index': i, 'state': f'/state/{i}'} for i in range(count)]}
                self.c.active_nvme = active
                with patch.object(controller, 'command') as command:
                    with self.assertRaises(controller.InfrastructureError):
                        self.c.disconnect_nvme(drained=drained)
                command.assert_not_called()
                self.c.request.assert_not_called()
                self.assertIs(self.c.active_nvme, active)

    def test_cleanup_uses_exact_per_owner_proof_and_retains_unknown_outcome(self):
        real_path = Path
        control = self.root / 'control' / 'run'
        control.mkdir(parents=True)
        hosts = []
        for index in range(5):
            host = {'index': index, 'state': str(control / f'state-{index}'),
                    'mirror': str(control / f'mirror-{index}'),
                    'ready_path': str(control / f'ready-{index}.json'),
                    'target_path': str(control / f'target-{index}.json')}
            real_path(host['state']).mkdir()
            real_path(host['mirror']).mkdir()
            real_path(host['ready_path']).write_text('{}')
            real_path(host['target_path']).write_text('{}')
            hosts.append(host)
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        active = {'generation': 'g', 'case': 'case', 'case_directory': str(self.root),
                  'hosts': hosts, 'target_implementation': 'kernel'}
        self.c.active_nvme = active
        self.c.request.side_effect = lambda index, *a, **kw: {'complete': index != 2}
        with patch.object(controller, 'Path', side_effect=mapped_path), \
             patch.object(controller, 'command', return_value=json.dumps({'phase': 'disconnected'})), \
             patch.object(controller, 's3_json') as upload:
            with self.assertRaises(controller.InfrastructureError):
                self.c.disconnect_nvme(drained=True)
        self.assertIs(self.c.active_nvme, active)
        self.assertEqual({call.args[0] for call in self.c.request.call_args_list}, set(range(5)))
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['host_disconnected_key'], f'run/results/cells/case/storage/{index}/nvme-disconnect/g.json')
            self.assertIs(request['readers_drained'], True)
            self.assertEqual(request['action'], 'nvme_owner_target_cleanup')
        for host in hosts:
            for field in ('state', 'ready_path', 'target_path'):
                self.assertEqual(real_path(host[field]).exists(), host['index'] == 2)
            self.assertTrue(real_path(host['mirror']).exists())

    def test_compact_sealed_targets_connect_trace_and_drain(self):
        real_path = Path
        jobs = cloud_campaign.make_focused_matrix('fabric_ab_compact')
        arms = [next(j for j in jobs if j['backend'] == backend and j.get('nvmeof_target') == target)
                for backend, target in (('tidehunter_nvme_rdma', 'kernel'),
                                        ('tidehunter_nvme_rdma', 'spdk'),
                                        ('tidehunter_nvme_tcp', 'kernel'))]
        self.c.config['fleet']['nodes'] = self.c.config['fleet']['nodes'][:1]
        for job in arms:
            with self.subTest(backend=job['backend'], target=job['nvmeof_target']), \
                 tempfile.TemporaryDirectory() as tmp:
                base = real_path(tmp)
                roots, targets = self.mirror_fixtures(base, [1, 2], {'catalog': [{}], 'nqn': 'test-nqn'})
                transport = 'tcp' if job['backend'].endswith('_tcp') else 'rdma'
                implementation = 'spdk' if job['nvmeof_target'] == 'spdk' else 'kernel_direct'
                target = targets[0]['target']
                target['transport'] = transport
                entry = target['exports'][0]
                entry['target'] = implementation
                entry['target_receipt'] = {'selected_transport': transport, 'target_cache_verified': True,
                    'target_cache': ('direct_file_bdev_no_OS_pagecache' if implementation == 'spdk'
                                     else 'direct_file_no_OS_pagecache'),
                    'buffered_io': 0, 'target_implementation': implementation,
                    'direct_io_verified': True, 'read_only_verified': True}
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}],
                    'mirror_roots': roots[:1], 'options': {'external_remote_config': {
                        'read_memory_allowance_bytes': 256 * 1024**2,
                        'read_cache_bytes': 0, 'max_pending_read_bytes': 0}}}
                self.c.owner_batch = Mock(return_value=targets[:1])
                self.c.active_nvme = None
                result = {'phase': 'connected', 'transport': transport,
                    'bindings': [{'export_uuid': [1, 2], 'namespace_path': '/dev/test',
                                  'transport': transport, 'target': implementation}],
                    'exports': [{'export_uuid': [1, 2], 'host_receipt': {'controller': 'nvme0'}}]}
                def mapped_path(value):
                    path = real_path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                def command(argv, *, timeout):
                    if argv[:2] == ['nvme', 'id-ctrl']:
                        return json.dumps({'cntlid': 7})
                    if 'host-disconnect' in argv:
                        return json.dumps({'phase': 'disconnected'})
                    real_path(argv[argv.index('--state') + 1]).mkdir()
                    return json.dumps(result)
                with patch.object(controller, 'Path', side_effect=mapped_path), \
                     patch.object(controller, 'command', side_effect=command), \
                     patch.object(controller, 'persist_json'):
                    readers = self.c.connect_nvme(job, base)
                    self.assertEqual(len(readers), 1)
                    self.assertEqual(readers[0]['binding']['transport'], transport)
                    self.assertEqual(self.c.nvme_trace('start', 10), [{'complete': True}])
                    with self.assertRaises(controller.InfrastructureError):
                        self.c.disconnect_nvme(drained=False)
                    self.c.disconnect_nvme(drained=True)
                    self.assertIsNone(self.c.active_nvme)
                self.assertEqual(self.c.owner_batch.call_args.kwargs['transport'], transport)

    def test_rdma_transport_is_used_for_targets_and_namespace_bindings(self):
        real_path = Path
        manifest = {'catalog': [{}], 'nqn': 'test-nqn'}
        roots, targets = self.mirror_fixtures(self.root, [1, 2], manifest)
        for owner in targets:
            owner['target']['transport'] = 'rdma'
            owner['target']['exports'][0]['target_receipt']['selected_transport'] = 'rdma'
        self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                              'mirror_roots': roots, 'options': {'external_remote_config': {
                                  'read_memory_allowance_bytes': 256 * 1024**2,
                                  'read_cache_bytes': 0, 'max_pending_read_bytes': 0}}}
        self.c.owner_batch = Mock(return_value=targets)
        result = {'phase': 'connected', 'transport': 'rdma',
                  'bindings': [{'export_uuid': [1, 2], 'namespace_path': '/dev/test-only', 'transport': 'rdma'}],
                  'exports': [{'export_uuid': [1, 2], 'host_receipt': {'controller': 'nvme0'}}]}
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        def command(argv, *, timeout):
            return json.dumps({'cntlid': 7} if argv[:2] == ['nvme', 'id-ctrl'] else result)
        with patch.object(controller, 'Path', side_effect=mapped_path), \
             patch.object(controller, 'command', side_effect=command):
            readers = self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_rdma',
                                           'nvmeof_route': 'sealed_read',
                                           'sealed_dataset': {'source_id': 'source-case'}}, self.root)
        self.assertEqual(self.c.owner_batch.call_args.kwargs['transport'], 'rdma')
        self.assertEqual(self.c.active_nvme['transport'], 'rdma')
        self.assertEqual({row['binding']['transport'] for row in readers}, {'rdma'})

    def test_missing_or_extra_reader_binding_is_rejected_and_quarantined(self):
        real_path = Path
        for bindings in ([], [{'export_uuid': [1], 'namespace_path': '/dev/test', 'transport': 'tcp'},
                              {'export_uuid': [2], 'namespace_path': '/dev/other', 'transport': 'tcp'}]):
            with self.subTest(bindings=bindings), tempfile.TemporaryDirectory() as directory:
                base = real_path(directory)
                self.c.active_nvme = None
                roots, targets = self.mirror_fixtures(base, [1], {'catalog': [{}], 'nqn': 'test-nqn'})
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                                      'mirror_roots': roots}
                self.c.owner_batch = Mock(return_value=targets)
                def mapped_path(value):
                    path = real_path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                result = {'phase': 'connected', 'transport': 'tcp', 'bindings': bindings,
                          'exports': [{'export_uuid': [1], 'host_receipt': {'controller': 'nvme0'}}]}
                def command(argv, *, timeout):
                    if argv[:2] == ['nvme', 'id-ctrl']:
                        return json.dumps({'cntlid': 7})
                    return json.dumps(result)
                with patch.object(controller, 'Path', side_effect=mapped_path), \
                     patch.object(controller, 'command', side_effect=command):
                    with self.assertRaises(controller.SafetyError):
                        self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_tcp',
                                             'nvmeof_route': 'sealed_read',
                                             'sealed_dataset': {'source_id': 'source-case'}}, base)
                self.assertEqual(len(self.c.active_nvme['hosts']), 1)

    def test_target_policy_and_transport_mismatch_prevent_host_connection(self):
        for field, value in (('selected_transport', 'rdma'), ('target_cache', 'direct'),
                             ('target_cache_verified', False)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                roots, targets = self.mirror_fixtures(base, [1], {'catalog': [{}], 'nqn': 'test-nqn'})
                targets[0]['target']['exports'][0]['target_receipt'][field] = value
                self.c.active_nvme = None
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                                      'mirror_roots': roots}
                self.c.owner_batch = Mock(return_value=targets)
                def mapped_path(value):
                    path = Path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                with patch.object(controller, 'command') as command, \
                     patch.object(controller, 'dump'), \
                     patch.object(controller, 'Path', side_effect=mapped_path):
                    with self.assertRaises(controller.SafetyError):
                        self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_tcp',
                                             'nvmeof_route': 'sealed_read',
                                             'sealed_dataset': {'source_id': 'source-case'}}, base)
                command.assert_not_called()
                self.assertEqual(self.c.active_nvme['hosts'], [])
                self.assertIs(self.c.active_nvme['targets'], targets)


if __name__ == '__main__':
    unittest.main()
