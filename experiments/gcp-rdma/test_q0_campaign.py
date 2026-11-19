import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import manual_campaign as manual
import cloud_campaign
import cloud_controller as controller
import cloud_package
import q0_campaign as q0
import workflow_input


def seals():
    return {role: {
        'revision': q0.BASELINE if role == 'baseline_ab' else 'a' * 40 if role == 'land4' else role,
        'sha256': hashlib.sha256(role.encode()).hexdigest(),
        **({'instrumentation_patch_sha256': q0.COUNTER_PATCH_SHA256} if role in q0.AB_ROLES else {}),
    } for role in q0.ROLES}


class SelectionTests(unittest.TestCase):
    def test_matrix_and_quote(self):
        spec = workflow_input.specification('kv', 'tidehunter_nvme_rdma', 'focused', q0.SELECTION,
                                            case_binaries=seals())
        jobs = manual._reviewed_jobs(spec)
        self.assertEqual(jobs, controller.controller_matrix(spec))
        self.assertEqual(spec['matrix_sha256'], 'ec7b1e585125b53a952b97f8da54b4fb10376c9b54db1f45163c8d442b9a82ec')
        self.assertEqual(len({j['case_id'] for j in jobs}), 60)
        self.assertEqual(jobs[0]['binary_role'], q0.BISECT_REVISIONS[5])
        orders = []
        for repeat in range(3):
            block = [j for j in jobs if j['repeat'] == repeat]
            probes = [j for j in block if j['arm'] == 'bisect']
            orders.append(tuple(j['binary_role'] for j in probes))
            self.assertEqual(set(orders[-1]), set(q0.BISECT_REVISIONS))
            ab = [(j['binary_role'], j['case_kind'], j['durability_tier']) for j in block if j['arm'] != 'bisect']
            self.assertCountEqual(ab, [(role, workload, tier) for role in q0.AB_ROLES
                                       for workload in ('remote_put', 'mainnet_replay')
                                       for tier in ('native', 'synchronous')])
            for job in block:
                self.assertEqual(cloud_campaign.validate_focused_job(job), job)
                self.assertEqual((job['value_size'], job['owner_count'], job['target_io']),
                                 (512, 2, 'buffered'))
                if job['case_kind'] == 'mainnet_replay':
                    self.assertEqual(job['concurrency'], 16)
                    self.assertEqual(job.get('checkpoint_pipeline_depth', 1), 1)
                else:
                    self.assertEqual(job['concurrency'], 64)
                    self.assertEqual(job['mix']['put'], 100)
        self.assertEqual(len(set(orders)), 3)
        quote = manual.q0_quote(spec)
        self.assertEqual((quote['put_cases'], quote['replay_cases']), (48, 12))
        self.assertEqual((quote['model_seconds'], quote['reservation_seconds'], quote['ceiling_cents']),
                         (11340, 14400, 4834))
        self.assertEqual(quote['guard_slack_seconds'], 2460)

    def test_bound_q0_builds_focused_rdma_fleet_and_pair_startup(self):
        case_binaries = seals()
        case_binaries[q0.BISECT_REVISIONS[-2]]['sha256'] = case_binaries[q0.BASELINE]['sha256']
        spec = manual.q0_spec(case_binaries)
        self.assertEqual(spec['job_count'], 57)
        qualify = manual.qualify
        members = [{'name': f'bench-q0-{i}', 'id': str(i + 1), 'rdma_ip': f'10.1.0.{i + 1}'}
                   for i in range(spec['owner_hosts'] + 1)]
        args = ('test-bucket', 'bench-0123456789abcdef', 'a' * 64,
                '2030-01-01T04:00:00Z', 'b' * 64, 'c' * 64)
        options = {'members': members, 'rdma_device': 'irdma0', 'machine_type': spec['machine_type'],
                   'live_transports': ['rdma'], 'live_spdk': bool(spec.get('spdk_comparison')),
                   'qualification_profile': spec['mode'], 'selection': spec['selection']}
        self.assertTrue(manual.gcp_runtime.live_mode(spec))
        script = qualify.fleet_startup_script(*args, **options)
        payload = script.split("<<'TIDEHUNTER_BOOTSTRAP'\n", 1)[1].removesuffix('TIDEHUNTER_BOOTSTRAP\n')
        compile(payload, '<Q0 fleet startup>', 'exec')
        for invalid in ({'members': members[:2]}, {'live_transports': ['tcp']},
                        {'live_transports': ['rdma', 'tcp']}, {'live_spdk': True},
                        {'selection': 'nvmeof_f7_highmem32'}):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, 'focused|write-engine'):
                qualify.fleet_startup_script(*args, **(options | invalid))

        # The host already quotes/waits 480 s; the generated guest subprocess
        # must enforce that same window, with its separate exit allowance.
        with patch.object(qualify.time, 'time', return_value=10), patch.object(qualify, '_call') as call:
            qualify._fleet_pair(Path('/mnt/package'), {'selection': spec['selection']}, 3600)
        argv, _, timeout = call.call_args.args
        self.assertEqual(json.loads(argv[3])['phase_deadline'], 490)
        self.assertEqual(timeout, 480 + qualify.PAIR_EXIT_SECONDS)

    def test_rejects_incomplete_or_wrong_bindings(self):
        value = seals()
        with self.assertRaisesRegex(ValueError, 'every selected'):
            manual.q0_spec({})
        spec = manual.q0_spec(value)
        del spec['case_binaries'][q0.F1]
        with self.assertRaisesRegex(ValueError, 'every selected'):
            manual._reviewed_jobs(spec)
        value['baseline_ab']['sha256'] = value[q0.BASELINE]['sha256']
        with self.assertRaisesRegex(ValueError, 'reused'):
            manual.q0_spec(value)
        value = seals()
        value['land4']['instrumentation_patch_sha256'] = 'b' * 64
        with self.assertRaisesRegex(ValueError, 'reviewed instrumentation'):
            manual.q0_spec(value)
        value = seals()
        value['land4']['sha256'] = value['baseline_ab']['sha256']
        with self.assertRaisesRegex(ValueError, 'across an instrumented A/B role'):
            manual.q0_spec(value)

    def test_byte_identical_history_runs_once_per_repeat(self):
        alias = q0.BISECT_REVISIONS[-2]
        binaries = seals()
        binaries[alias]['sha256'] = binaries[q0.BASELINE]['sha256']
        spec = manual.q0_spec(binaries)
        jobs = manual._reviewed_jobs(spec)
        self.assertEqual(jobs, controller.controller_matrix(spec))
        self.assertEqual(spec['bisect_revisions'][q0.BASELINE], [q0.BASELINE, alias])
        self.assertEqual(sum(j['binary_role'] == q0.BASELINE for j in jobs), q0.REPEATS)
        self.assertNotIn(alias, {j['binary_role'] for j in jobs})
        self.assertEqual(len(jobs), 57)
        for repeat in range(q0.REPEATS):
            probe_digests = [binaries[j['binary_role']]['sha256'] for j in jobs
                             if j['repeat'] == repeat and j['arm'] == 'bisect']
            self.assertEqual(len(probe_digests), 11)
            self.assertEqual(len(set(probe_digests)), 11)
        quote = manual.q0_quote(spec)
        self.assertEqual((quote['model_seconds'], quote['guard_slack_seconds'], quote['ceiling_cents']),
                         (11100, 2700, 4834))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cloud_package.dump(root / 'matrix.json', {'spec': spec, 'jobs': jobs})
            report = cloud_campaign.report(root)
        self.assertEqual(report['bisect_revisions'][q0.BASELINE], [q0.BASELINE, alias])
        self.assertEqual(sum(case.get('bisect_revisions') == [q0.BASELINE, alias]
                             for case in report['cases']), q0.REPEATS)
        spec['bisect_revisions'][q0.BASELINE].remove(alias)
        with self.assertRaisesRegex(ValueError, 'revision attribution'):
            controller.controller_matrix(spec)

    def test_packaging_verifies_both_sources_but_installs_shared_bytes_once(self):
        alias = q0.BISECT_REVISIONS[-2]
        binaries = seals()
        binaries[alias]['sha256'] = binaries[q0.BASELINE]['sha256']
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            packages = {}
            for role, seal in binaries.items():
                if role == 'land4':
                    continue
                package = root / role
                binary = package / 'runtime/bin/kv-research'
                binary.parent.mkdir(parents=True)
                binary.write_bytes((q0.BASELINE if role == alias else role).encode())
                cloud_package.dump(package / 'manifest.json', {
                    'revision': seal['revision'], 'dirty': role == 'baseline_ab',
                })
                packages[role] = package

            def verify(package):
                identity = cloud_package.identity(package / 'runtime/bin/kv-research')
                return {'build_profile': 'focused', 'binary_sha256': identity['sha256']}

            output = root / 'stage'
            with patch.object(cloud_package, 'verify_directory', side_effect=verify) as verified:
                cloud_package._stage_case_binaries(output, packages, binaries)
            self.assertCountEqual([call.args[0] for call in verified.call_args_list], packages.values())
            paths = list((output / 'runtime/case-binaries').glob('*/kv-research'))
            self.assertEqual(len(paths), 12)
            shared = cloud_package.case_binary_path(output, binaries[alias]['sha256'])
            self.assertEqual(shared.read_bytes(), q0.BASELINE.encode())

    def test_rejects_generated_matrix_drift(self):
        spec = manual.q0_spec(seals())
        jobs = cloud_campaign.make_focused_matrix(q0.SELECTION)
        jobs[0]['value_size'] += 1
        with patch.object(cloud_campaign, 'make_focused_matrix', return_value=jobs):
            with self.assertRaisesRegex(ValueError, 'differs from the reviewed matrix'):
                manual._reviewed_jobs(spec)

    def test_counter_delta_never_interprets_missing_or_reset_as_zero(self):
        evidence = {'measurement_drained': True, 'measurement_attempted': True,
                    'start': {'owner_storage': {'available': True, 'live_frontier_installs': 7},
                              'durable_cut': {'enabled': True, 'epoch': 1, 'nudges': 2}},
                    'end': {'owner_storage': {'available': True, 'live_frontier_installs': 37},
                            'durable_cut': {'enabled': True, 'epoch': 1, 'nudges': 8}}}
        self.assertEqual(q0.overhead_counts(evidence, 2_000_000_000, 'native')['counts'],
                         {'live_frontier_installs': 30, 'durable_cut_nudges': 6})
        for field, value, failure in (('nudges', 1, 'decreased'), ('epoch', 2, 'population'),
                                      ('enabled', False, 'population')):
            altered = copy.deepcopy(evidence)
            altered['end']['durable_cut'][field] = value
            with self.assertRaisesRegex(ValueError, failure):
                q0.overhead_counts(altered, 2_000_000_000, 'native')


class DispatchTests(unittest.TestCase):
    def setUp(self):
        stage = tempfile.TemporaryDirectory()
        self.addCleanup(stage.cleanup)
        self.stage = Path(stage.name)
        self.seals = seals()
        self.spec = manual.q0_spec(self.seals)
        self.paths = {}
        default = self.stage / 'runtime/bin/kv-research'
        files = {}
        for role, seal in self.seals.items():
            path = default if role == 'land4' else cloud_package.case_binary_path(self.stage, seal['sha256'])
            path.parent.mkdir(parents=True)
            path.write_bytes(role.encode())
            path.chmod(0o755)
            self.paths[role] = path
            files[path.relative_to(self.stage).as_posix()] = cloud_package.identity(path)
        self.manifest = {'files': files, 'case_binaries': self.seals, 'dirty': False,
                         'binary_sha256': self.seals['land4']['sha256'],
                         'revision': self.seals['land4']['revision']}
        cloud_package.dump(self.stage / 'manifest.json', self.manifest)
        self.config = {'binary': str(default), 'repository_root': str(self.stage / 'source'), 'spec': self.spec,
                       'prefix': 'bench-0000000000000000',
                       'fleet': {'nodes': [{'index': 0}, {'index': 1}], 'data_root': str(self.stage)}}
        self.jobs = {j['binary_role']: j for j in controller.controller_matrix(self.spec)}

    def test_distinct_exact_and_instrumented_pair(self):
        for job in controller.controller_matrix(self.spec):
            role = job['binary_role']
            self.assertEqual(controller.resolve_case_binary(self.config, job),
                             (self.paths[role], self.seals[role]))
        self.assertEqual(controller.resolve_case_binary({'binary': '/legacy'}, {}), (Path('/legacy'), None))
        changed = {**self.jobs[q0.F1], 'binary_role': 'land4'}
        with self.assertRaisesRegex(controller.SafetyError, 'role differs'):
            controller.resolve_case_binary(self.config, changed)

    def test_archive_preparation_keeps_the_sealed_binary_config(self):
        runtime = controller.Controller.__new__(controller.Controller)
        runtime.config = self.config
        runtime.remaining = lambda: 60
        self.config['fleet'].update(server_cpus=8, server_memory_mib=8192)
        for index, node in enumerate(self.config['fleet']['nodes']):
            node['address'] = '10.0.0.' + str(index + 2)
        directory = self.stage / 'prepare'
        directory.mkdir()
        job = self.jobs[q0.F1]
        with patch.object(controller, 'command', side_effect=RuntimeError('prepare captured')) as prepare, \
             patch.object(controller, 'environment', return_value={}):
            with self.assertRaisesRegex(RuntimeError, 'prepare captured'):
                runtime.prepare_tide(job, directory)
        self.assertEqual(Path(prepare.call_args.args[0][0]), self.paths[q0.F1])
        config = json.loads((directory / 'prepare-input.json').read_text())
        self.assertEqual(config['history_persistence'], 'wal_replay')
        live = config['live_nvmeof']
        self.assertEqual(live['write_mode'], 'leased')
        nqn = 'nqn.2014-08.org.nvmexpress:uuid:01234567-89ab-4def-8123-456789abcdef'
        keys = ('max_inflight_requests', 'max_inflight_bytes', 'connect_timeout_ms',
                'io_timeout_ms', 'inline_reads', 'queue_geometry', 'export_mode',
                'lease_ahead', 'polled_writes', 'leased_write_window', 'write_mode',
                'leased_sync', 'leased_flush_min_frame_bytes')
        prepared = {key: live[key] for key in keys if key in live}
        prepared.update(host_nqn=nqn, host_id=controller.live_host_identity(nqn),
                        leased_durability=job['durability_tier'])
        self.assertEqual(controller.admit_prepared_live_client(
            prepared, live, job['durability_tier']), nqn)
        with self.assertRaises(controller.SafetyError):
            controller.admit_prepared_live_client(
                prepared | {'write_mode': 'reserved'}, live, job['durability_tier'])

    def test_archive_counter_schema_remains_admissible(self):
        job = self.jobs[q0.F1]
        counters = dict.fromkeys(cloud_campaign.LIVE_NVME_COUNTERS, 0) | {
            'payload_rpc_requests': 0, 'reserve_exchanges': 0, 'complete_exchanges': 0,
            'owner_completion_ns': 0, 'owner_completion_samples': 0,
        }
        owners = {'available': True, 'partitions': {
            str(i): {'available': True, 'counters': {}} for i in range(job['owner_count'])}}
        frame = {'live_nvmeof': counters, 'owner_executor': owners}
        evidence = {'schema_version': 1, 'scope': 'measurement_boundary',
                    'measurement_attempted': True, 'measurement_drained': True,
                    'deltas': {'available': False, 'owner_executor': owners},
                    'start': frame, 'end': copy.deepcopy(frame),
                    'live_nvmeof': {'available': True, 'counters': counters.copy()}}
        result = self.stage / 'result'
        result.mkdir()
        (result / 'measurement-read-evidence.json').write_text(json.dumps(evidence))
        self.assertEqual(cloud_campaign.admit_measurement_read_evidence(self.stage)['observed'], evidence)
        evidence['end']['live_nvmeof']['payload_rpc_requests'] = 1
        evidence['live_nvmeof']['counters']['payload_rpc_requests'] = 1
        self.assertIn('payload RPC observed on archived live NVMe route',
                      cloud_campaign.qualify_live_nvme_measurement(evidence, job)['reasons'])

    def test_no_fallback_when_bytes_or_manifest_do_not_match(self):
        path = self.paths[q0.F1]
        path.write_bytes(b'wrong bytes')
        with self.assertRaisesRegex(controller.SafetyError, 'reviewed SHA'):
            controller.resolve_case_binary(self.config, self.jobs[q0.F1])
        path.unlink()
        with self.assertRaisesRegex(controller.SafetyError, 'case binary admission failed'):
            controller.resolve_case_binary(self.config, self.jobs[q0.F1])
        del self.manifest['files'][path.relative_to(self.stage).as_posix()]
        cloud_package.dump(self.stage / 'manifest.json', self.manifest)
        with self.assertRaisesRegex(ValueError, 'missing, extra or mismatched'):
            cloud_package.admit_case_binaries(self.stage, self.seals)

    def test_owner_refuses_cross_revision_before_accepting_capsule(self):
        req = {'index': 0, 'action': 'tide_start', 'case_id': self.jobs[q0.F1]['case_id'],
               'case_binary': self.seals['land4']}
        with patch.object(controller, 'verify_local_identity'), patch.object(
                controller, 'guarded', side_effect=lambda path, *_args, **_kwargs: Path(path)), patch.object(
                controller, 'download_object') as download:
            with self.assertRaisesRegex(controller.SafetyError, 'owner and coordinator binary seals differ'):
                controller.guest_operation(self.config, req, {})
            download.assert_not_called()


if __name__ == '__main__':
    unittest.main()
