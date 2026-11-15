import copy
import hashlib
import itertools
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import analyze
import cloud_campaign
import cloud_package
import nvmeof_path
import cloud_controller
import fleet_tools
import focused_analysis


class FleetToolsTests(unittest.TestCase):
    def jobs(self, kind):
        return [j for j in fleet_tools.make_jobs() if j['case_kind'] == kind]

    def raw_fio(self, spec):
        return json.dumps({'jobs': [{'error': 0, 'elapsed': 2,
            'read': {'total_ios': 0},
            'write': {'total_ios': 1000, 'io_bytes': spec['block_bytes'] * 1000,
                      'runtime': 1500, 'clat_ns': {'percentile': {
                          '50.000000': 2000, '99.000000': 4000, '99.900000': 6000}}},
            'iodepth_level': {'32': 100}}]}).encode()

    def admission_raw(self, spec):
        metadata = dict(type='metadata', schema=2, label=fleet_tools.LABEL, engine_path=False,
                        callers=[spec['callers']], owners=[spec['owners']], batches=spec['batches'],
                        fractions_pct=spec['fractions_pct'], distributions=[spec['distribution']],
                        options=spec['options'], seconds=spec['seconds'], repeats=spec['repeats'],
                        sample_limit_per_caller=spec['sample_limit'], pin_map_actor_then_callers=[0, 1])
        rows = []
        for batch in spec['batches']:
            fractions = [0] if spec['owners'] == 1 or batch == 1 else spec['fractions_pct']
            for fraction, option, repeat in itertools.product(fractions, spec['options'], range(1, spec['repeats'] + 1)):
                rows.append(dict(type='case', label=fleet_tools.LABEL, case=f'{option}-{batch}-{fraction}',
                                 batch_keys=batch, requested_multi_pct=fraction, option=option, repetition=repeat,
                                 callers=spec['callers'], owners=spec['owners'], distribution=spec['distribution'],
                                 seconds=spec['seconds'], offered=10, successful=10, rejected=0,
                                 latency_samples=10, successful_complete_buckets=9, batches_s=9,
                                 mutations_s=9 * batch, p50_ns=1000, p99_ns=2000, p999_ns=3000,
                                 mean_inflight=1, cpu_ns_per_batch=500, contaminated=False))
        return ('\n'.join(json.dumps(r) for r in [metadata, *rows]) + '\n').encode()

    def controller(self, root):
        return SimpleNamespace(root=Path(root), config={'binary_blake2b512': 'a' * 128},
            active_nvme=None, active_tide=None, active_backend=None, shared_tide=None,
            guard=MagicMock(), stop_tide=MagicMock(), remaining=lambda **kw: 10000,
            select_monitoring=MagicMock(), finish_monitoring=MagicMock())

    def test_fixed_grid_digest_and_exact_admission(self):
        jobs = fleet_tools.make_jobs()
        self.assertEqual(len(jobs), 34)
        self.assertEqual(len({j['case_id'] for j in jobs}), len(jobs))
        self.assertEqual(cloud_campaign.make_focused_matrix(fleet_tools.SELECTION), jobs)
        self.assertEqual(cloud_campaign.selection_owner_hosts(fleet_tools.SELECTION), 1)
        self.assertEqual(cloud_campaign.focused_group_counts(fleet_tools.SELECTION), {
            'G33F': 8, 'G33A': 26, 'nvmeof_write_ceiling': 8, 'admission_options': 26})
        for job in jobs:
            cloud_campaign.validate_focused_job(job)
            self.assertEqual(cloud_campaign.job_owner_hosts(job), 1)
        bad = copy.deepcopy(jobs[0]); bad['fio_write']['queue_depth'] = 64
        with self.assertRaises(ValueError):
            cloud_campaign.validate_focused_job(bad)
        digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(digest, '68915419fabaa9f74f52c3f4f29beedbf1aa64550c81109682bc0db1eada682d')

    def test_measurement_budget_and_internal_followup_builder(self):
        rows = self.jobs('admission_options')
        windows = sum(len(j['admission']['options']) * j['admission']['repeats'] *
                      sum(1 if j['admission']['owners'] == 1 or b == 1 else 3
                          for b in j['admission']['batches']) for j in rows)
        self.assertEqual(windows, 640)
        self.assertEqual(windows * 1.2 + 240, fleet_tools.TOOL_BUDGET_SECONDS['admission_options'])
        self.assertEqual({tuple(j['admission']['batches']) for j in rows if j['concurrency'] in (1, 4)}, {(2, 22)})
        followup = fleet_tools._measurement_jobs(repetitions=3,
            cell_filter=lambda c, o, d, b: c == 16 and o == 4 and d == 'uniform' and b == 22,
            options=['A-model', 'D'], selection='not-registered')
        admission = [j for j in followup if j['case_kind'] == 'admission_options']
        self.assertEqual(len(admission), 1)
        self.assertEqual(admission[0]['admission']['repeats'], 3)
        self.assertEqual(admission[0]['admission']['options'], ['A-model', 'D'])
        with self.assertRaises(ValueError):
            cloud_campaign.validate_focused_job(admission[0])

    def test_write_matrix_and_disjoint_pinned_fio_commands(self):
        jobs = self.jobs('nvmeof_write_ceiling')
        self.assertEqual({(j['fio_write']['block_bytes'], j['fio_write']['jobs'], j['fio_write']['dsync']) for j in jobs},
                         set(itertools.product((4096, 65536), (1, 4), (False, True))))
        for job in jobs:
            spec = job['fio_write']
            queues = [dict(queue=q, cpu=q + 1, cpu_list=str(q + 1)) for q in range(spec['jobs'])]
            argv = fleet_tools.fio_argv('/usr/bin/fio', '/dev/nvme3n1', spec, queues)
            self.assertIn('--rw=write', argv)
            self.assertIn('--iodepth=32', argv)
            self.assertIn('--sync=' + ('dsync' if spec['dsync'] else 'none'), argv)
            self.assertIn('--size=' + str(fleet_tools.REGION_BYTES // spec['jobs']), argv)
            self.assertIn('--offset_increment=' + str(fleet_tools.REGION_BYTES // spec['jobs']), argv)
            self.assertIn('--cpus_allowed_policy=split', argv)
            self.assertNotIn('--readonly', argv)
        with self.assertRaises(ValueError):
            fleet_tools.fio_argv('fio', '/dev/nvme3n1', jobs[-1]['fio_write'], [dict(queue=0, cpu=1)] * 4)

    def test_hardware_queue_cpu_map_and_insufficient_queues_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); mq = root / 'nvme3n1' / 'mq'
            for index, cpus in enumerate(('0-1,8-9', '2-3', '4-5', '6-7')):
                (mq / str(index)).mkdir(parents=True)
                (mq / str(index) / 'cpu_list').write_text(cpus)
            result = fleet_tools.queue_cpus('/dev/nvme3n1', 4, root, set(range(1, 8)))
            self.assertEqual([r['cpu'] for r in result], [1, 2, 4, 6])
            self.assertEqual(len({r['queue'] for r in result}), 4)
            with self.assertRaises(ValueError):
                fleet_tools.queue_cpus('/dev/nvme3n1', 4, root, {1, 2})
            (mq / '0' / 'cpu_list').write_text('0-9999999')
            with self.assertRaises(ValueError):
                fleet_tools.queue_cpus('/dev/nvme3n1', 1, root, {1})

    def test_fio_runtime_units_and_invalid_populations(self):
        spec = self.jobs('nvmeof_write_ceiling')[0]['fio_write']
        raw = self.raw_fio(spec)
        metrics = fleet_tools.fio_metrics(raw, spec)
        self.assertEqual(metrics['iops'], 1000 / 1.5)  # not rounded job elapsed
        self.assertEqual(metrics['mb_per_second'], 4096000 / 1.5 / 1e6)
        self.assertEqual(metrics['completion_latency']['p99_ns'], 4000)
        document = json.loads(raw); document['jobs'][0]['error'] = 5
        with self.assertRaises(ValueError):
            fleet_tools.fio_metrics(json.dumps(document).encode(), spec)
        document = json.loads(raw); document['jobs'][0]['write']['io_bytes'] += 1
        with self.assertRaises(ValueError):
            fleet_tools.fio_metrics(json.dumps(document).encode(), spec)

    def test_admission_cli_and_exact_jsonl_population(self):
        for job in self.jobs('admission_options'):
            spec = job['admission']
            argv = fleet_tools.admission_argv('/sealed/admission-options', '/results/case', spec)
            self.assertIn('--skip-codec', argv)
            self.assertEqual(argv[argv.index('--repeats') + 1], '1')
            result = fleet_tools.admission_records(self.admission_raw(spec), spec)
            self.assertEqual(result['interpretation'], fleet_tools.CAVEATS)
            self.assertEqual(result['value_independent_controls_bytes'], [64, 1024])
        spec = self.jobs('admission_options')[-1]['admission']; raw = self.admission_raw(spec)
        lines = raw.splitlines()
        for bad in (b'\n'.join(lines[:-1]), raw + lines[-1] + b'\n'):
            with self.assertRaises(ValueError):
                fleet_tools.admission_records(bad, spec)
        metadata = json.loads(lines[0]); metadata['pin_map_actor_then_callers'] = []
        with self.assertRaises(ValueError):
            fleet_tools.admission_records(b'\n'.join([json.dumps(metadata).encode(), *lines[1:]]), spec)

    def test_controller_dispatch_and_selection_binding(self):
        instance = object.__new__(cloud_controller.Controller)
        instance.config = {'spec': {'selection': fleet_tools.SELECTION}}
        for kind in fleet_tools.KINDS:
            job = self.jobs(kind)[0]
            with patch.object(fleet_tools, 'run_case', return_value='tool') as call:
                self.assertEqual(instance.run_case(job), 'tool')
                call.assert_called_once_with(instance, job, cloud_controller)
        instance.config['spec']['selection'] = 'nvmeof_f8'
        with self.assertRaises(cloud_controller.SafetyError):
            instance.run_case(self.jobs('admission_options')[0])

    def test_guest_rejects_unowned_wrong_host_and_case(self):
        job = self.jobs('nvmeof_write_ceiling')[0]
        config = {'spec': {'mode': 'focused'}}
        for index, quiescent, case in ((-1, True, job['case_id']), (0, False, job['case_id']), (0, True, 'wrong')):
            with self.assertRaises(cloud_controller.SafetyError):
                fleet_tools.guest(config, dict(job=job, index=index, quiescent=quiescent,
                    case_id=case, action='fleet_tools_target_setup'), cloud_controller)

    def test_target_collision_never_creates_scratch(self):
        job = self.jobs('nvmeof_write_ceiling')[0]
        config = dict(repository_root='/repo', prefix='bench-test', fleet={'nodes': [{'rdma_address': '10.0.0.2'}]})
        with tempfile.TemporaryDirectory() as tmp:
            manager = MagicMock(); manager.CONFIG = Path(tmp)
            expected = fleet_tools.target_identity(config, job)
            (manager.CONFIG / 'subsystems' / expected['nqn']).mkdir(parents=True)
            with patch.object(fleet_tools, 'manager', return_value=manager), \
                    patch.object(cloud_controller, 'provision_fio_scratch') as provision:
                with self.assertRaises(cloud_controller.SafetyError):
                    fleet_tools.target_setup(config, job, Path(tmp) / 'case', cloud_controller)
                provision.assert_not_called()

    def test_controller_receipt_raw_binding_and_archive_analysis(self):
        job = self.jobs('admission_options')[0]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctl = self.controller(root)
            raw = self.admission_raw(job['admission'])
            result = dict(schema_version=1, case_id=job['case_id'],
                          **fleet_tools.admission_records(raw, job['admission']),
                          command=fleet_tools.admission_argv('/sealed/admission-options', root / 'cases' / job['case_id'], job['admission']),
                          binary_sha256='b' * 64,
                          raw_path='admission-options.jsonl', raw_sha256=hashlib.sha256(raw).hexdigest())
            def execute(controller, job, directory, api, deadline):
                (directory / 'admission-options.jsonl').write_bytes(raw)
                cloud_controller.dump(directory / 'admission_command.json', result['command'])
                return result
            with patch.object(fleet_tools, 'run_admission', side_effect=execute):
                receipt = fleet_tools.run_case(ctl, job, cloud_controller)
            self.assertEqual(receipt['status'], 'completed')
            ctl.select_monitoring.assert_called_once()
            ctl.finish_monitoring.assert_called_once()
            directory = root / 'cases' / job['case_id']
            summary = cloud_campaign.summarize_case(directory)
            rows = fleet_tools.analysis_rows([summary])['admission_options']
            self.assertTrue(rows)
            self.assertEqual(rows[0]['repeat_count'], 1)
            self.assertIsNone(rows[0]['timed_metric_sample_variance']['batches_s'])
            self.assertEqual(rows[0]['interpretation'], fleet_tools.CAVEATS)
            (root / 'matrix.json').write_text(json.dumps({'jobs': [job]}))
            report = root / 'report'; report.mkdir()
            (report / 'comparison.json').write_text(json.dumps({'cases': [summary]}))
            focused_analysis.generate(root, report, root / 'analysis')
            self.assertTrue((root / 'analysis' / 'admission_options.csv').is_file())
            (directory / 'admission-options.jsonl').write_bytes(raw + b' ')
            with self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(directory, job, receipt, completed=True)

    def test_run_to_run_variance_does_not_pool_repetitions(self):
        rows = [dict(status='completed', option='D', batches_s=v, p50_ns=v) for v in (10, 20, 30)]
        fleet_tools.repeat_variance(rows, ('option',), ('batches_s', 'p50_ns'))
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(r['repeat_count'] == 3 for r in rows))
        self.assertTrue(all(r['timed_metric_sample_variance']['batches_s'] == 100 for r in rows))
        self.assertTrue(all('no warmup calls included' in r['variance_population'] for r in rows))

    def test_fio_lifecycle_archives_raw_cpu_and_cleans_after_failure(self):
        job = self.jobs('nvmeof_write_ceiling')[-1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctl = self.controller(root)
            ctl.config.update(prefix='bench-test', repository_root='/repo',
                              fleet={'nodes': [{'rdma_address': '10.0.0.2'}]})
            identity = fleet_tools.target_identity(ctl.config, job)
            target = dict(identity, filesystem='ext4 /dev/nvme0n1',
                          backing_device={'major': 259, 'minor': 0}, region_bytes=fleet_tools.REGION_BYTES,
                          buffered_io=1)
            cpu = dict(available=True, truncated=False, samples=[{'cpus': {'cpu0': {'busy_pct': 90}}}],
                       summary={'max_busy_pct': 90})
            queues = [dict(queue=i, cpu=i, cpu_list=str(i)) for i in range(4)]
            for failed in (False, True):
                directory = root / str(failed); directory.mkdir()
                ctl.request = MagicMock(side_effect=[target, {}, cpu, {'complete': True}])
                raw = self.raw_fio(job['fio_write'])
                with patch.object(fleet_tools, 'connect', return_value=({}, Path('/dev/nvme3n1'), {'verified': True})), \
                        patch.object(fleet_tools, 'queue_cpus', return_value=queues), \
                        patch.object(fleet_tools, 'disconnect', return_value=identity) as disconnect, \
                        patch.object(fleet_tools.shutil, 'which', return_value='/usr/bin/fio'), \
                        patch.object(cloud_controller, 'digest', return_value='b' * 64), \
                        patch.object(cloud_controller, 'floor_client_command',
                                     return_value=(1 if failed else 0, raw.decode())):
                    if failed:
                        with self.assertRaises(cloud_controller.FioFailure):
                            fleet_tools.run_fio(ctl, job, directory, cloud_controller, time.monotonic() + 500)
                    else:
                        result = fleet_tools.run_fio(ctl, job, directory, cloud_controller, time.monotonic() + 500)
                        fleet_tools.validate_result(result, job, directory)
                        self.assertEqual(result['target_buffered_io'], 1)
                        self.assertEqual(result['region_bytes'], 2 * 1024**3)
                        self.assertEqual(result['owner_cpu'], cpu)
                    disconnect.assert_called_once()
                    self.assertEqual(ctl.request.call_args_list[-1].args[1]['action'], 'fleet_tools_target_cleanup')
                    self.assertEqual(ctl.request.call_args_list[-1].args[1]['host_disconnected'], identity)
                self.assertTrue((directory / 'fio_raw.json').is_file())
                self.assertTrue((directory / 'owner-cpu.json').is_file())

    def test_optional_sealed_admission_recipe_and_elf_binding(self):
        command = ['cargo', '--config', 'build.rustflags=["-C","target-cpu=x86-64"]',
                   'build', '--release', '--frozen', '--target', cloud_package.TARGET,
                   '-p', 'kv-research', '--bin', 'kv-research', '--no-default-features',
                   '--features', 'fabric,nvmeof,test-utils']
        expected = command[:command.index('-p')] + ['-p', 'tidehunter', '--example',
                                                    'admission_options', '--no-default-features']
        self.assertEqual(cloud_package.admission_options_command(command), expected)
        self.assertNotIn('--features', expected)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'admission-options'
            self.assertIsNone(cloud_package.admit_admission_options(None, command, path))
            path.write_bytes(b'ELF fixture'); path.chmod(0o755)
            elf = {'identity': {'sha256': 'a' * 64}, 'needed': ['libc.so.6']}
            receipt = dict(build_command=expected, binary=elf)
            with patch.object(cloud_package, 'elf_identity', return_value=elf):
                self.assertEqual(cloud_package.admit_admission_options(receipt, command, path), elf)
                with self.assertRaisesRegex(ValueError, 'unbound'):
                    cloud_package.admit_admission_options(None, command, path)
                bad = copy.deepcopy(receipt); bad['build_command'] += ['--features', 'fabric']
                with self.assertRaises(ValueError):
                    cloud_package.admit_admission_options(bad, command, path)
                bad = copy.deepcopy(elf); bad['needed'] += ['libfabric.so.1']
                with patch.object(cloud_package, 'elf_identity', return_value=bad):
                    with self.assertRaises(ValueError):
                        cloud_package.admit_admission_options(dict(build_command=expected, binary=bad), command, path)

    def test_multi_owner_measurement_groups_reach_analysis(self):
        source = cloud_campaign.make_focused_matrix('nvmeof_f8')[0]
        jobs = [dict(copy.deepcopy(source), group=group, case_id=group + '-test')
                for group in ('G33W', 'G33R', 'G33M')]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); matrix = root / 'matrix.json'
            matrix.write_text(json.dumps({'jobs': jobs}))
            rows = nvmeof_path.generate(matrix=matrix, output=root / 'analysis', settings_only=True)
            self.assertEqual(len(rows), 3)

    def test_fio_completed_archive_analysis_and_pinning_tamper(self):
        job = self.jobs('nvmeof_write_ceiling')[0]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); directory = root / 'cases' / job['case_id']
            directory.mkdir(parents=True)
            raw = self.raw_fio(job['fio_write'])
            (directory / 'fio_raw.json').write_bytes(raw)
            queues = [dict(queue=0, cpu=1, cpu_list='1')]
            command = fleet_tools.fio_argv('/usr/bin/fio', '/dev/nvme3n1', job['fio_write'], queues)
            cloud_controller.dump(directory / 'fio_command.json', command)
            result = dict(schema_version=1, case_id=job['case_id'],
                          **fleet_tools.fio_metrics(raw, job['fio_write']),
                          command=command, device='/dev/nvme3n1', pinned_queues=queues,
                          fio_binary_sha256='b' * 64, target_buffered_io=1,
                          region_bytes=fleet_tools.REGION_BYTES, filesystem='ext4 /dev/nvme0n1',
                          owner_cpu={'available': True, 'summary': {'max_busy_pct': 75}, 'samples': []},
                          raw_path='fio_raw.json', raw_sha256=hashlib.sha256(raw).hexdigest())
            cloud_controller.dump(directory / 'fleet_tools_result.json', result)
            cloud_controller.dump(directory / 'phase_timing.json', dict(schema_version=1, case_id=job['case_id'], spans=[]))
            receipt = dict(job=job, status='completed', binary_blake2b512='a' * 128)
            for name in ('fleet_tools_result', 'phase_timing'):
                path = directory / (name + '.json')
                receipt[name] = dict(path=path.name, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            cloud_controller.dump(directory / 'receipt.json', receipt)
            summary = cloud_campaign.summarize_case(directory)
            (root / 'matrix.json').write_text(json.dumps({'jobs': [job]}))
            report = root / 'report'; report.mkdir()
            (report / 'comparison.json').write_text(json.dumps({'cases': [summary]}))
            focused_analysis.generate(root, report, root / 'analysis')
            self.assertTrue((root / 'analysis/nvmeof_write_ceiling.csv').is_file())
            tampered = copy.deepcopy(result); tampered['pinned_queues'][0]['cpu'] = 2
            with self.assertRaisesRegex(ValueError, 'command'):
                fleet_tools.validate_result(tampered, job, directory)

    def test_admission_failure_is_case_local_and_retains_diagnostics(self):
        job = self.jobs('admission_options')[0]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctl = self.controller(root)
            binary = root / 'runtime/bin/admission-options'
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b'benchmark fixture'); binary.chmod(0o755)
            ctl.config['binary'] = str(binary.with_name('kv-research'))
            with patch.object(cloud_controller, 'floor_client_command',
                              return_value=(2, 'admission invariant failed')) as run:
                receipt = fleet_tools.run_case(ctl, job, cloud_controller)
            self.assertEqual(receipt['status'], 'failed')
            self.assertIn('admission invariant failed', receipt['error'])
            self.assertIn('--skip-codec', run.call_args.args[0])
            directory = root / 'cases' / job['case_id']
            self.assertEqual((directory / 'admission-stdout.log').read_text(), 'admission invariant failed')
            ctl.finish_monitoring.assert_called_once()

    def test_budget_exhaustion_and_cleanup_failure_stop_campaign(self):
        job = self.jobs('admission_options')[0]
        with tempfile.TemporaryDirectory() as tmp:
            ctl = self.controller(tmp); ctl._fleet_tool_seconds = {'admission_options': 1200}
            with patch.object(fleet_tools, 'run_admission') as run:
                with self.assertRaises(cloud_controller.DeadlineError):
                    fleet_tools.run_case(ctl, job, cloud_controller)
                run.assert_not_called()
                receipt = json.loads((ctl.root / 'cases' / job['case_id'] / 'receipt.json').read_text())
                self.assertEqual(receipt['status'], 'failed')
        with self.assertRaises(cloud_controller.DeadlineError):
            fleet_tools.tool_timeout(SimpleNamespace(remaining=lambda: 60), time.monotonic(), 30, cloud_controller)


if __name__ == '__main__':
    unittest.main()
