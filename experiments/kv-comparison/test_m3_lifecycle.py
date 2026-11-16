"""Exercise the fleet capsule boundary and the admission authority handoff."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml
import cloud_campaign as campaign
import cloud_controller as cloud
import local_multiowner_check as multiowner
from test_cloud_controller import config_fixture

class KernelLogTests(unittest.TestCase):
    def test_failed_capture_returns_evidence_instead_of_raising(self):
        errors = []
        with patch.object(multiowner.subprocess, 'check_output', side_effect=
                subprocess.CalledProcessError(1, ['dmesg'], output='permission denied')):
            self.assertIsNone(multiowner.capture_kernel_log(Path('/unused/dmesg-after.txt'), errors))
        self.assertIn('dmesg-after.txt', errors[0])
        self.assertIn('permission denied', errors[0])

    def test_rotation_of_older_messages_preserves_the_run_interval(self):
        before = '<6>[1.000000] old\n<6>[2.000000] boundary\n'
        after = '<6>[2.000000] boundary\n<6>[3.000000] new controller\n'
        interval = multiowner.kernel_log_interval(before, after)
        self.assertTrue(interval['complete_interval'])
        self.assertEqual(interval['boundary_monotonic_ns'], 2_000_000_000)
        self.assertEqual(interval['io_errors_or_controller_recovery'], [])

    def test_detects_recovery_and_io_errors_only_after_the_boundary(self):
        before = '[1.000000] Buffer I/O error from an older run\n[2.000000] boundary\n'
        after = before + '[3.000000] nvme nvme2: starting error recovery\n' + (
            '[3.100000] nvme nvme2: Property Set error: 880\n'
            '[3.200000] Buffer I/O error on dev nvme2n1\n')
        interval = multiowner.kernel_log_interval(before, after)
        self.assertTrue(interval['complete_interval'])
        self.assertEqual(len(interval['io_errors_or_controller_recovery']), 3)

    def test_rotation_past_the_boundary_is_reported_as_missing_evidence(self):
        interval = multiowner.kernel_log_interval('[2.000000] boundary\n', '[3.000000] newest\n')
        self.assertFalse(interval['complete_interval'])
        self.assertFalse(multiowner.kernel_log_interval(None, '[3.000000] newest\n')['complete_interval'])



class CapsuleLayoutTests(unittest.TestCase):
    def test_colocated_owners_extract_into_independent_fresh_directories(self):
        # Use the real producer and guest consumer. Only cloud IO and systemd
        # are replaced; archive paths, extraction and launch validation are real.
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root = Path(temporary)
            config = config_fixture()
            config['fleet']['nodes'] = config['fleet']['nodes'][:1]
            config['fleet']['data_root'] = str(root)
            job = {'case_id': 'capsule-layout', 'backend': 'tidehunter_tcp_trusted',
                   'owner_count': 2, 'owner_topology': 'colocated', 'keys': 2,
                   'warmup_operations': 0, 'measurement_operations': 2,
                   'coordinator_cache_bytes': 0}
            runtime = cloud.Controller.__new__(cloud.Controller)
            runtime.config, runtime.clients = config, {}
            runtime.sequence_lock = threading.Lock()
            runtime.remaining = lambda: 300
            archives = {}
            guarded = cloud.guarded
            stack.enter_context(patch.object(cloud, 'guarded', side_effect=lambda path, base, **kw:
                guarded(path, root if base == Path('/mnt/tidehunter-kv-comparison') else base, **kw)))
            stack.enter_context(patch.object(cloud, 'case_owner_job', return_value=job))
            stack.enter_context(patch.object(cloud, 'verify_local_identity', return_value={}))
            stack.enter_context(patch.object(cloud, 'configure_owner_slice', return_value=None))
            stack.enter_context(patch.object(cloud, 'unit_status', return_value={'ActiveState': 'active', 'MainPID': '1'}))
            stack.enter_context(patch.object(cloud, 'systemd_argv', side_effect=lambda *args, **kw:
                ['start-owner', args[6][-1]]))
            stack.enter_context(patch.object(cloud, 'owner_durability_receipt', return_value={}))
            stack.enter_context(patch.object(cloud, 'confirm_owner_durability',
                side_effect=lambda proof, tier, index, nonce: {'partition': index}))
            stack.enter_context(patch.object(cloud, 'environment', return_value={}))
            def upload(clients, cfg, key, archive):
                saved = root / ('saved-' + archive.name)
                shutil.copyfile(archive, saved)
                archives[key] = saved
            stack.enter_context(patch.object(cloud, 'upload_object', side_effect=upload))
            stack.enter_context(patch.object(cloud, 'download_object', side_effect=lambda clients, cfg, key, dest:
                shutil.copyfile(archives[key], dest)))
            def command(argv, **kwargs):
                if argv[0] == 'start-owner':
                    launch = yaml.safe_load(Path(argv[1]).read_text())
                    Path(launch['ready_file']).write_text(launch['listen'])
                    return ''
                prepared = Path(argv[-1])
                prepared.mkdir()
                geometry = {'fragment_bytes': 4194304, 'wal_file_bytes': 1073741824}
                (prepared / 'remote.json').write_text(json.dumps({'tls': {}, 'storage': geometry}))
                (prepared / 'external_owner_read_config.json').write_text('[]')
                for index in range(2):
                    node = root / 'cells' / job['case_id'] / f'node-{index}'
                    launch = {'ready_file': str(node / 'ready'), 'stop_file': str(node / 'stop'),
                              'node': {'root': str(node / 'storage'), 'partition': geometry},
                              'listen': '10.0.0.2:' + str(9400 + index),
                              'limits': {'polling': 'adaptive'},
                              'tls': {'transport': campaign.TRANSPORTS[job['backend']], 'ca': '', 'cert': '', 'key': ''}}
                    source = prepared / f'node-{index}'
                    source.mkdir()
                    (source / 'launch.yaml').write_text(yaml.safe_dump(launch))
                return ''
            stack.enter_context(patch.object(cloud, 'command', side_effect=command))
            directory = root / 'results' / 'case'
            guest_lock = threading.Lock()
            def request(index, payload, **kwargs):
                with guest_lock:
                    return cloud.guest_operation(config, {**payload, 'owner_index': index, 'index': 0}, {})
            runtime.request = request
            directory.mkdir(parents=True)
            runtime.prepare_tide(job, directory)
            for index in range(2):
                node = root / 'cells' / job['case_id'] / f'node-{index}'
                self.assertTrue((node / 'launch.yaml').is_file())
                self.assertTrue((node / 'ready').is_file())
            # Duplicate starts must still fail closed instead of overwriting.
            archive = archives[next(iter(archives))]
            with self.assertRaisesRegex(cloud.SafetyError, 'storage capsule requires a fresh directory'):
                cloud.safe_extract_capsule(archive, node, ['launch.yaml'])


@unittest.skipUnless(os.environ.get('TIDEHUNTER_LOCAL_DIAGNOSTIC_BINARY'),
                     'set TIDEHUNTER_LOCAL_DIAGNOSTIC_BINARY for real admission config validation')
class AdmissionConfigTests(unittest.TestCase):
    def test_every_m3_job_validates_with_real_binary(self):
        binary = Path(os.environ['TIDEHUNTER_LOCAL_DIAGNOSTIC_BINARY']).resolve(strict=True)
        class Validated(Exception):
            pass
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for selection in campaign.M3_SELECTIONS:
                for job in campaign.make_focused_matrix(selection):
                    with self.subTest(selection=selection, case=job['case_id']):
                        directory = root / job['case_id']
                        directory.mkdir()
                        prepared = directory / 'prepared'
                        count = job['owner_count']
                        cfg = config_fixture()
                        cfg['spec'].update(mode='focused', selection=selection,
                                           backends=['tidehunter_nvme_rdma'])
                        live = cloud.live_nvme_preparation(cfg, job['case_id'], 'rdma')
                        live.update(write_mode=job['live_write_mode'],
                                    export_mode=job['live_export_mode'],
                                    inline_reads=job['live_inline_reads'],
                                    lease_ahead=job['live_lease_ahead'],
                                    polled_writes=job['live_polled_writes'],
                                    queue_geometry={'io_queues': job['live_io_queues'],
                                        'queue_size': job['live_io_queue_size'],
                                        'poll_queues': job['live_poll_queues']})
                        prep = {'guest_root': str(prepared),
                            'addresses': [host['address'] + ':' + str(9400 + i)
                                for i, host in enumerate(cloud.logical_owner_hosts(cfg, job))],
                            'transport': cloud.owner_control_settings(job)['owner_transport'],
                            'durability_tier': campaign.durability_tier(job),
                            'owner_wal_geometry': job['owner_wal_geometry'],
                            'max_batches_per_partition': max(16384, cloud.planned_write_batches_per_partition(job)),
                            'max_admitted_batches': 1000000, 'max_index_bytes': 512 * 1024**2,
                            'coordinator_cache_bytes': job['coordinator_cache_bytes'],
                            'owner_read_workers': 4, 'owner_wal_member_read': 'async',
                            'owner_read_memory_allowance_bytes': 128 * 1024**2,
                            'polling': job['owner_polling'], 'history_persistence': job['history_persistence'],
                            'owner_read_resident_first': job.get('owner_read_resident_first', False),
                            'live_nvmeof': live}
                        (directory / 'prepare.json').write_text(json.dumps(prep))
                        result = subprocess.run([str(binary), '--prepare-external-case',
                            str(directory / 'prepare.json'), '--output', str(prepared)],
                            capture_output=True, text=True, timeout=60)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        remote = json.loads((prepared / 'remote.json').read_text())
                        remote['read_lanes'] = job['read_lanes']
                        options = {'path': str(directory / 'database'), 'owners': count,
                            'key_count': max(1048576, job['keys']),
                            'max_batches_per_partition': prep['max_batches_per_partition'],
                            'coordinator_read_cache_bytes': job['coordinator_cache_bytes'],
                            'owner_read_resident_first': False, 'external_remote_config': remote,
                            'durability_tier': prep['durability_tier'],
                            'external_owner_read_config': json.loads(
                                (prepared / 'external_owner_read_config.json').read_text()),
                            'external_owner_durability_receipts': [
                                str(prepared / f'node-{i}/ready.durability.json') for i in range(count)]}
                        def validate(argv, **kwargs):
                            result = subprocess.run([str(binary), '--config', str(argv[2]),
                                '--validate-config'], capture_output=True, text=True, timeout=60)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            admitted = json.loads(result.stdout)['admitted_config']
                            self.assertFalse(admitted['phases']['warmup_key_sweep'])
                            self.assertTrue(admitted['phases']['measurement_key_sweep'])
                            self.assertIsNone(admitted['measurement_gate'])
                            self.assertIsNone(admitted['measurement_completion_gate'])
                            print('M3 admission validated:', selection, job['case_id'], flush=True)
                            raise Validated
                        runtime = SimpleNamespace(config={'binary': str(binary),
                            'source_revision': 'a' * 40}, clock=time.monotonic, remaining=lambda: 3600)
                        runtime.run_logical_owner_admission = lambda *args: (
                            cloud.Controller.run_logical_owner_admission(runtime, *args))
                        with patch.object(cloud, 'command', side_effect=validate), patch.object(
                                cloud, 'environment', return_value=dict(os.environ)), self.assertRaises(Validated):
                            cloud.Controller.admit_logical_owners(runtime, job, directory, options)


@unittest.skipUnless(os.environ.get('TIDEHUNTER_LOCAL_DIAGNOSTIC_BINARY'),
                     'set TIDEHUNTER_LOCAL_DIAGNOSTIC_BINARY to exercise real TCP owners')
class AuthorityHandoffTests(unittest.TestCase):
    def test_admission_then_second_coordinator_open_with_live_owners(self):
        binary = Path(os.environ['TIDEHUNTER_LOCAL_DIAGNOSTIC_BINARY']).resolve(strict=True)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            guest = root / 'guest'
            sockets, processes, logs = [], [], []
            try:
                for _ in range(5):
                    listener = socket.socket()
                    listener.bind(('127.0.0.1', 0))
                    sockets.append(listener)
                prep = {'guest_root': str(guest), 'addresses': [
                    '127.0.0.1:' + str(listener.getsockname()[1]) for listener in sockets],
                    'transport': {'kind': 'tcp'}, 'durability_tier': 'synchronous',
                    'owner_wal_geometry': 'research', 'owner_wal_file_bytes': 8388608,
                    'max_batches_per_partition': 16384, 'max_admitted_batches': 16384,
                    'max_index_bytes': 128 * 1024**2, 'coordinator_cache_bytes': 0,
                    'owner_read_workers': 2, 'owner_wal_member_read': 'async',
                    'owner_read_memory_allowance_bytes': 128 * 1024**2, 'polling': 'adaptive'}
                (root / 'prepare.json').write_text(json.dumps(prep))
                subprocess.run([str(binary), '--prepare-external-case', str(root / 'prepare.json'),
                    '--output', str(guest)], check=True, capture_output=True, timeout=60)
                prepared = guest
                remote = json.loads((prepared / 'remote.json').read_text())
                for index, listener in enumerate(sockets):
                    launch = yaml.safe_load((prepared / f'node-{index}/launch.yaml').read_text())
                    launch['launch_nonce'] = 'authority-handoff-' + str(index)
                    launch_file = guest / f'node-{index}/launch.yaml'
                    launch_file.write_text(yaml.safe_dump(launch))
                    listener.close()
                    log = (root / f'owner-{index}.log').open('wb')
                    logs.append(log)
                    processes.append(subprocess.Popen([str(binary), '--experimental-storage-node', str(launch_file)],
                        stdout=log, stderr=subprocess.STDOUT))
                deadline = time.monotonic() + 30
                for index, process in enumerate(processes):
                    while not (guest / f'node-{index}/ready').is_file():
                        if process.poll() is not None or time.monotonic() >= deadline:
                            self.fail('owner did not become ready: ' + (root / f'owner-{index}.log').read_text())
                        time.sleep(.01)
                case = root / 'case'
                case.mkdir()
                options = {'path': str(case / 'database'), 'evidence': str(case / 'read-evidence'),
                    'owners': 5, 'key_count': 1048576, 'max_batches_per_partition': 16384,
                    'coordinator_read_cache_bytes': 0, 'external_remote_config': remote,
                    'external_owner_read_config': json.loads((prepared / 'external_owner_read_config.json').read_text()),
                    'external_owner_durability_receipts': [str(guest / f'node-{i}/ready.durability.json') for i in range(5)],
                    'durability_tier': 'synchronous'}
                job = {'case_id': 'authority-handoff', 'backend': 'tidehunter_tcp', 'repeat': 0,
                    'owner_count': 5, 'keys': 32, 'value_size': 1024, 'mix': campaign.mix(read=100),
                    'concurrency': 2, 'warmup_operations': 0, 'measurement_operations': 32,
                    'offered_ops_per_second': None, 'distribution': 'uniform', 'durability_tier': 'synchronous'}
                def run(job, directory, config):
                    # TCP has no NVMe route inventory. Keep the fleet's admission
                    # config/path producer, but execute the real engine directly.
                    config.update(measurement_gate=None, measurement_completion_gate=None)
                    config['dataset'].update(keys=32)
                    target = directory / 'owner-admission'
                    (target / 'config.json').write_text(json.dumps(config))
                    subprocess.run([str(binary), '--config', str(target / 'config.json'), '--output', str(target / 'result')],
                        check=True, capture_output=True, timeout=60)
                runtime = SimpleNamespace(config={'binary': str(binary), 'source_revision': 'a' * 40},
                    run_logical_owner_admission=run)
                cloud.Controller.admit_logical_owners(runtime, job, case, options)
                second = campaign.make_run_config(job, case, options, 'a' * 40, 60)
                second.update(measurement_gate=None, measurement_completion_gate=None)
                (case / 'config.json').write_text(json.dumps(second))
                result = subprocess.run([str(binary), '--config', str(case / 'config.json'), '--output', str(case / 'result')],
                    capture_output=True, text=True, timeout=60)
                self.assertEqual(result.returncode, 0, result.stderr)
                summary = json.loads((case / 'result/summary.json').read_text())
                self.assertEqual(summary['status'], 'completed')
            finally:
                for index, process in enumerate(processes):
                    (guest / f'node-{index}/stop').touch()
                for process in processes:
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                for listener in sockets:
                    listener.close()
                for log in logs:
                    log.close()


if __name__ == '__main__':
    unittest.main()
