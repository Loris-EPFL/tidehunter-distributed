import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import cloud_controller
import owner_ftrace
import replay_memory


class ReplayMemoryTests(unittest.TestCase):
    def test_terminal_evidence_requires_drain_and_promotes_an_uncensored_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = root / 'experiments/sui-mainnet/fixture/manifest.json'
            fixture.parent.mkdir(parents=True)
            fixture.write_bytes(b'fixture')
            job = {'case_kind': 'mainnet_replay', 'case_id': 'probe', 'repeat': 0, 'concurrency': 8}
            workload = replay_memory.workload_sha256(job, hashlib.sha256(b'fixture').hexdigest())
            admission = {'workload_sha256': workload, 'cap_bytes': 200, 'qualified_peak_bytes': None}
            service = root / 'client.service'
            service.mkdir()
            (service / 'memory.events').write_text('max 0\nhigh 0\noom 0\n')
            (service / 'memory.current').write_text('150\n')
            (service / 'memory.max').write_text('200\n')
            (service / 'memory.peak').write_text('160\n')
            (root / 'cgroup-memory.stop').touch()
            owner_ftrace.sample_memory(root, service, duration_seconds=1)
            path = root / 'cgroup-memory.json'
            client = json.loads(path.read_text())
            cloud_controller.dump_compact(path, {'client': client, 'owners': [], 'admission': admission,
                                                 'measurement_completed': False})
            binding = {'path': path.name, 'sha256': cloud_controller.digest(path)}
            receipt = {'cgroup_memory_evidence': binding}
            with self.assertRaisesRegex(cloud_controller.InfrastructureError, 'CGROUP_MEMORY_INCOMPLETE'):
                cloud_controller.finalize_replay_memory(root, receipt, admission, False)
            self.assertNotIn('replay_memory', receipt)
            self.assertFalse(json.loads(path.read_text())['measurement_completed'])
            cloud_controller.finalize_replay_memory(root, receipt, admission, True)
            self.assertEqual(receipt['replay_memory']['status'], 'exploratory')
            self.assertEqual(receipt['replay_memory']['peak_bytes'], 160)
            self.assertTrue(json.loads(path.read_text())['measurement_completed'])
            policy = {workload: {'qualified_peak_bytes': 160, 'evidence': [binding]}}
            next_job = dict(job, case_id='qualification', repeat=1)
            self.assertEqual(replay_memory.admit(policy, [next_job], root, 200), {workload: 160})
            with self.assertRaisesRegex(ValueError, '25% headroom'):
                replay_memory.admit(policy, [next_job], root, 199)
            with self.assertRaisesRegex(ValueError, 'invalid replay_memory entry'):
                replay_memory.admit(policy, [dict(next_job, concurrency=4)], root, 200)
            binding['sha256'] = '0' * 64
            with self.assertRaisesRegex(ValueError, 'digest'):
                replay_memory.admit(policy, [next_job], root, 200)

    def test_any_pressure_or_near_cap_peak_censors_a_qualified_run(self):
        admission = {'cap_bytes': 200, 'qualified_peak_bytes': 150}
        for event in replay_memory.PRESSURE_EVENTS + ('peak_within_5_percent_of_cap', None):
            with self.subTest(event=event):
                sample = {'memory.current': 120, 'memory.peak': 130, 'memory.max': 200,
                          'memory.events': {'max': 0, 'high': 0, 'oom': 0}}
                if event == 'peak_within_5_percent_of_cap':
                    sample['memory.peak'] = 190
                elif event:
                    sample['memory.events'][event] = 1
                artifact = {'measurement_completed': True, 'admission': admission,
                            'client': {'available': True, 'samples': [sample]}}
                observed = replay_memory.assess(artifact, admission)
                self.assertEqual(observed['status'], 'censored' if event else 'qualified')
                self.assertEqual(observed['reasons'], [event] if event else [])
        sample['memory.max'] = 'max'
        with self.assertRaisesRegex(ValueError, 'memory.max differs'):
            replay_memory.assess(artifact, admission)

    def test_archive_cli_classifies_land4_q0_without_modifying_saved_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jobs = [{'case_id': f'G37S-{name}', 'case_kind': 'mainnet_replay'}
                    for name in ('capped', 'clear', 'missing', 'tampered')]
            matrix = {'spec': {'mode': 'focused'}, 'jobs': jobs + [
                {'case_id': 'G37W-put', 'case_kind': 'remote_put'}]}
            (root / 'matrix.json').write_text(json.dumps(matrix))
            (root / 'comparison.json').write_text('saved primary report\n')
            for job in jobs:
                directory = root / 'cases' / job['case_id']
                directory.mkdir(parents=True)
                receipt = {'job': job, 'status': 'completed'}
                if job['case_id'] != 'G37S-missing':
                    sample = {'memory.current': 120, 'memory.peak': 125, 'memory.max': 200,
                              'memory.events': {'max': int(job['case_id'] == 'G37S-capped'),
                                                'high': 0, 'oom': 0}}
                    artifact = directory / 'cgroup-memory.json'
                    artifact.write_text(json.dumps({'client': {'available': True, 'samples': [sample]},
                                                    'owners': []}))
                    receipt['cgroup_memory_evidence'] = {'path': artifact.name, 'sha256': cloud_controller.digest(artifact)}
                    if job['case_id'] == 'G37S-tampered':
                        artifact.write_text('{}')
                (directory / 'receipt.json').write_text(json.dumps(receipt))
            before = {p: p.read_bytes() for p in root.rglob('*') if p.is_file()}
            output = subprocess.check_output(
                [sys.executable, '-B', replay_memory.__file__, str(root)], text=True)
            rows = json.loads(output)
            self.assertEqual([row['status'] for row in rows],
                             ['censored', 'valid', 'memory_unqualified', 'memory_unqualified'])
            self.assertEqual(rows[0]['reasons'], ['max'])
            self.assertEqual(rows[1]['peak_bytes'], 125)
            self.assertIn('digest', rows[3]['reason'])
            self.assertEqual({p: p.read_bytes() for p in root.rglob('*') if p.is_file()}, before)
