import csv
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import remote_rw


def make_job(name, group, backend, size, readers, repeat=0, **extra):
    return {'case_id': name, 'group': group, 'backend': backend, 'value_size': size,
            'concurrency': readers, 'repeat': repeat, 'keys': 65536,
            'key_order': 'random_with_replacement', 'distribution': 'uniform',
            'measurement_operations': 4, 'warmup_operations': 0, 'mix': {'read': 100},
            'coordinator_cache_bytes': 0, 'sealed_dataset': {'tier': 'first_touch'},
            'media': {'expected': 'cold'}, 'read_lanes': 0 if backend == 'native' else (4 if readers == 16 else 1),
            'durability_tier': 'synchronous', **extra}


def make_case(job, values=(1000, 2000, 3000, 9000), status='completed', device=1.0):
    ordered = sorted(values)
    stats = {'p50': ordered[math.ceil(len(ordered) * .5) - 1],
             'p99': ordered[math.ceil(len(ordered) * .99) - 1]}
    return {'case_id': job['case_id'], 'job': job, 'status': status,
            'metrics': {'success_calls_per_second': len(values) / 2,
                        'populations': [{'operation': 'read', 'outcome': 'success', 'count': len(values),
                                         'api_mean_ns': sum(values) / len(values), 'api_ns': stats}]},
            'focused_evidence': {'media_evidence': {'status': 'qualified' if device >= .9 else 'unqualified',
                                                     'read_ops_per_success': device, 'reason_codes': []}}}


class RemoteRwTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name); self.export = self.root / 'export'; self.report = self.root / 'report'; self.output = self.root / 'out'
        self.export.mkdir(); self.report.mkdir()

    def publish(self, cases):
        (self.export / 'matrix.json').write_text(json.dumps({'jobs': [c['job'] for c in cases]}))
        (self.report / 'comparison.json').write_text(json.dumps({'cases': cases}))

    def test_independent_cases_qualification_and_ratios(self):
        cases = []
        for size in (1024, 8192):
            for readers in (1, 5):
                for backend, short in [('native', 'n'), ('tidehunter_tcp', 't'), ('tidehunter_verbs_native', 'v')]:
                    name = f'{short}{size}-{readers}'
                    status = 'failed' if name == 't8192-5' else 'not_run' if name == 'v8192-5' else 'completed'
                    device = .5 if name == 'v1024-5' else 1.0
                    cases.append(make_case(make_job(name, 'G15C', backend, size, readers), status=status, device=device))
        cases.append(make_case(make_job('n-repeat', 'G15C', 'native', 1024, 1, repeat=1), values=(10000, 20000)))
        cases.append(make_case(make_job('t-repeat', 'G15C', 'tidehunter_tcp', 1024, 1, repeat=1), values=(20000, 40000)))
        cases.append(make_case(make_job('v-repeat', 'G15C', 'tidehunter_verbs_native', 1024, 1, repeat=1), values=(20000, 40000)))
        cases[-1]['job']['keys'] = 7
        for backend, short in [('native', 'n'), ('tidehunter_tcp', 't'), ('tidehunter_verbs_native', 'v')]:
            cases.append(make_case(make_job('warm-' + short, 'G15M', backend, 1024, 1)))
        self.publish(cases)
        # A complete three-arm validator: the production gate calls the shared validator by name.
        def validator(jobs, *, complete=True):
            return [] if len(jobs) == 3 and len({j['keys'] for j in jobs}) == 1 else ['contract mismatch']
        with patch.object(remote_rw.cloud_campaign, 'parity_violations', validator, create=True):
            remote_rw.generate(self.export, self.report, self.output)
        with (self.output / 'cold_vs_warm.csv').open(newline='') as stream:
            rows = {r['case_id']: r for r in csv.DictReader(stream)}
        self.assertEqual(rows['n1024-1']['n'], '4')
        self.assertEqual(rows['n1024-1']['mean_us'], '3.75')
        self.assertEqual(rows['n1024-1']['p50_us'], '2.0')
        self.assertEqual(rows['n1024-1']['p99_us'], '9.0')
        self.assertEqual(rows['n1024-1']['calls_per_second'], '2.0')
        self.assertEqual(rows['n-repeat']['mean_us'], '15.0')
        self.assertEqual(rows['warm-n']['status'], 'completed')
        self.assertEqual(rows['n1024-1']['read_lanes'], '0')
        self.assertEqual(rows['t1024-5']['read_lanes'], '1')
        self.assertEqual(rows['t1024-5']['durability_tier'], 'synchronous')
        self.assertEqual(rows['v1024-5']['status'], 'unqualified')
        self.assertEqual(rows['v1024-5']['mean_us'], '')
        self.assertEqual(rows['t8192-5']['status'], 'failed')
        self.assertEqual(rows['v8192-5']['status'], 'not_run')
        text = (self.output / 'remote_rw.md').read_text()
        self.assertIn('device reads per call', text)
        self.assertIn('contract mismatch', text)
        self.assertIn('| t-repeat | n-repeat | unmatched | — |', text)
        self.assertIn('separate populations', text)
        self.assertEqual(text.count('Owner read lanes: 1 at 1 and 5 readers, 4 at 16 readers'), 3)
        for i, line in enumerate(text.splitlines()):
            if line.startswith('| ') and i + 1 < len(text.splitlines()) and text.splitlines()[i + 1].startswith('| ---'):
                caption = text.splitlines()[i - 1]
                for phrase in ('durable mechanics', 'sync scope', 'network in the data path', 'index', 'routing', 'instances and SSD count'):
                    self.assertIn(phrase, caption)
        for family in ('latency_vs_size', 'latency_vs_concurrency', 'throughput_vs_concurrency', 'cold_vs_warm', 'floors', 'parity_matrix'):
            with (self.output / (family + '.csv')).open(newline='') as stream:
                self.assertEqual(csv.DictReader(stream).fieldnames[-4:],
                                 ['read_lanes', 'native_shards', 'durability_tier', 'native_durability'])

    def test_write_and_floor_receipts(self):
        common = {'keys': 4096, 'mix': {'put': 100}, 'measurement_operations': 4,
                  'warmup_operations': 0, 'distribution': 'uniform', 'concurrency': 1,
                  'value_size': 1024, 'repeat': 0}
        n = dict(common, case_id='wn', group='G14N', backend='native',
                 native_durability='value_wal_barrier', native_shards=1)
        o = dict(common, case_id='wo', group='G14O1', backend='tidehunter_tcp',
                 durability_tier='synchronous', owner_count=1)
        n['durability_tier'] = 'value_wal_barrier'
        cases = [make_case(n), make_case(o)]
        for mode in ('page_cache', 'value_wal_group_barrier'):
            cases.append(make_case(dict(n, case_id='wn-' + mode, native_durability=mode, durability_tier=mode)))
        cases.append(make_case(dict(o, case_id='wv', backend='tidehunter_verbs_native')))
        for case in cases:
            case['metrics']['populations'][0]['operation'] = 'put'
        spec = {'host': 'client', 'mode': 'append_mmap', 'block_bytes': 1024}
        job = {'case_id': 'floor', 'group': 'G13W', 'repeat': 0, 'ssd_write': spec}
        cases.append({'case_id': 'floor', 'job': job, 'status': 'completed',
                      'focused_evidence': {'ssd_write_floor_result': {
                          'total': {'mean': 3000, 'p50': 2000, 'p99': 9000},
                          'logged_operations': 4, 'total_operations': 4, 'elapsed_seconds': 2}}})
        net = {'tool': 'tcp_rtt', 'interface': 'eth0', 'sizes': [1024], 'mode': 'block'}
        job = {'case_id': 'net', 'group': 'G11N', 'repeat': 0, 'net': net}
        cases.append({'case_id': 'net', 'job': job, 'status': 'completed',
                      'focused_evidence': {'net_floor_result': {'runs': [{
                          'size_bytes': 1024, 'status': 'completed', 'latency_semantics': 'round_trip',
                          'latency_us': {'avg': 5, 'p50': 4, 'p99': 8}}]}}})
        self.publish(cases)
        remote_rw.generate(self.export, self.report, self.output)
        text = (self.output / 'remote_rw.md').read_text()
        self.assertIn('| wo | wn | matched | 1.0 |', text)
        with (self.output / 'parity_matrix.csv').open(newline='') as stream:
            writes = {r['case_id']: r for r in csv.DictReader(stream)}
        self.assertEqual(writes['wn']['native_shards'], '1')
        self.assertEqual(writes['wo']['durability_tier'], 'synchronous')
        self.assertEqual({writes[name]['native_durability'] for name in
                          ('wn', 'wn-page_cache', 'wn-value_wal_group_barrier')},
                         {'value_wal_barrier', 'page_cache', 'value_wal_group_barrier'})
        with (self.output / 'floors.csv').open(newline='') as stream:
            floors = {r['case_id']: r for r in csv.DictReader(stream)}
        self.assertEqual(floors['floor']['arm'], 'client')
        self.assertEqual(floors['floor']['mean_us'], '3.0')
        self.assertEqual(floors['floor']['calls_per_second'], '2.0')
        self.assertEqual(floors['net']['mean_us'], '5')
        self.assertIn('round_trip', floors['net']['floor_contract'])

    def test_shared_validator_with_reviewed_read_jobs(self):
        jobs = [j for j in remote_rw.cloud_campaign._focused_jobs()
                if j.get('group') in ('G15C', 'G15M') and j.get('value_size') == 1024
                and j.get('concurrency') == 1 and j.get('repeat') == 0]
        self.assertEqual(len(jobs), 6)
        self.assertEqual(remote_rw.cloud_campaign.parity_violations(jobs, complete=False), [])
        cases = [make_case(j) for j in jobs]
        self.publish(cases)
        remote_rw.generate(self.export, self.report, self.output)
        self.assertIn('Parity gate: 4 matched pairs; 0 missing or unmatched partners',
                      (self.output / 'remote_rw.md').read_text())
        cases[1]['job']['keys'] += 1
        self.publish(cases)
        remote_rw.generate(self.export, self.report, self.output)
        self.assertIn('unmatched', (self.output / 'remote_rw.md').read_text())

    def test_full_matrix_validation_is_reported(self):
        case = make_case(make_job('n', 'G15C', 'native', 1024, 1))
        self.publish([case])
        (self.export / 'matrix.json').write_text(json.dumps({
            'spec': {'selection': 'remote_rw_compact'}, 'jobs': [case['job']]}))
        with patch.object(remote_rw.cloud_campaign, 'parity_violations',
                          return_value=['missing grid']) as validator:
            remote_rw.generate(self.export, self.report, self.output)
        validator.assert_called_once_with([case['job']])
        self.assertIn('Full matrix parity: 1 violations: missing grid',
                      (self.output / 'remote_rw.md').read_text())

    def test_matrix_integrity(self):
        case = make_case(make_job('n', 'G15C', 'native', 1024, 1))
        self.publish([case])
        (self.report / 'comparison.json').write_text(json.dumps({'cases': []}))
        with self.assertRaisesRegex(ValueError, 'do not match'):
            remote_rw.generate(self.export, self.report, self.output)


if __name__ == '__main__':
    unittest.main()
