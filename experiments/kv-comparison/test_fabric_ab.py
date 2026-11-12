import csv
import json
import math
from pathlib import Path
import tempfile
import unittest

import cloud_campaign
import fabric_ab


class FabricAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.export = self.root / 'export'; self.report = self.root / 'report'; self.out = self.root / 'out'
        self.export.mkdir(); self.report.mkdir()
        self.jobs = cloud_campaign.make_focused_matrix('fabric_ab_compact')
        self.assertEqual(len(self.jobs), 80)
        self.cases = [{'case_id': j['case_id'], 'job': j, 'status': 'not_run'} for j in self.jobs]

    def find(self, group, arm, size=1024, readers=1, repeat=0):
        return next(c for c in self.cases if c['job']['group'] == group and fabric_ab._arm(c['job']) == arm
                    and c['job']['value_size'] == size and c['job']['concurrency'] == readers and c['job']['repeat'] == repeat)

    def complete(self, case, values, *, device=1.0, nvme_ok=True, cq=None, cpu=True):
        j = case['job']; case['status'] = 'completed'
        values = sorted(values); n = len(values)
        case['metrics'] = {'success_calls_per_second': n / 2, 'populations': [{
            'operation': 'read', 'outcome': 'success', 'count': n, 'api_mean_ns': sum(values) / n,
            'api_ns': {'p50': values[math.ceil(.5 * n) - 1], 'p99': values[math.ceil(.99 * n) - 1]}}]}
        case['backend'] = {'configuration': {'transport_security': 'mutual_tls' if j['backend'] == 'tidehunter_tcp' else 'trusted_network_no_tls'}}
        if j['group'] == 'G16C':
            case['focused_evidence'] = {'media_evidence': {'status': 'qualified' if device >= .9 else 'unqualified',
                                         'read_ops_per_success': device, 'reason_codes': []}}
            if j['backend'] in ('tidehunter_nvme_rdma', 'tidehunter_nvme_tcp'):
                case['nvmeof_measurement_qualification'] = {'qualified': nvme_ok, 'reasons': [] if nvme_ok else ['invalid target']}
                case['sealed_read_accounting'] = {'qualified': nvme_ok, 'reasons': [] if nvme_ok else ['invalid accounting']}
        directory = self.export / 'cases' / case['case_id']; (directory / 'result').mkdir(parents=True)
        label = {'tidehunter_tcp': 'tcp', 'tidehunter_tcp_trusted': 'tcp-trusted'}.get(j['backend'], 'ofi-verbs-trusted')
        (directory / 'result' / 'manifest.json').write_text(json.dumps(
            {'backend_info': {'configuration': {'transport': label}}}))
        if cpu:
            (directory / 'resource-interval.json').write_text(json.dumps({'processes': [
                {'process_role': 'client', 'available': True, 'cpu_seconds': 2, 'elapsed_seconds': 4},
                {'process_role': 'storage-0', 'available': True, 'cpu_seconds': 3, 'elapsed_seconds': 4}]}))
        if cq is not None:
            (directory / 'result' / 'measurement-read-evidence.json').write_text(json.dumps(
                {'deltas': {'owner_executor': {'partitions': {'0': {'counters': {'transport.cq_idle_waits': cq}}}}}}))

    def publish(self):
        (self.export / 'matrix.json').write_text(json.dumps({'spec': {'selection': 'fabric_ab_compact'}, 'jobs': [c['job'] for c in self.cases]}))
        (self.report / 'comparison.json').write_text(json.dumps({'cases': self.cases}))

    def csv_rows(self, name):
        with (self.out / (name + '.csv')).open(newline='') as stream:
            return list(csv.DictReader(stream))

    def test_measurements_qualification_pairs_and_repetitions(self):
        for arm in fabric_ab.WARM:
            self.complete(self.find('G16P', arm), [1000, 2000, 3000, 9000], cq=7 if arm == 'verbs-busy' else 0 if arm == 'verbs-adaptive' else None,
                          cpu=arm != 'tcp-mtls-adaptive')
        busy = self.find('G16P', 'verbs-busy')
        (self.export / 'cases' / busy['case_id'] / 'resource-interval.json').write_text(json.dumps({'processes': [
            {'process_role': 'client', 'available': True, 'cpu_seconds': 4, 'elapsed_seconds': 4},
            {'process_role': 'storage-0', 'available': True, 'cpu_seconds': 3.6, 'elapsed_seconds': 4}]}))
        self.complete(self.find('G16P', 'tcp-trusted-adaptive', repeat=1), [10000, 20000])
        for arm in fabric_ab.COLD:
            self.complete(self.find('G16C', arm), [1000, 2000, 3000, 9000], nvme_ok=arm != 'nvmeof-rdma-spdk')
        failed = self.find('G16P', 'verbs-busy', size=65536); failed['status'] = 'failed'; failed['error'] = 'fixture failure'
        self.publish(); fabric_ab.generate(self.export, self.report, self.out)
        warm = {r['arm'] + ':' + r['repetition']: r for r in self.csv_rows('ab_warm') if r['size_bytes'] == '1024' and r['readers'] == '1'}
        self.assertIn('unexpected parking: Busy CQ idle waits nonzero', warm['verbs-busy:0']['polling_check'])
        self.assertEqual(warm['verbs-adaptive:0']['polling_check'], 'suspect: Adaptive CQ idle waits zero')
        self.assertEqual(warm['tcp-mtls-adaptive:0']['client_cores'], '')
        self.assertEqual(warm['tcp-trusted-adaptive:0']['client_cores'], '0.5')
        self.assertEqual(warm['tcp-trusted-adaptive:0']['owner_cores'], '0.75')
        self.assertEqual(warm['tcp-trusted-adaptive:0']['cq_idle_waits'], 'not recorded')
        self.assertEqual(warm['tcp-trusted-adaptive:0']['mean_us'], '3.75')
        self.assertEqual(warm['tcp-trusted-adaptive:0']['p50_us'], '2.0')
        self.assertEqual(warm['tcp-trusted-adaptive:0']['p99_us'], '9.0')
        self.assertEqual(warm['tcp-trusted-adaptive:0']['calls_per_second'], '2.0')
        self.assertEqual(warm['tcp-trusted-adaptive:1']['mean_us'], '15.0')
        pair = next(p for p in self.csv_rows('ab_pairs') if p['comparison'] == 'parking verbs' and
                    p['size_bytes'] == '1024' and p['readers'] == '1' and p['repetition'] == '0')
        self.assertEqual(pair['mean_ratio_a_over_b'], '1.0')
        self.assertEqual(float(pair['client_cores_b_minus_a']), .5)
        self.assertAlmostEqual(float(pair['owner_cores_b_minus_a']), .15)
        cold = next(r for r in self.csv_rows('cold_fabric') if r['arm'] == 'nvmeof-rdma-spdk' and r['size_bytes'] == '1024' and r['readers'] == '1' and r['repetition'] == '0')
        self.assertEqual(cold['status'], 'unqualified'); self.assertEqual(cold['mean_us'], '')
        pairs = self.csv_rows('cold_pairs')
        unqualified = next(p for p in pairs if p['arm_a'] == 'owner-verbs' and p['arm_b'] == 'nvmeof-rdma-spdk' and p['size_bytes'] == '1024' and p['readers'] == '1' and p['repetition'] == '0')
        self.assertEqual(unqualified['mean_ratio_a_over_b'], '')
        good = next(p for p in pairs if p['arm_a'] == 'owner-verbs' and p['arm_b'] == 'nvmeof-rdma-kernel' and p['size_bytes'] == '1024' and p['readers'] == '1' and p['repetition'] == '0')
        self.assertEqual(good['mean_ratio_a_over_b'], '1.0')
        self.assertTrue(any(r['status'] == 'failed' and r['error'] == 'fixture failure' for r in self.csv_rows('ab_warm')))
        self.assertTrue(any(r['status'] == 'not_run' for r in self.csv_rows('ab_warm')))
        lines = (self.out / 'fabric_ab.md').read_text().splitlines()
        for index, line in enumerate(lines):
            if line.startswith('| ') and index + 1 < len(lines) and lines[index + 1].startswith('| ---'):
                for text in ('TLS', 'polling policy', 'user-space spinning', 'kernel busy poll', 'coordinator read driver',
                             'owner RPC versus direct namespace', 'verbs trusted', 'two independent repetitions',
                             'lane count is 1 at 1 reader and 4 at 16', 'sampled separately', 'nvmet workers'):
                    self.assertIn(text, lines[index - 1])

    def test_unmatched_job_refuses_ratio(self):
        for arm in fabric_ab.WARM:
            self.complete(self.find('G16P', arm), [1000, 2000])
        target = self.find('G16P', 'verbs-busy'); target['job']['keys'] += 1
        self.publish(); fabric_ab.generate(self.export, self.report, self.out)
        pairs = [p for p in self.csv_rows('ab_pairs') if p['size_bytes'] == '1024' and p['readers'] == '1' and p['repetition'] == '0']
        self.assertTrue(all(p['parity'] == 'unmatched' and p['mean_ratio_a_over_b'] == '' for p in pairs))

    def test_manifest_transport_mismatch_is_failed(self):
        case = self.find('G16P', 'tcp-trusted-adaptive')
        self.complete(case, [1000, 2000])
        path = self.export / 'cases' / case['case_id'] / 'result' / 'manifest.json'
        path.write_text(json.dumps({'backend_info': {'configuration': {'transport': 'tcp'}}}))
        self.publish(); fabric_ab.generate(self.export, self.report, self.out)
        row = next(r for r in self.csv_rows('ab_warm') if r['case_id'] == case['case_id'])
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(row['security_label'], 'mutual_tls')
        self.assertIn('transport mismatch', row['error'])
        self.assertEqual(row['mean_us'], '')

    def test_busy_low_cpu_is_not_called_consistent(self):
        case = self.find('G16P', 'verbs-busy')
        self.complete(case, [1000, 2000], cq=0)
        path = self.export / 'cases' / case['case_id'] / 'resource-interval.json'
        path.write_text(json.dumps({'processes': [
            {'process_role': 'storage-0', 'available': True, 'cpu_seconds': .05, 'elapsed_seconds': 1}]}))
        self.publish(); fabric_ab.generate(self.export, self.report, self.out)
        row = next(r for r in self.csv_rows('ab_warm') if r['case_id'] == case['case_id'])
        self.assertIn('Busy owner CPU below 0.5 cores', row['polling_check'])
        self.assertNotIn('consistent', row['polling_check'])
        self.assertEqual(row['cq_idle_waits'], '0')

    def test_matrix_report_mismatch_rejected(self):
        self.publish(); (self.report / 'comparison.json').write_text(json.dumps({'cases': []}))
        with self.assertRaisesRegex(ValueError, 'do not match'):
            fabric_ab.generate(self.export, self.report, self.out)


if __name__ == '__main__':
    unittest.main()
