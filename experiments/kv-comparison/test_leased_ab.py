import csv
import json
from pathlib import Path
import tempfile
import unittest

import analyze
import cloud_campaign
import research_report
from unittest.mock import patch
import leased_ab


class LeasedAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.export = self.root / 'export'; self.report = self.root / 'report'; self.out = self.root / 'out'
        self.export.mkdir(); self.report.mkdir()
        self.jobs = []
        self.cases = []

    def add(self, repeat, group='G17W', arm='leased', **changes):
        job = {'case_id': str(len(self.jobs)), 'group': group, 'backend': 'tidehunter_verbs_native' if arm == 'owner RPC' else 'tidehunter_nvme_rdma',
               'durability_tier': 'synchronous', 'value_size': 1024, 'concurrency': 1, 'repeat': repeat,
               'mix': {'put' if group == 'G17W' else 'read': 100}, 'coordinator_cache_bytes': 0, 'cohort': 'primary_rf1'}
        if arm != 'owner RPC':
            job.update(nvmeof_route='live', live_write_mode='leased', live_inline_reads=arm != 'inline off')
        job.update(changes); self.jobs.append(job)
        case = {'case_id': job['case_id'], 'job': job, 'status': 'completed', 'metrics': {'success_calls_per_second': 1, 'measurement_timeline': {'mean_observed_api_concurrency': 1}}}
        self.cases.append(case)
        result = self.export / 'cases' / job['case_id'] / 'result'; result.mkdir(parents=True)
        ack = {('leased', 'synchronous'): 'leased extent: FUA data sync; owner frontier root asynchronous',
               ('leased', 'native'): 'leased extent: page-cache write',
               ('reserved', 'synchronous'): 'reserved frame: owner WAL and frontier root synchronized',
               ('reserved', 'native'): 'reserved frame: owner validated page-cache write'}
        (result / 'manifest.json').write_text(json.dumps({'backend_info': {'configuration': {'live_write': {
            'write_mode': job.get('live_write_mode'), 'inline_reads': job.get('live_inline_reads'),
            'acknowledgement_contract': ack.get((job.get('live_write_mode'), job['durability_tier']))}}}}))
        fields = ['phase', 'operation', 'outcome', 'scheduled_ns', 'dispatched_ns', 'dispatch_observed', 'completed_ns', 'api_ns', 'response_ns', 'scheduling_ns', 'logical_keys', 'bytes', 'error_kind']
        with (result / 'samples.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
            for value in (1000 + repeat * 1000, 3000 + repeat * 1000):
                writer.writerow(dict(phase='measurement', operation=next(iter(job['mix'])), outcome='ok', scheduled_ns=1, dispatched_ns=1, dispatch_observed='true', completed_ns=value + 1, api_ns=value, response_ns=value, scheduling_ns=0, logical_keys=1, bytes=1024, error_kind=''))
        counters = dict(reserve_exchanges=0, complete_exchanges=0, fua_writes=2, leased_frames=2, leased_skip_markers=0,
                        leased_failed_writes=0, leased_short_writes=0, leased_timed_out_writes=0,
                        adoption_async_ns=0, adoption_async_samples=0, adoption_settle_ns=10, adoption_settle_samples=1)
        (result / 'measurement-read-evidence.json').write_text(json.dumps({'live_nvmeof': {'counters': counters}}))
        (result / 'read-diagnostics.json').write_text(json.dumps({'profile': {'counters': {'read_profile_live_inline': 0}}}))
        return result

    def publish(self, selection='leased_ab_compact'):
        (self.export / 'matrix.json').write_text(json.dumps({'spec': {'selection': selection}, 'jobs': self.jobs}))
        (self.report / 'comparison.json').write_text(json.dumps({'cases': self.cases}))

    def test_repetitions_and_unavailable(self):
        self.add(0); self.add(1); self.publish()
        rows = leased_ab.generate(self.export, self.report, self.out)
        self.assertEqual([r['mean_us'] for r in rows], [2, 3])
        self.assertEqual([r['p999_us'] for r in rows], [3, 4])
        self.assertIn('not available', (self.out / 'leased_ab.md').read_text())
        self.assertEqual([r['status'] for r in rows], ['completed', 'completed'])

    def test_contradictions_fail(self):
        for field in ('reserve_exchanges', 'complete_exchanges', 'fua_writes', 'leased_failed_writes', 'leased_short_writes', 'leased_timed_out_writes'):
            result = self.add(0)
            path = result / 'measurement-read-evidence.json'; data = json.loads(path.read_text()); data['live_nvmeof']['counters'][field] = 1; path.write_text(json.dumps(data))
        result = self.add(0, group='G17R', arm='inline off')
        (result / 'read-diagnostics.json').write_text(json.dumps({'profile': {'counters': {'read_profile_live_inline': 1}}}))
        self.publish(); rows = leased_ab.generate(self.export, self.report, self.out)
        self.assertTrue(all(row['status'] == 'failed' for row in rows))

    def test_executed_live_configuration_mismatch_is_failed(self):
        for group, arm, change in (('G17W', 'leased', {'write_mode': 'reserved'}),
                                   ('G17R', 'inline off', {'inline_reads': True}),
                                   ('G17R', 'inline on', {'inline_reads': False})):
            result = self.add(0, group=group, arm=arm)
            path = result / 'manifest.json'
            data = json.loads(path.read_text())
            data['backend_info']['configuration']['live_write'].update(change)
            path.write_text(json.dumps(data))
        self.publish()
        self.assertTrue(all(row['status'] == 'failed'
                            for row in leased_ab.generate(self.export, self.report, self.out)))

    def test_success_inflight_excludes_failed_dispatched_time(self):
        result = self.add(0)
        with (result / 'samples.csv').open('a', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['measurement', 'put', 'error', 1, 1, 'true', 10001, 10000, 10000, 0, 1, 1024, 'error'])
        self.publish()
        row = leased_ab.generate(self.export, self.report, self.out)[0]
        self.assertEqual(row['mean_inflight_calls'], .4)

    def test_long_successful_calls_and_settle_exposure_are_per_case(self):
        result = self.add(0)
        self.add(1)
        with (result / 'samples.csv').open('a', newline='') as stream:
            writer = csv.writer(stream)
            for value, outcome in [(60_000_000, 'success')] * 3 + [(4_000_000, 'success')] * 3 + [(70_000_000, 'error')]:
                writer.writerow(['measurement', 'put', outcome, 1, 1, 'true', value + 1,
                                 value, value, 0, 1, 1024, ''])
        self.publish()
        rows = leased_ab.generate(self.export, self.report, self.out)
        self.assertEqual(rows[0]['calls_ge_5ms'], 3)
        self.assertEqual(rows[0]['time_ge_5ms_share'], f"{180_000_000 / 192_004_000:.3f}")
        self.assertEqual((rows[1]['calls_ge_5ms'], rows[1]['time_ge_5ms_share']), (0, '0.000'))
        with (self.out / 'leased_counters.csv').open(newline='') as stream:
            counters = list(csv.DictReader(stream))
        self.assertEqual(counters[0]['adoption_settle_samples'], '1')
        self.assertEqual(counters[0]['adoption_settle_total_ms'], '1e-05')
        self.assertIn('owner reference-history spills', (self.out / 'leased_ab.md').read_text())

    def test_read_contract_and_preload_are_not_write_acknowledgement(self):
        self.add(0, group='G17R', arm='inline off')
        self.add(0, group='G17R', arm='owner RPC')
        self.publish()
        live, owner = leased_ab.generate(self.export, self.report, self.out)
        self.assertEqual(live['cache_contract'], 'target page cache through kernel NVMe-oF')
        self.assertEqual(live['acknowledgement_contract'], 'not applicable to read')
        self.assertEqual((live['preload_write_mode'], live['preload_durability_tier']), ('leased', 'synchronous'))
        self.assertEqual(owner['cache_contract'], 'owner WAL through its page cache')

    def test_manifest_acknowledgement_contradiction_fails(self):
        result = self.add(0)
        path = result / 'manifest.json'; data = json.loads(path.read_text())
        data['backend_info']['configuration']['live_write']['acknowledgement_contract'] = 'wrong'
        path.write_text(json.dumps(data)); self.publish()
        row = leased_ab.generate(self.export, self.report, self.out)[0]
        self.assertEqual(row['status'], 'failed')
        self.assertIn('acknowledgement contract', row['error'])

    def test_skip_marker_fua_and_native_fua(self):
        for repeat, tier, skips, fua in ((0, 'synchronous', 1, 3), (1, 'synchronous', 1, 2),
                                         (0, 'native', 0, 0), (1, 'native', 0, 1)):
            result = self.add(repeat, durability_tier=tier)
            path = result / 'measurement-read-evidence.json'; data = json.loads(path.read_text())
            data['live_nvmeof']['counters'].update(leased_skip_markers=skips, fua_writes=fua)
            path.write_text(json.dumps(data))
        self.publish()
        rows = leased_ab.generate(self.export, self.report, self.out)
        self.assertEqual([r['status'] for r in rows], ['completed', 'failed', 'completed', 'failed'])
        self.assertIn('leased_skip_markers', (self.out / 'leased_counters.csv').read_text())

    def test_missing_manifest_and_boundary_are_not_zero(self):
        first = self.add(0); (first / 'manifest.json').unlink()
        second = self.add(1); (second / 'measurement-read-evidence.json').unlink()
        self.publish()
        rows = leased_ab.generate(self.export, self.report, self.out)
        self.assertTrue(all(row['status'] == 'failed' for row in rows))
        self.assertIn('unavailable', rows[1]['error'])

    def test_primary_live_route_and_research_classification(self):
        job = next(j for j in cloud_campaign.make_focused_matrix('leased_ab_compact')
                   if j['group'] == 'G17R' and j.get('nvmeof_route') == 'live')
        self.assertEqual(job['cohort'], 'primary_rf1')
        self.assertNotIn('sealed_dataset', job)
        self.assertEqual(analyze.requested_nvmeof_route({'job': job}), 'live')
        run = self.root / 'classification'; run.mkdir()
        (run / 'receipt.json').write_text(json.dumps({'status': 'completed', 'job': job,
            'started_at_unix': 1, 'binary_sha256': 'hash'}))
        (run / 'config.json').write_text(json.dumps({'namespace': 'test'}))
        with patch.object(research_report.analyze, 'boundary', return_value={'route': analyze.requested_nvmeof_route({'job': job})}), \
             patch.object(research_report.analyze, 'analyze', return_value={'per_run_accounting': []}), \
             patch.object(research_report, 'repetition_uncertainty', return_value={}):
            result = research_report.make_report([run])
        self.assertEqual(result['coverage'][0]['classification'], 'performance')

    def test_other_selection_is_rejected(self):
        self.publish('fabric_ab_compact')
        with self.assertRaises(ValueError):
            leased_ab.generate(self.export, self.report, self.out)
