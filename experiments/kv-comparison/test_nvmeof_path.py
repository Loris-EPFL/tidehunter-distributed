import csv
import json
from pathlib import Path
import tempfile
import unittest

import nvmeof_path as analysis


class NvmeofPathTests(unittest.TestCase):
    def test_stalls_join_switch_checkpoint_and_unattributed_calls(self):
        directory = self.add('tidehunter_nvme_rdma', group='G19W')
        samples = []
        records = []
        for index, start in enumerate((0, 10_000_000, 20_000_000)):
            samples.append(dict(caller_sample_id=index, api_ns=6_000_000,
                                dispatch_observed='true', dispatched_ns=start))
            records.append(dict(caller_sample_id=index, batch=dict(epoch=1, sequence=index),
                                events=[dict(stage='caller_start', elapsed_ns=start, owner=0),
                                        dict(stage='caller_finished', elapsed_ns=start + 6_000_000, owner=0)]))
        samples.append(dict(caller_sample_id=3, api_ns=7_000_000,
                            dispatch_observed='true', dispatched_ns=30_000_000))
        records[0]['events'].extend([
            dict(stage='live_extent_switch_started', elapsed_ns=1),
            dict(stage='live_extent_switch_completed', elapsed_ns=5001)])
        timeline = dict(records=records, live_groups=[
            dict(owner=0, batches=[dict(epoch=1, sequence=0)])],
            checkpoint_events=[dict(generation=1, stage='capture_start', elapsed_ns=11_000_000),
                               dict(generation=1, stage='capture_end', elapsed_ns=15_000_000)])
        (directory / 'write-timeline.json').write_text(json.dumps(dict(timeline=timeline)))
        from unittest.mock import patch
        with patch.object(analysis.analyze, 'read_samples', return_value=iter(samples)):
            rows = analysis.stall_rows(self.cases[0], self.export)
        self.assertEqual([row['label'] for row in rows],
                         ['extent_switch', 'checkpoint:capture_start', 'unattributed', 'unattributed'])
        self.assertEqual(rows[0]['switch_us'], 5)
        self.assertEqual([row['first_sample'] for row in rows], [0, 1, 2, 3])
        self.assertEqual([row['calls'] for row in rows], [1, 1, 1, 1])
        self.assertEqual([row['first_measured_call'] for row in rows], [True, False, False, False])

    def test_first_measured_call_uses_dispatch_order_and_exports_g20_tables(self):
        directory = self.add(group='G20N', latency=6_000_000, selection='nvmeof_first_put')
        self.add('tidehunter_nvme_rdma', group='G20R', op='read',
                 selection='nvmeof_first_put')
        path = directory / 'samples.csv'
        with path.open(newline='') as stream:
            reader = csv.DictReader(stream)
            fields = reader.fieldnames
            samples = list(reader)
        for sample, start in zip(samples, (100, 1)):
            sample.update(scheduled_ns=start, dispatched_ns=start,
                          completed_ns=start + int(sample['api_ns']))
        with path.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(samples)
        (directory / 'write-timeline.json').write_text(json.dumps(dict(timeline=dict(records=[]))))
        matrix = self.publish()
        matrix.write_text(json.dumps(dict(spec=dict(selection='nvmeof_first_put'), jobs=self.jobs)))
        rows = analysis.generate(self.export, self.report, self.output)
        self.assertEqual(rows[0]['first_call_us'], 18000)
        with (self.output / 'puts.csv').open() as stream:
            self.assertEqual(next(csv.DictReader(stream))['first_call_us'], '18000.0')
        with (self.output / 'reads.csv').open() as stream:
            self.assertEqual(next(csv.DictReader(stream))['group'], 'G20R')
        with (self.output / 'stalls.csv').open() as stream:
            self.assertEqual([row['first_measured_call'] for row in csv.DictReader(stream)], ['False', 'True'])
        self.assertEqual(analysis.stall_rows(self.cases[1], self.export), [])

    def test_first_measured_call_marks_other_members_of_its_live_group(self):
        from unittest.mock import patch
        directory = self.add('tidehunter_nvme_rdma', group='G20W')
        records = [dict(caller_sample_id=index, batch=dict(epoch=1, sequence=index), events=[])
                   for index in (0, 1)]
        timeline = dict(records=records, live_groups=[
            dict(batches=[dict(epoch=1, sequence=index) for index in (0, 1)], owner=0)])
        (directory / 'write-timeline.json').write_text(json.dumps(dict(timeline=timeline)))
        samples = [dict(caller_sample_id=index, api_ns=1 if index == 0 else 6_000_000,
                        dispatch_observed='true', dispatched_ns=index) for index in (0, 1)]
        with patch.object(analysis.analyze, 'read_samples', return_value=iter(samples)):
            rows = analysis.stall_rows(self.cases[0], self.export)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]['first_measured_call'])

    def test_profile_means_and_history_default(self):
        directory = self.add('tidehunter_nvme_rdma', group='G19W', history_persistence='wal_replay')
        counters = dict(lease_reserve_ns=120, lease_namespace_sync_ns=240,
                        lease_publish_ns=360, lease_profile_samples=3, adopt_owner_ns=80,
                        adopt_close_scan_ns=160, adopt_close_scan_bytes=320,
                        adopt_profile_samples=2, extent_preleases=4, extent_switch_waits=0)
        (directory / 'measurement-read-evidence.json').write_text(
            json.dumps(dict(live_nvmeof=dict(counters=counters))))
        row = analysis.result_row(self.cases[0], self.export)
        self.assertEqual(row['lease_reserve_ns_mean'], 40)
        self.assertEqual(row['adopt_close_scan_bytes_mean'], 160)
        self.assertEqual(row['extent_switch_waits'], 0)
        settings, errors = analysis.settings(self.jobs)
        self.assertEqual(errors, [])
        self.assertNotIn('history_persistence=', settings[0]['differs from engine default'])

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.export = self.root / 'export'; self.export.mkdir()
        self.report = self.root / 'report'; self.report.mkdir()
        self.output = self.root / 'output'
        self.jobs = []; self.cases = []

    def add(self, backend='native', repeat=0, group='G18N', op='put', latency=1000, **changes):
        job = dict(case_id=str(len(self.jobs)), group=group, backend=backend, repeat=repeat,
                   durability_tier='page_cache' if backend == 'native' else 'native',
                   native_durability='page_cache',
                   native_wal_geometry='native_default', native_shards=1, keys=65536,
                   distribution='uniform', value_size=1024, concurrency=1, mix={op: 100})
        if backend != 'native':
            job.update(analysis.REMOTE_DEFAULTS, history_persistence='memory_only')
        if backend == 'tidehunter_nvme_rdma':
            job.update(analysis.LIVE_DEFAULTS, nvmeof_route='live', live_write_mode='leased', live_export_mode='per_owner')
        job.update(changes)
        self.jobs.append(job)
        self.cases.append(dict(case_id=job['case_id'], job=job, status='completed'))
        directory = self.export / 'cases' / job['case_id'] / 'result'; directory.mkdir(parents=True)
        fields = ('phase', 'operation', 'outcome', 'scheduled_ns', 'dispatched_ns', 'dispatch_observed', 'completed_ns', 'api_ns', 'response_ns', 'scheduling_ns', 'logical_keys', 'bytes', 'error_kind')
        with (directory / 'samples.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
            for value in (latency, latency * 3):
                writer.writerow(dict(phase='measurement', operation=op, outcome='ok', scheduled_ns=1, dispatched_ns=1, dispatch_observed='true', completed_ns=value + 1, api_ns=value, response_ns=value, scheduling_ns=0, logical_keys=1, bytes=1024, error_kind=''))
        return directory

    def publish(self):
        path = self.export / 'matrix.json'
        path.write_text(json.dumps({'spec': {'selection': 'nvmeof_path_compact'}, 'jobs': self.jobs}))
        (self.report / 'comparison.json').write_text(json.dumps({'cases': self.cases}))
        return path

    def generate(self):
        self.publish()
        return analysis.generate(self.export, self.report, self.output)

    def test_settings_and_repetitions(self):
        self.add(); self.add(repeat=1)
        self.add('tidehunter_verbs_native', group='G18W')
        self.add('tidehunter_nvme_rdma', group='G18W')
        rows, errors = analysis.settings(self.jobs)
        self.assertEqual(errors, [])
        self.assertEqual(rows[0]['differs from engine default'], '')
        self.assertEqual(rows[0]['repetitions'], '0, 1')
        self.assertEqual(rows[0]['cases'], 2)
        self.assertIn('history_persistence=memory_only', rows[1]['differs from engine default'])
        self.assertIn('live_export_mode=per_owner', rows[2]['differs from engine default'])
        self.assertIn('history_persistence=memory_only', rows[2]['differs from engine default'])

    def test_settings_only_and_missing(self):
        self.add('tidehunter_verbs_native', group='G18W')
        matrix = self.publish()
        analysis.main(['--settings-only', '--matrix', str(matrix), '--output', str(self.output)])
        text = (self.output / 'nvmeof_path.md').read_text()
        for statement in analysis.STATEMENTS:
            self.assertIn(statement, text)
        self.assertIn('## Settings', text)
        self.assertFalse((self.output / 'puts.csv').exists())
        del self.jobs[0]['history_persistence']
        matrix.write_text(json.dumps(self.jobs))
        with self.assertRaises(SystemExit) as error:
            analysis.main(['--settings-only', '--matrix', str(matrix), '--output', str(self.output)])
        self.assertEqual(error.exception.code, 1)
        with (self.output / 'settings.csv').open() as stream:
            self.assertEqual(next(csv.DictReader(stream))['history persistence'], 'missing')

    def test_reads_media_and_missing_counters(self):
        first = self.add('tidehunter_nvme_rdma', group='G18R', op='read', media={'expected': 'cold'})
        (first.parent / 'media_evidence.json').write_text(json.dumps(dict(schema_version=2, status='qualified', read_ops_per_success=.93, device_bytes_per_read=4096, reason_codes=[])))
        second = self.add('tidehunter_nvme_rdma', repeat=1, group='G18R', op='read', media={'expected': 'cold'})
        (second.parent / 'media_evidence.json').write_text(json.dumps(dict(schema_version=2, status='cold_read_ratio_below_threshold', read_ops_per_success=.8)))
        rows = self.generate()
        self.assertEqual(rows[0]['media_status'], 'qualified')
        self.assertEqual(rows[0]['read_ops_per_success'], .93)
        self.assertEqual(rows[0]['device_bytes_per_read'], 4096)
        self.assertEqual(rows[1]['cache_label'], 'partly cached')
        with (self.output / 'reads.csv').open() as stream:
            self.assertEqual(next(csv.DictReader(stream))['namespace_connects'], 'not available')

    def test_matched_reference_and_separate_repetitions(self):
        self.add(latency=1000)
        self.add('tidehunter_verbs_native', group='G18W', latency=2000)
        self.add('tidehunter_verbs_native', repeat=1, group='G18W', latency=3000)
        rows = self.generate()
        self.assertEqual(rows[1]['native_p50_ratio'], 2)
        self.assertIsNone(rows[2]['native_p50_ratio'])
        with (self.output / 'puts.csv').open() as stream:
            exported = list(csv.DictReader(stream))
        self.assertEqual(len(exported), 3)
        self.assertEqual(exported[2]['native_p50_ratio'], 'not available')

    def test_read_reference_requires_equal_population(self):
        self.add(op='read', media={'expected': 'memory'}, latency=1000)
        self.add('tidehunter_nvme_rdma', group='G18M', op='read',
                 media={'expected': 'memory'}, keys=65536, latency=2000)
        self.add('tidehunter_nvme_rdma', group='G18M', op='read',
                 media={'expected': 'memory'}, keys=4096, latency=2000)
        rows = self.generate()
        self.assertEqual(rows[1]['native_p50_ratio'], 2)
        self.assertIsNone(rows[2]['native_p50_ratio'])

    def test_reviewed_selection_names_in_report(self):
        self.add()
        matrix = self.publish()
        for selection in analysis.NVMEOF_PATH_SELECTIONS:
            for wrapped in (False, True):
                with self.subTest(selection=selection, wrapped=wrapped):
                    jobs = [dict(job, selection=selection) for job in self.jobs]
                    data = {'spec': {'selection': selection}, 'jobs': jobs} if wrapped else jobs
                    matrix.write_text(json.dumps(data))
                    analysis.generate(output=self.output, matrix=matrix, settings_only=True)
                    self.assertIn(selection, (self.output / 'nvmeof_path.md').read_text().splitlines()[0])

    def test_device_and_stage_evidence(self):
        directory = self.add('tidehunter_nvme_rdma', group='G18W')
        (directory.parent / 'device_counters.json').write_text(json.dumps({'before': [{'write_sectors': 10}], 'after': [{'write_sectors': 18}]}))
        (directory / 'measurement-read-evidence.json').write_text(json.dumps({'live_nvmeof': {'counters': {'control_ns': 4000, 'control_samples': 2, 'controller_connect_ns': 6000, 'controller_connect_samples': 2}}}))
        history = directory.parent / 'owner-durability'; history.mkdir()
        (history / 'node-0.history-maintenance.json').write_text('{"waits": 0}')
        row = self.generate()[0]
        self.assertEqual(row['device_write_payload_ratio'], 2)
        self.assertEqual(row['Control_mean_us'], 2)
        self.assertEqual(row['ControllerConnect_mean_us'], 3)
        self.assertEqual(row['history_waits'], 0)


if __name__ == '__main__':
    unittest.main()
