import copy
import csv
import json
from pathlib import Path
import tempfile
import unittest

import nvmeof_path as analysis
import cloud_campaign


def authority_evidence():
    first = dict.fromkeys(analysis.AUTHORITY_COUNTERS, 10000)
    counters = dict(zip(analysis.AUTHORITY_COUNTERS, (32000, 64000, 2, 4)))
    return {'scope': 'measurement_boundary', 'measurement_attempted': True, 'measurement_drained': True,
            'start': {'coordinator': first},
            'end': {'coordinator': {name: first[name] + value for name, value in counters.items()}},
            'deltas': {'coordinator': {'available': True, 'counters': counters}}}


def authority_diagnostics():
    return {'phase_attempted': True, 'measurement_drained': True, 'pre_completion_gate': {
        'available': True, 'scope': 'coordinator_arm_through_pre_completion_gate',
        'clock_scope': 'exclusive local wall elapsed time, not CPU',
        'stages': authority_evidence()['deltas']['coordinator']['counters']}}


def option_c_evidence(end_counters, levels=None, start_counters=None):
    start_counters = start_counters or dict.fromkeys(end_counters, 0)
    levels = {'cell_gauge_workers': 2, 'cell_gauge_thread_cpu_available': 1} if levels is None else levels
    return {'scope': 'measurement_boundary', 'measurement_attempted': True, 'measurement_drained': True,
            'start': {'option_c': {'available': True, 'counters': start_counters, 'levels': {}}},
            'end': {'option_c': {'available': True, 'counters': end_counters, 'levels': levels}}}


def per_cell_counters():
    return {'cell_frames': 10, 'cell_exact_buffered_cells': 0, 'cell_exact_streamed_cells': 0,
            'cell_blake2b_bytes': 4000, 'cell_exact_blake2b_bytes': 0,
            'cell_encoder_invocations': 10, 'cell_exact_encoder_invocations': 0,
            'cell_buffered_cells': 10, 'cell_streamed_cells': 0,
            'cell_conservative_fallbacks': 0, 'cell_exact_conservative_fallbacks': 0,
            'cell_maintenance_cpu_ns': 2_000_000, 'cell_logical_index_write_bytes': 5000,
            'cell_root_payload_cell_encodes': 0, 'cell_root_commits': 2, 'cell_exact_checkpoints': 0,
            'cell_admission_blocked_replay_tail_ns': 3_000_000, 'cell_admission_blocked_replay_tail': 1,
            'cell_dequeued': 10, 'cell_queue_wait_ns': 50_000, 'cell_w0_captured': 8, 'cell_w1_captured': 2}


def exact_counters(streamed):
    return {'cell_frames': 0, 'cell_exact_buffered_cells': 10 - streamed, 'cell_exact_streamed_cells': streamed,
            'cell_blake2b_bytes': 0, 'cell_exact_blake2b_bytes': 8000,
            'cell_encoder_invocations': 0, 'cell_exact_encoder_invocations': 10 + streamed,
            'cell_buffered_cells': 0, 'cell_streamed_cells': 0, 'cell_conservative_fallbacks': 0,
            'cell_exact_conservative_fallbacks': 0, 'cell_maintenance_cpu_ns': 8_000_000,
            'cell_logical_index_write_bytes': 8000, 'cell_root_payload_cell_encodes': 0,
            'cell_root_commits': 0, 'cell_exact_checkpoints': 4}


def write_replay_run(directory, mode, counters=None, checkpoints=100, status='completed', repetitions=4096,
                     streaming=None):
    directory.mkdir(parents=True)
    configuration = {'durability_tier': 'synchronous', 'owner_count': 2, 'history_persistence': 'wal_replay',
                     'client': {'replay_limits': {'max_batches': 4096}, 'max_index_bytes': 1 << 29}}
    if mode is not None:
        configuration['checkpoint_mode'] = mode
    if streaming is not None:
        configuration['force_streaming_cell_encoder'] = streaming
    profile = {'case_identity': {'scope': 'perpetual', 'fixture_sha256': 'f' * 64, 'repetitions': repetitions, 'N': 16},
               'measurement_elapsed_seconds': 10.0, 'successful_calls': 1000, 'api_calls_per_second': 100.0,
               'checkpoints_per_second': checkpoints / 10, 'mean_in_flight_api_calls': 4.0,
               'latency': {'operation:commit:success': {'api': {'count': 300, 'p50_ns': 1000, 'p99_ns': 9000, 'p999_ns': 20000}},
                           'commit:success:db0:phase1': {'api': {'count': 300, 'p50_ns': 1, 'p99_ns': 1, 'p999_ns': 1}}}}
    summary = {'status': status, 'backend': {'configuration': configuration},
               'phase_counts': {'profile': profile, 'counts': {'checkpoints': checkpoints, 'transactions': 50}}}
    (directory / 'summary.json').write_text(json.dumps(summary))
    if counters is not None:
        (directory / 'measurement-read-evidence.json').write_text(json.dumps(option_c_evidence(counters)))


class NvmeofPathTests(unittest.TestCase):
    def test_authority_columns_prefer_validated_read_boundaries_and_reach_csv(self):
        directory = self.add('tidehunter_nvme_rdma', group='G32W')
        evidence, diagnostics = authority_evidence(), authority_diagnostics()
        diagnostics['pre_completion_gate']['stages']['write_authority_prepare_ns'] = 128000
        (directory / 'measurement-read-evidence.json').write_text(json.dumps(evidence))
        (directory / 'write-diagnostics.json').write_text(json.dumps(diagnostics))
        row = analysis.result_row(self.cases[0], self.export)
        self.assertEqual(row['authority_prepare_us_per_put'], 8)
        self.assertEqual(row['authority_admit_us_per_put'], 16)
        self.assertEqual(row['batches_per_window'], 2)
        self.assertEqual(row['authority_interval_source'], 'measurement-read-evidence.json')
        self.generate()
        with (self.output / 'puts.csv').open() as stream:
            exported = next(csv.DictReader(stream))
        self.assertEqual(float(exported['authority_prepare_us_per_put']), 8)
        self.assertEqual(exported['authority_interval_scope'], 'measurement_start_to_drained_end_counter_delta')

    def test_option_c_window_derives_attribution_and_reaches_csv(self):
        directory = self.add('tidehunter_nvme_rdma', group='G32W')
        levels = {'cell_gauge_workers': 2, 'cell_gauge_thread_cpu_available': 1,
                  'cell_gauge_segment_dead_bytes': 128, 'cell_gauge_lag_l_f_batches': 7}
        (directory / 'measurement-read-evidence.json').write_text(
            json.dumps(option_c_evidence(per_cell_counters(), levels)))
        row = analysis.result_row(self.cases[0], self.export)
        self.assertEqual(row['option_c_source'], 'measurement-read-evidence.json')
        self.assertEqual(row['cell_maintenance_cpu_s'], 0.002)
        self.assertEqual((row['cell_rewritten_bytes'], row['cell_frames']), (4000, 10))
        self.assertEqual((row['cell_duplicate_encodes'], row['cell_root_payload_cell_encodes']), (0, 0))
        self.assertEqual((row['cell_snapshots'], row['cell_admission_blocked_ms']), (2, 3))
        self.assertEqual(row['cell_queue_wait_mean_us'], 5)
        self.assertEqual(row['cell_cpu_ns_per_emitted_byte'], 500)
        self.assertEqual(row['cell_range_imbalance'], 1.6)
        self.assertEqual((row['cell_segment_dead_bytes'], row['cell_lag_l_f_batches']), (128, 7))
        self.generate()
        with (self.output / 'puts.csv').open() as stream:
            exported = next(csv.DictReader(stream))
        self.assertEqual(int(exported['cell_rewritten_bytes']), 4000)

    def test_option_c_window_rejects_bad_boundaries_and_never_estimates(self):
        good = option_c_evidence(per_cell_counters())
        self.assertIsNotNone(analysis.option_c_window(good))
        for path, value in ((('end', 'option_c', 'counters', 'cell_frames'), -1),
                            (('start', 'option_c', 'counters', 'cell_frames'), 11),
                            (('end', 'option_c', 'counters', 'cell_frames'), 2**64 - 1),
                            (('end', 'option_c', 'counters', 'cell_frames'), 10.0),
                            (('end', 'option_c', 'counters', 'cell_extra'), 1),
                            (('end', 'option_c', 'available'), False),
                            (('end', 'option_c', 'levels', 'cell_gauge_workers'), True),
                            (('measurement_drained',), False), (('scope',), 'lifetime')):
            evidence = copy.deepcopy(good)
            target = evidence
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            self.assertIsNone(analysis.option_c_window(evidence), path)
        # Without thread CPU or a counter, the dependent values stay unavailable.
        counters = per_cell_counters(); del counters['cell_exact_blake2b_bytes']
        values = analysis.option_c_values(counters, {'cell_gauge_thread_cpu_available': 0})
        self.assertIsNone(values['cell_maintenance_cpu_s'])
        self.assertIsNone(values['cell_rewritten_bytes'])
        self.assertIsNone(values['cell_cpu_ns_per_emitted_byte'])
        self.assertIsNone(values['cell_range_imbalance'])

    def test_option_c_attribution_matches_arms_and_reports_ratios(self):
        runs = {'exact': [], 'exact_encode_once': [], 'per_cell': []}
        for repetition in range(2):
            for arm, mode, counters, streaming in (('exact', None, exact_counters(10), True),
                                                   ('exact_encode_once', 'exact', exact_counters(0), False),
                                                   ('per_cell', 'per_cell', per_cell_counters(), None)):
                directory = self.root / arm / str(repetition)
                write_replay_run(directory, mode, counters, streaming=streaming)
                runs[arm].append(str(directory))
        summary = analysis.option_c_attribution(runs, self.output)
        by_arm = {entry['arm']: entry for entry in summary}
        self.assertEqual([entry['arm'] for entry in summary], list(analysis.ATTRIBUTION_ARMS))
        self.assertEqual(by_arm['exact']['cell_duplicate_encodes'], 10)
        self.assertEqual(by_arm['exact_encode_once']['cell_duplicate_encodes'], 0)
        self.assertEqual(by_arm['per_cell']['cell_maintenance_cpu_s_vs_exact'], 0.25)
        self.assertEqual(by_arm['per_cell']['cell_rewritten_bytes_vs_exact'], 0.5)
        self.assertEqual(by_arm['per_cell']['cell_cpu_us_per_checkpoint'], 20)
        self.assertEqual(by_arm['exact']['completed_runs'], 2)
        with (self.output / 'option-c-latency.csv').open() as stream:
            latency = list(csv.DictReader(stream))
        self.assertEqual({row['population'] for row in latency}, {'operation:commit:success'})
        self.assertEqual(float(latency[0]['p99_us']), 9)
        self.assertTrue((self.output / 'option-c-attribution.md').is_file())
        # A pre-option-C Exact run has no counters: unavailable, not zero.
        legacy = self.root / 'legacy'
        write_replay_run(legacy, None)
        summary = analysis.option_c_attribution({'exact': [str(legacy)], 'per_cell': runs['per_cell']},
                                                self.root / 'legacy-output')
        self.assertIsNone(summary[0]['cell_maintenance_cpu_s'])
        self.assertIsNone(summary[1]['cell_maintenance_cpu_s_vs_exact'])

    def test_option_c_attribution_refuses_unmatched_runs(self):
        write_replay_run(self.root / 'exact', None, exact_counters(10), streaming=True)
        write_replay_run(self.root / 'cell', 'per_cell', per_cell_counters(), repetitions=1024)
        with self.assertRaisesRegex(ValueError, 'not matched'):
            analysis.option_c_attribution({'exact': [str(self.root / 'exact')],
                                           'per_cell': [str(self.root / 'cell')]}, self.output)
        write_replay_run(self.root / 'wrong', 'exact', per_cell_counters())
        with self.assertRaisesRegex(ValueError, 'checkpoint_mode per_cell'):
            analysis.option_c_attribution({'exact': [str(self.root / 'exact')],
                                           'per_cell': [str(self.root / 'wrong')]}, self.output)
        # The Exact arms are told apart by the recorded encoder switch.
        write_replay_run(self.root / 'once', 'exact', exact_counters(0), streaming=True)
        with self.assertRaisesRegex(ValueError, 'exact_encode_once needs force_streaming_cell_encoder false'):
            analysis.option_c_attribution({'exact': [str(self.root / 'exact')],
                                           'exact_encode_once': [str(self.root / 'once')]}, self.output)
        write_replay_run(self.root / 'unswitched', 'exact', exact_counters(0))
        with self.assertRaisesRegex(ValueError, 'arm exact needs force_streaming_cell_encoder true'):
            analysis.option_c_attribution({'exact': [str(self.root / 'unswitched')]}, self.output)
        with self.assertRaises(ValueError):
            analysis.option_c_attribution({'per_cell': [str(self.root / 'cell')]}, self.output)
        with self.assertRaises(SystemExit) as exit_status:
            analysis.main(['--output', str(self.output), '--option-c-attribution',
                           'exact=' + str(self.root / 'exact'), '--option-c-attribution',
                           'per_cell=' + str(self.root / 'cell')])
        self.assertEqual(exit_status.exception.code, 1)

    def test_authority_boundaries_reject_missing_reset_mismatched_and_saturated_counters(self):
        for path, value in (
                (('start', 'coordinator', 'write_authority_prepare_ns'), None),
                (('end', 'coordinator', 'write_authority_prepare_ns'), 9999),
                (('end', 'coordinator', 'write_authority_prepare_ns'), 2**64 - 1),
                (('deltas', 'coordinator', 'counters', 'write_authority_prepare_ns'), 32001),
                (('deltas', 'coordinator', 'counters', 'write_authority_batches'), True),
                (('deltas', 'coordinator', 'counters', 'write_authority_batches'), 4.0),
                (('deltas', 'coordinator', 'counters', 'write_authority_windows'), -1),
                (('deltas', 'coordinator', 'available'), False),
                (('measurement_attempted',), False), (('measurement_drained',), False),
                (('start',), 17), (('end', 'coordinator'), ['wrong']),
                (('deltas', 'coordinator', 'counters'), 'write_authority_batches'),
                (('scope',), 'lifetime')):
            with self.subTest(path=path, value=value):
                evidence = authority_evidence()
                target = evidence
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                self.assertTrue(all(value is None for value in analysis.authority_columns(
                    evidence, authority_diagnostics()).values()))
        evidence = authority_evidence()
        del evidence['deltas']['coordinator']['counters']['write_authority_prepare_ns']
        self.assertTrue(all(value is None for value in analysis.authority_columns(
            evidence, authority_diagnostics()).values()))

    def test_authority_profile_fallback_preserves_the_selected_interval_scope(self):
        diagnostics = authority_diagnostics()
        interval = copy.deepcopy(diagnostics['pre_completion_gate'])
        interval.pop('available')
        interval.update(scope='measurement_boundary_cumulative_delta')
        interval['stages']['write_authority_prepare_ns'] = 128000
        diagnostics['profile'] = {'coordinator_interval': interval}
        columns = analysis.authority_columns({}, diagnostics)
        self.assertEqual(columns['authority_prepare_us_per_put'], 8)
        self.assertEqual(columns['authority_interval_scope'], 'coordinator_arm_through_pre_completion_gate')
        diagnostics['pre_completion_gate'] = {'available': False, 'reason': 'unavailable'}
        columns = analysis.authority_columns({}, diagnostics)
        self.assertEqual(columns['authority_prepare_us_per_put'], 32)
        self.assertEqual(columns['authority_interval_source'], 'write-diagnostics.json:coordinator_interval')
        self.assertEqual(columns['authority_interval_scope'], 'measurement_boundary_cumulative_delta')
        for key in ('measurement_drained', 'phase_attempted'):
            broken = copy.deepcopy(diagnostics)
            broken[key] = False
            self.assertTrue(all(value is None for value in analysis.authority_columns({}, broken).values()))
        interval['clock_scope'] = ''
        self.assertTrue(all(value is None for value in analysis.authority_columns({}, diagnostics).values()))

    def test_authority_missing_or_zero_denominators_are_not_fabricated_ratios(self):
        self.assertTrue(all(value is None for value in analysis.authority_columns({}, {}).values()))
        evidence = authority_evidence()
        evidence['end']['coordinator'] = evidence['start']['coordinator'].copy()
        evidence['deltas']['coordinator']['counters'] = dict.fromkeys(analysis.AUTHORITY_COUNTERS, 0)
        columns = analysis.authority_columns(evidence, {})
        for name in analysis.AUTHORITY_COLUMNS[:3]:
            self.assertIsNone(columns[name])
        self.assertEqual(columns['authority_interval_source'], 'measurement-read-evidence.json')
        for name in analysis.AUTHORITY_COUNTERS[:2]:
            evidence['end']['coordinator'][name] = 42000 if name.endswith('prepare_ns') else 74000
        evidence['end']['coordinator']['write_authority_batches'] += 4
        evidence['deltas']['coordinator']['counters'].update(
            write_authority_prepare_ns=32000, write_authority_admit_ns=64000, write_authority_batches=4)
        columns = analysis.authority_columns(evidence, {})
        self.assertEqual(columns['authority_prepare_us_per_put'], 8)
        self.assertIsNone(columns['batches_per_window'])

    def test_thread_cpu_columns_keep_role_and_missing_class_semantics(self):
        first = {'available': True, 'summary': {
            'wal_write_owner': {'available': True, 'thread_count': 2, 'max_busy_pct': 90, 'mean_busy_pct': 70},
            'other': {'available': True, 'thread_count': 1, 'max_busy_pct': 99, 'mean_busy_pct': 99,
                      'busiest_name': 'driver'}}}
        second = {'available': True, 'summary': {
            'wal_write_owner': {'available': True, 'thread_count': 1, 'max_busy_pct': 40, 'mean_busy_pct': 30}}}
        columns = analysis.thread_cpu_columns({'coordinator': first, 'owners': [first, second]})
        self.assertEqual(columns['coordinator_wal_write_owner_max_busy_pct'], 90)
        self.assertAlmostEqual(columns['owner_wal_write_owner_mean_busy_pct'], 170 / 3)
        self.assertEqual(columns['coordinator_other_busiest_thread'], 'driver')
        self.assertIsNone(columns['owner_other_max_busy_pct'])
        self.assertIsNone(columns['coordinator_wal_live_read_mean_busy_pct'])
        first['available'] = False
        self.assertTrue(all(value is None for value in analysis.thread_cpu_columns(
            {'coordinator': first, 'owners': [first, second]}).values()))

    def test_wal_payload_metrics_use_measured_successful_puts(self):
        directory = self.add('tidehunter_nvme_rdma', group='G32W', measurement_operations=1000)
        row = analysis.result_row(self.cases[0], self.export)
        self.assertIsNone(row['wal_bytes_per_payload'])
        self.assertIsNone(row['puts_per_frame'])
        (directory / 'measurement-read-evidence.json').write_text(json.dumps({
            'live_nvmeof': {'counters': {'completed_write_bytes': 8192, 'leased_frames': 1}}}))
        row = analysis.result_row(self.cases[0], self.export)
        self.assertEqual(row['wal_bytes_per_payload'], 4)
        self.assertEqual(row['puts_per_frame'], 2)
        native = self.add('native', group='G32N', measurement_operations=1000)
        (native.parent / 'native-wal-evidence.json').write_text(json.dumps({
            'available': True, 'logical_wal_bytes': 4096}))
        row = analysis.result_row(self.cases[1], self.export)
        self.assertEqual(row['native_logical_wal_bytes_per_payload'], 2)
        self.assertIsNone(row['wal_bytes_per_payload'])
        (native.parent / 'native-wal-evidence.json').write_text(json.dumps({'available': False}))
        self.assertIsNone(analysis.result_row(self.cases[1], self.export)['native_logical_wal_bytes_per_payload'])

    def test_every_reviewed_selection_group_is_accepted(self):
        for selection in cloud_campaign.NVMEOF_PATH_SELECTIONS:
            with self.subTest(selection=selection):
                jobs = cloud_campaign.make_focused_matrix(selection)
                matrix = self.export / 'all-groups.json'
                matrix.write_text(json.dumps(jobs))
                rows = analysis.generate(matrix=matrix, output=self.output, settings_only=True)
                self.assertEqual({r['group'] for r in rows}, {j['group'] for j in jobs})

    def test_new_groups_reach_operation_tables(self):
        reads = ('G30R', 'G30S', 'G30V', 'G31G')
        puts = ('G30W', 'G31W', 'G31H', 'G31S', 'G31X', 'G32W', 'G32H', 'G32S', 'G32N')
        for group in reads + puts:
            self.add('tidehunter_nvme_rdma', group=group, op='read' if group in reads else 'put')
        self.generate()
        for name, groups in (('reads', reads), ('puts', puts)):
            with (self.output / (name + '.csv')).open() as stream:
                self.assertEqual({row['group'] for row in csv.DictReader(stream)}, set(groups))

    def test_live_executed_columns_and_legacy_export(self):
        directory = self.add('tidehunter_nvme_rdma', group='G32W')
        missing = analysis.result_row(self.cases[0], self.export)
        self.assertIsNone(missing['live_leased_sync'])
        self.assertIsNone(missing['live_leased_flush_min_frame_bytes'])
        path = directory / 'manifest.json'
        manifest = {'backend_info': {'configuration': {}}}
        path.write_text(json.dumps(manifest))
        job = self.cases[0]['job']
        job.update(live_leased_sync='group_flush', live_leased_flush_min_frame_bytes=32768)
        row = analysis.result_row(self.cases[0], self.export)
        self.assertEqual((row['live_leased_sync'], row['live_leased_flush_min_frame_bytes']),
                         ('per_frame_fua', 0))
        for mode in ('group_flush', 'per_frame_fua'):
            manifest['backend_info']['configuration'].update(
                live_leased_sync=mode, live_leased_flush_min_frame_bytes=65536,
                live_polled_writes=mode == 'group_flush')
            path.write_text(json.dumps(manifest))
            row = analysis.result_row(self.cases[0], self.export)
            self.assertEqual((row['live_leased_sync'], row['live_leased_flush_min_frame_bytes']),
                             (mode, 65536))
            self.assertEqual(row['polled_writes'], mode == 'group_flush')
        self.cases[0]['status'] = 'not_run'
        self.assertIsNone(analysis.result_row(self.cases[0], self.export)['live_leased_sync'])

    def test_owner_trace_coverage_reaches_csv_without_case_or_cpu_normalization(self):
        directory = self.add('tidehunter_nvme_rdma', group='G29R', selection='nvmeof_f6', op='read')
        samples_path = directory / 'samples.csv'
        with samples_path.open() as stream:
            samples = list(csv.DictReader(stream))
        samples[-1].update(completed_ns=100_000_000_001, api_ns=100_000_000_000,
                           response_ns=100_000_000_000)
        with samples_path.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=tuple(samples[0]))
            writer.writeheader()
            writer.writerows(samples)
        artifact = {'truncated': True, 'buffer_overruns': 7, 'trace_coverage': {
            'event_count': 100, 'first_timestamp_ns': 10_000_000_000,
            'last_timestamp_ns': 12_000_000_000, 'duration_ns': 2_000_000_000,
            'scope': 'captured_complete_trace_events', 'clock': 'mono'}}
        (directory.parent / 'owner-ftrace.json').write_text(json.dumps(artifact))
        (directory.parent / 'owner-cpu.json').write_text(json.dumps({
            'available': True, 'elapsed_seconds': 100, 'summary': {
                'max_busy_pct': 95, 'mean_busy_pct': 72.5, 'max_softirq_pct': 10,
                'cpus_above_90_busy': 1}}))
        rows = self.generate()
        self.assertEqual(rows[0]['owner_trace_scope'], 'primary_owner')
        self.assertEqual(rows[0]['owner_trace_captured_events'], 100)
        self.assertEqual(rows[0]['owner_trace_covered_seconds'], 2)
        self.assertEqual(rows[0]['owner_trace_observed_events_per_second'], 50)
        self.assertAlmostEqual(rows[0]['calls_per_second'], 2 / 100)
        self.assertEqual(rows[0]['owner_trace_truncated'], True)
        self.assertEqual(rows[0]['owner_trace_buffer_overruns'], 7)
        self.assertEqual(rows[0]['owner_cpu_mean_busy_pct'], 72.5)
        with (self.output / 'reads.csv').open() as stream:
            row = next(csv.DictReader(stream))
        self.assertEqual(float(row['owner_trace_observed_events_per_second']), 50)
        text = (self.output / 'nvmeof_path.md').read_text()
        self.assertIn('not read IOPS', text)
        self.assertIn('separate full sampling window', text)

    def test_owner_trace_bad_missing_and_zero_coverage_never_invents_a_rate(self):
        valid = {'event_count': 100, 'first_timestamp_ns': 10_000_000_000,
                 'last_timestamp_ns': 12_000_000_000, 'duration_ns': 2_000_000_000,
                 'scope': 'captured_complete_trace_events', 'clock': 'mono'}
        broken = [None, {}, 'bad']
        for key, value in (
                ('event_count', True), ('event_count', -1), ('event_count', 1),
                ('event_count', 0), ('first_timestamp_ns', None),
                ('last_timestamp_ns', 9_000_000_000), ('duration_ns', 0),
                ('duration_ns', 2_000_000_001), ('duration_ns', 2e9),
                ('scope', 'case'), ('clock', 'wall')):
            broken.append(valid | {key: value})
        broken.extend([
            valid | {'first_timestamp_ns': 12_000_000_000, 'duration_ns': 0},
            valid | {'event_count': 0, 'first_timestamp_ns': None,
                     'last_timestamp_ns': None, 'duration_ns': None}])
        for coverage in broken:
            with self.subTest(coverage=coverage):
                row = analysis.owner_trace_columns({
                    'trace_coverage': coverage, 'truncated': True, 'buffer_overruns': 3})
                self.assertIsNone(row['owner_trace_observed_events_per_second'])
                self.assertIsNone(row['owner_trace_covered_seconds'])
                self.assertTrue(row['owner_trace_truncated'])
                self.assertEqual(row['owner_trace_buffer_overruns'], 3)
        self.assertIsNone(analysis.owner_trace_columns({})['owner_trace_observed_events_per_second'])

    def test_owner_cpu_summary_keeps_unavailable_distinct(self):
        directory = self.add('tidehunter_nvme_rdma', group='G29R', selection='nvmeof_f6', op='read')
        path = directory.parent / 'owner-cpu.json'
        for available in (True, False):
            path.write_text(json.dumps({'available': available, 'summary': {
                'max_busy_pct': 95, 'mean_busy_pct': 72.5, 'max_softirq_pct': 10, 'cpus_above_90_busy': 1}}))
            row = analysis.result_row(self.cases[0], self.export)
            self.assertEqual([row[column] for column in analysis.CPU_COLUMNS],
                             [95, 72.5, 10, 1] if available else [None] * 4)
        path.unlink()
        self.assertTrue(all(analysis.result_row(self.cases[0], self.export)[column] is None
                            for column in analysis.CPU_COLUMNS))
        self.generate()
        with (self.output / 'reads.csv').open() as stream:
            self.assertEqual(next(csv.DictReader(stream))['group'], 'G29R')

    def test_crossing2_report_keeps_contracts_separate(self):
        for selection, tier in (('nvmeof_contracts_crossing', 'synchronous'),
                                ('nvmeof_contracts_crossing2', 'native')):
            self.add('tidehunter_nvme_rdma', group='G22W', selection=selection,
                     durability_tier=tier, owner_wal_file_bytes=268435456)
        matrix = self.publish()
        matrix.write_text(json.dumps(self.jobs))
        rows = analysis.generate(self.export, self.report, self.output)
        self.assertEqual([row['contract_class'] for row in rows], ['S', 'P'])
        text = (self.output / 'nvmeof_path.md').read_text()
        self.assertIn('NVMe-oF data path — nvmeof_contracts_crossing2', text)
        self.assertIn('## Puts — class S', text)
        self.assertIn('## Puts — class P', text)
        with (self.output / 'puts.csv').open() as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 2)

    def test_crossing_effective_geometry_and_puts(self):
        self.add('tidehunter_nvme_rdma', group='G22W',
                 selection='nvmeof_contracts_crossing', owner_wal_file_bytes=268435456)
        self.generate()
        with (self.output / 'settings.csv').open() as stream:
            row = next(csv.DictReader(stream))
        self.assertEqual(row['owner WAL file bytes'], '268435456')
        self.assertIn('owner_wal_file_bytes=268435456', row['differs from engine default'])
        with (self.output / 'puts.csv').open() as stream:
            self.assertEqual(next(csv.DictReader(stream))['group'], 'G22W')

    def test_contract_classes_and_separate_markdown_tables(self):
        for tier, expected in (('value_wal_barrier', 'S'), ('page_cache', 'P')):
            self.add(group='G21N', native_durability=tier, selection='nvmeof_contracts')
            self.assertEqual(analysis.contract_class(self.jobs[-1]), expected)
        for tier, expected in (('synchronous', 'S'), ('native', 'P')):
            self.add('tidehunter_nvme_rdma', group='G21W', durability_tier=tier,
                     selection='nvmeof_contracts')
            self.assertEqual(analysis.contract_class(self.jobs[-1]), expected)
        rows = self.generate()
        self.assertEqual([row['contract_class'] for row in rows], ['S', 'P', 'S', 'P'])
        text = (self.output / 'nvmeof_path.md').read_text()
        for label in ('S', 'P'):
            table = text.split('## Puts — class ' + label + '\n', 1)[1].split('## ', 1)[0]
            case_ids = {line.split('|')[1].strip() for line in table.splitlines() if line.startswith('|')}
            for row in rows:
                self.assertEqual(row['case_id'] in case_ids,
                                 row['contract_class'] == label)
        self.assertIn('## Settings — S — synced', text)
        self.assertIn('## Settings — P — page cache', text)

    def test_conditional_stages_exclude_calls_without_pair(self):
        directory = self.add('tidehunter_nvme_rdma')
        records = [
            dict(events=[dict(stage='caller_start', elapsed_ns=0),
                         dict(stage='caller_finished', elapsed_ns=1000000)]),
            dict(events=[dict(stage='caller_start', elapsed_ns=0),
                         dict(stage='live_extent_switch_started', elapsed_ns=0),
                         dict(stage='live_extent_switch_completed', elapsed_ns=1000000),
                         dict(stage='caller_finished', elapsed_ns=1000000)])]
        (directory / 'write-timeline.json').write_text(json.dumps(dict(timeline=dict(records=records))))
        row = next(row for row in analysis.stage_rows(directory.parent)
                   if row['stage_pair'] == 'live_extent_switch_started -> live_extent_switch_completed')
        self.assertEqual(row['calls_with_pair'], 1)
        self.assertEqual(row['cond_p50_us'], 1000)
        self.assertEqual(row['cond_mean_us'], 1000)
        self.assertEqual(row['cond_p99_us'], 1000)
        self.assertEqual(row['mean_us'], 500)

    def test_load_model_cold_label_and_worker_counters(self):
        directory = self.add('tidehunter_nvme_rdma', media={'expected': 'cold'})
        (directory / 'measurement-read-evidence.json').write_text(json.dumps(
            {'live_nvmeof': {'counters': {'leased_frames': 20, 'leased_worker_waits': 22,
                                        'leased_worker_wait_timeouts': 1}}}))
        row = analysis.result_row(self.cases[0], self.export)
        self.assertEqual(row['cache_label'], 'cold unqualified')
        self.assertEqual(row['load_model'], 'closed_loop')
        self.assertEqual(row['leased_worker_waits'], 22)
        self.assertEqual(row['worker_timeouts_per_frame'], .05)
        self.jobs[0]['offered_ops_per_second'] = 100
        self.assertEqual(analysis.result_row(self.cases[0], self.export)['load_model'], 'open_loop')

    def test_stages_csv_uses_three_complete_captured_calls(self):
        directory = self.add('tidehunter_nvme_rdma', group='G21W', selection='nvmeof_contracts')
        records = [dict(caller_sample_id=index, events=[
            dict(stage='caller_start', elapsed_ns=0),
            dict(stage='prepared', elapsed_ns=(index + 1) * 1000),
            dict(stage='caller_finished', elapsed_ns=(index + 2) * 2000)])
            for index in range(3)]
        (directory / 'write-timeline.json').write_text(json.dumps(
            dict(timeline=dict(records=records))))
        expected = analysis.stage_rows(directory.parent)
        self.assertEqual([row['stage_pair'] for row in expected],
                         ['caller_start -> prepared', 'prepared -> caller_finished'])
        self.assertEqual([row['mean_us'] for row in expected], [2, 4])
        self.assertEqual([row['p50_us'] for row in expected], [2, 4])
        self.assertEqual([row['share'] for row in expected], [1 / 3, 2 / 3])
        self.generate()
        with (self.output / 'stages.csv').open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['case_id'], self.cases[0]['case_id'])
        self.assertEqual([float(row['mean_us']) for row in rows], [2, 4])
        self.assertEqual([float(row['p50_us']) for row in rows], [2, 4])
        self.assertAlmostEqual(sum(float(row['share']) for row in rows), 1)

    def test_leased_write_stages_are_consecutive_csv_intervals(self):
        directory = self.add('tidehunter_nvme_rdma', group='G21W', selection='nvmeof_contracts')
        names = ['caller_start', 'live_io_started', 'live_frame_encoded',
                 'live_io_submitted', 'live_io_completed', 'caller_finished']
        records = [dict(caller_sample_id=0, events=[dict(stage=name, elapsed_ns=index * 1000)
                                for index, name in enumerate(names)])]
        (directory / 'write-timeline.json').write_text(json.dumps(dict(timeline=dict(records=records))))
        rows = analysis.stage_rows(directory.parent)
        self.assertEqual([row['stage_pair'] for row in rows],
                         [start + ' -> ' + end for start, end in zip(names, names[1:])])
        self.assertEqual([row['mean_us'] for row in rows], [1] * 5)
        self.generate()
        with (self.output / 'stages.csv').open() as stream:
            self.assertEqual([row['stage_pair'] for row in csv.DictReader(stream)],
                             [row['stage_pair'] for row in rows])

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

    def test_parallel_writer_measurements_keep_native_and_leased_scopes(self):
        native = self.add(group='G23N', selection='nvmeof_parallel_writes', concurrency=64)
        leased = self.add('tidehunter_nvme_rdma', group='G23W', selection='nvmeof_parallel_writes', concurrency=64)
        self.jobs[-1].pop('live_leased_write_window', None)
        (leased / 'measurement-read-evidence.json').write_text(json.dumps(dict(
            live_nvmeof_peaks=dict(max_leased_in_flight=dict(at_start=4, at_end=31,
                scope='lifetime maximum, not a measurement-window maximum')))))
        (leased / 'manifest.json').write_text(json.dumps(dict(backend_info=dict(configuration=dict(
            live_leased_write_window=None, live_leased_write_window_bound=31)))))
        rows = self.generate()
        self.assertAlmostEqual(rows[0]['mean_inflight_calls'], 4 / 3)
        self.assertAlmostEqual(rows[1]['mean_inflight_calls'], 4 / 3)
        self.assertIsNone(rows[0]['max_leased_in_flight'])
        self.assertEqual(rows[1]['max_leased_in_flight'], 31)
        self.assertIn('lifetime maximum', rows[1]['max_leased_in_flight_scope'])
        self.assertIsNone(rows[1]['live_leased_write_window'])
        self.assertEqual(rows[1]['live_leased_write_window_bound'], 31)
        with (self.output / 'puts.csv').open() as stream:
            output = list(csv.DictReader(stream))
        self.assertEqual(output[1]['max_leased_in_flight'], '31')
        self.assertEqual(output[1]['live_leased_write_window_bound'], '31')

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
        self.assertEqual(rows[1]['cache_label'], 'cold unqualified')
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
