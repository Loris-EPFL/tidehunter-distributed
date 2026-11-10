"""Independent checks of measurement-window profiler and device arithmetic."""
import copy
import unittest

import decomposition


def by_stage(rows):
    return {row['stage']: row for row in rows}


def profile_stage(total, count):
    return {'available': bool(count), 'sum_ns': total if count else None,
            'count': count, 'max_ns': total if count else None,
            'unavailable_reason': None if count else 'no observations for this stage'}


def sample(identifier, completion):
    return {'phase': 'measurement', 'operation': 'put', 'outcome': 'success',
            'caller_sample_id': identifier, 'scheduled_ns': completion - 1000,
            'dispatched_ns': completion - 1000, 'dispatch_observed': 'true',
            'completed_ns': completion, 'api_ns': 1000, 'response_ns': 1000,
            'scheduling_ns': 0, 'logical_keys': 1, 'bytes': 1024, 'error_kind': ''}


class DecompositionTests(unittest.TestCase):
    def test_native_cold_rejects_contaminated_and_negative_splits(self):
        for count, mean in ((11, 1), (10, 11)):
            row = decomposition.read_residuals({}, [], {'count': count, 'mean_us': mean},
                                               10, 10, 'native', True)[0]
            self.assertIsNone(row['mean_us'])
            self.assertIn('not computed', row['selection'])
        row = decomposition.read_residuals({}, [], {'count': 10, 'mean_us': 4},
                                           10, 10, 'native', True)[0]
        self.assertEqual(row['mean_us'], 6)

    def test_read_profiles_keep_independent_populations_and_unavailable_locks(self):
        profile = {'selection_policy': 'first N remote physical flights',
                   'overlap_semantics': 'independent prefixes; not additive',
                   'coordinator_resolution_valid': True,
                   'coordinator_resolution': {
                       'selection_policy': 'independent first N API resolution attempts',
                       'selected': 3, 'stages': {
                           'index_lookup': profile_stage(9000, 3),
                           'cache_lookup': profile_stage(4000, 2),
                           'index_lock_wait': profile_stage(0, 0)}},
                   'stages': {'exchange': profile_stage(20000, 2),
                              'request_encode': profile_stage(2000, 2),
                              'nvme_flight': profile_stage(30000, 1)}}
        rows = decomposition.read_profile(profile)
        self.assertIsInstance(rows, list)
        stages = by_stage(rows)
        for name, total, count in [('index_lookup', 9000, 3), ('cache_lookup', 4000, 2),
                                   ('exchange', 20000, 2), ('request_encode', 2000, 2),
                                   ('nvme_flight', 30000, 1)]:
            row = stages[name]
            self.assertEqual(row['count'], count)
            self.assertAlmostEqual(row['mean_us'], total / count / 1000)
            self.assertEqual(row['exactness'], 'sampled_mean')
            self.assertTrue(row['selection'])
            self.assertTrue(row['overlap'])
        self.assertNotIn('index_lock_wait', stages)
        self.assertNotEqual(stages['index_lookup']['selection'], stages['exchange']['selection'])

    def test_hybrid_reader_uses_only_available_measurement_deltas(self):
        evidence = {'scope': 'measurement_boundary', 'nvmeof': {'available': True,
            'readers': {'r0': {'available': True, 'identity': {'partition': 0, 'transport': 'rdma'},
                'stages': {'queue': {'delta': {'available': True, 'counters': {'count': 2, 'sum_ns': 8000}}},
                           'submit_to_cq': {'delta': {'available': False}}}},
                        'r1': {'available': True, 'identity': {'partition': 1, 'transport': 'rdma'},
                'stages': {'queue': {'delta': {'available': True, 'counters': {'count': 1, 'sum_ns': 4000}}}}}}}}
        rows = decomposition.reader_stages(evidence)
        self.assertIsInstance(rows, list)
        queue = by_stage(rows)['queue']
        self.assertEqual(queue['count'], 3)
        self.assertAlmostEqual(queue['mean_us'], (8000 + 4000) / 3 / 1000)
        self.assertEqual(queue['exactness'], 'sampled_mean')
        self.assertTrue(queue['selection'])
        self.assertTrue(queue['overlap'])
        self.assertNotIn('submit_to_cq', by_stage(rows))

    def test_run_seven_hybrid_reader_excerpt(self):
        # Trimmed from G10R-r0-866e801c10fb6b732d36 measurement-read-evidence.
        evidence = {'nvmeof': {'readers': {'one': {'stages': {
            'submit_to_cq': {'delta': {'available': True, 'counters': {'count': 480, 'sum_ns': 62045966}}},
            'queue': {'delta': {'available': True, 'counters': {'count': 480, 'sum_ns': 1659857}}}}}}}}
        stages = by_stage(decomposition.reader_stages(evidence))
        self.assertAlmostEqual(stages['submit_to_cq']['mean_us'], 62045966 / 480 / 1000)
        self.assertAlmostEqual(stages['queue']['mean_us'], 1659857 / 480 / 1000)

    def test_real_buffered_memory_submit_excerpt(self):
        # G12D-r0-2d105120fe757255aaa3: all 4096 memory-read flights.
        raw = [(894, 24614007), (978, 27641780), (755, 20897442),
               (565, 16877043), (904, 25675781)]
        evidence = {'nvmeof': {'readers': {str(i): {'stages': {
            'submit_to_cq': {'delta': {'available': True, 'counters': {
                'count': count, 'sum_ns': total}}}}}
            for i, (count, total) in enumerate(raw)}}}
        stage = by_stage(decomposition.reader_stages(evidence))['submit_to_cq']
        self.assertEqual(stage['count'], 4096)
        self.assertAlmostEqual(stage['mean_us'], sum(total for _, total in raw) / 4096 / 1000)

    def test_kernel_diskstats_delta_uses_device_identity_and_host(self):
        before = {'hosts': [{'diskstats': ''}, {'diskstats': '8 0 sda 10 0 20 30 0 0 0 0 0 0 0\n8 1 sda1 90 0 90 90 0 0 0 0 0 0 0\n'}]}
        after = {'hosts': [{'diskstats': ''}, {'diskstats': '8 0 sda 14 0 36 38 0 0 0 0 0 0 0\n8 1 sda1 190 0 190 190 0 0 0 0 0 0 0\n'}]}
        result = decomposition.kernel_device(before, after, [{'owner_index': 0, 'major': 8, 'minor': 0}])
        self.assertEqual(result['count'], 14 - 10)
        self.assertEqual(result['bytes'], (36 - 20) * 512)
        self.assertEqual(result['mean_us'], (38 - 30) * 1000 / (14 - 10))
        self.assertEqual(result['exactness'], 'device_counter')

    def test_real_contaminated_diskstats_excerpt(self):
        # G12D-r0-6c4d9d94da989fe10037, hosts 1..5, 2048 successful reads.
        before = ['259 3 nvme0n1 24491 39 462640 1664 0 0 0 0 0 0 0',
                  '259 3 nvme0n1 26035 39 517344 1867 0 0 0 0 0 0 0',
                  '259 3 nvme0n1 25672 39 492128 5434 0 0 0 0 0 0 0',
                  '259 3 nvme0n1 24914 40 472160 5347 0 0 0 0 0 0 0',
                  '259 3 nvme0n1 24979 40 481440 2409 0 0 0 0 0 0 0']
        after = ['259 3 nvme0n1 24926 53 481240 1709 0 0 0 0 0 0 0',
                 '259 3 nvme0n1 26565 55 537896 1920 0 0 0 0 0 0 0',
                 '259 3 nvme0n1 26164 53 512280 5484 0 0 0 0 0 0 0',
                 '259 3 nvme0n1 25392 56 491552 5398 0 0 0 0 0 0 0',
                 '259 3 nvme0n1 25457 56 500648 2457 0 0 0 0 0 0 0']
        snapshots = [{'hosts': [{'diskstats': ''}] + [{'diskstats': line} for line in lines]}
                     for lines in (before, after)]
        devices = [{'owner_index': i, 'major': 259, 'minor': 3} for i in range(5)]
        result = decomposition.kernel_device(*snapshots, devices)
        delta = [sum(int(b.split()[i]) - int(a.split()[i]) for a, b in zip(before, after))
                 for i in (3, 5, 6)]
        self.assertEqual(result['count'], delta[0])
        self.assertGreater(delta[0] / 2048, 1.05)
        self.assertEqual(result['bytes'], delta[1] * 512)
        self.assertAlmostEqual(result['mean_us'], delta[2] * 1000 / delta[0])
        self.assertAlmostEqual(result['bytes'] / delta[0] / 1024, delta[1] / delta[0] / 2)

    def test_spdk_uses_tick_rate_and_counter_deltas(self):
        start = [{'index': 0, 'complete': True, 'target': {'kind': 'spdk-target-stats',
                  'target_implementation': 'spdk', 'campaign_id': 'g', 'unit_name': 't',
                  'pid': 1, 'process_start_ticks': 2, 'binary_sha256': 'a' * 64,
                  'bdev_iostat': {'ticks': 1000, 'tick_rate': 1000000,
                    'bdevs': [{'name': 'wal', 'bytes_read': 4096, 'num_read_ops': 1,
                               'bytes_written': 0, 'num_write_ops': 0, 'read_latency_ticks': 10, 'io_error': {}}]}}}]
        end = copy.deepcopy(start)
        bdev = end[0]['target']['bdev_iostat']['bdevs'][0]
        bdev.update(bytes_read=16384, num_read_ops=4, read_latency_ticks=70)
        result = decomposition.spdk_bdev(start, end)
        self.assertEqual(result['count'], 4 - 1)
        self.assertEqual(result['bytes'], 16384 - 4096)
        self.assertAlmostEqual(result['mean_us'], (70 - 10) / 1000000 * 1000000 / 3)
        self.assertEqual(result['exactness'], 'device_counter')

    def test_device_zero_and_reset_are_not_fabricated_latency(self):
        snapshot = {'hosts': [{'diskstats': '8 0 sda 10 0 20 30 0 0 0 0 0 0 0'}]}
        device = [{'host_index': 0, 'major': 8, 'minor': 0}]
        zero = decomposition.kernel_device(snapshot, snapshot, device)
        self.assertEqual(zero['count'], 0)
        self.assertIsNone(zero['mean_us'])
        reset = {'hosts': [{'diskstats': '8 0 sda 9 0 20 30 0 0 0 0 0 0 0'}]}
        with self.assertRaises(ValueError):
            decomposition.kernel_device(snapshot, reset, device)
        with self.assertRaises(KeyError):
            decomposition.kernel_device(snapshot, snapshot, [{'host_index': 0, 'major': 8, 'minor': 1}])

    def test_timeline_intervals_are_recomputed_from_adjacent_local_events(self):
        records = [{'caller_sample_id': 7, 'completed': True, 'error': None, 'events': [
            {'stage': 'caller_start', 'elapsed_ns': 100},
            {'stage': 'gate_acquired', 'elapsed_ns': 2100},
            {'stage': 'caller_finished', 'elapsed_ns': 7100}]},
            {'caller_sample_id': 8, 'completed': True, 'error': None, 'events': [
            {'stage': 'caller_start', 'elapsed_ns': 200},
            {'stage': 'gate_acquired', 'elapsed_ns': 4200},
            {'stage': 'caller_finished', 'elapsed_ns': 10200}]}]
        rows = decomposition.write_timeline({'records': records})
        self.assertIsInstance(rows, list)
        actual = {(r['caller_sample_id'], r['start_stage'], r['end_stage']): r for r in rows}
        for record in records:
            for first, second in zip(record['events'], record['events'][1:]):
                row = actual[(record['caller_sample_id'], first['stage'], second['stage'])]
                self.assertEqual(row['start_ns'], first['elapsed_ns'])
                self.assertEqual(row['end_ns'], second['elapsed_ns'])
                self.assertAlmostEqual(row['mean_us'], (second['elapsed_ns'] - first['elapsed_ns']) / 1000)
                self.assertEqual(row['count'], 1)
                self.assertEqual(row['exactness'], 'exact_interval')
                self.assertTrue(row['selection'])
                self.assertTrue(row['overlap'])

    def test_owner_timeline_real_linked_envelopes_are_counted_once(self):
        # Trimmed from 0491 write-timeline.json, timeline (not the wrapper).
        timeline = {'owner_records': [
            {'owner': 0, 'envelope_id': 403, 'batch_count': 2,
             'handler_ns': 234839, 'append_persist_ns': 183025,
             'append_wal_ns': 30350, 'control_queue_ns': 12419,
             'appender_queue_ns': 4660, 'flusher_queue_ns': 7369},
            {'owner': 0, 'envelope_id': 404, 'batch_count': 1,
             'handler_ns': 58084, 'append_persist_ns': 38109,
             'append_wal_ns': 9340, 'control_queue_ns': 10365,
             'appender_queue_ns': 4385, 'flusher_queue_ns': 5610},
        ], 'records': [
            {'caller_sample_id': 1, 'completed': True, 'error': None, 'events': [
                {'stage': 'tx_queued', 'elapsed_ns': 1467103, 'owner': 0, 'envelope_id': 403},
                {'stage': 'completion_harvested', 'elapsed_ns': 1744462, 'owner': 0, 'envelope_id': 403}]},
            {'caller_sample_id': 14, 'completed': True, 'error': None, 'events': [
                {'stage': 'tx_queued', 'elapsed_ns': 1468188, 'owner': 0, 'envelope_id': 403},
                {'stage': 'completion_harvested', 'elapsed_ns': 1744521, 'owner': 0, 'envelope_id': 403}]},
            {'caller_sample_id': 25, 'completed': True, 'error': None, 'events': [
                {'stage': 'tx_queued', 'elapsed_ns': 1912520, 'owner': 0, 'envelope_id': 404},
                {'stage': 'completion_harvested', 'elapsed_ns': 2006335, 'owner': 0, 'envelope_id': 404}]},
        ]}
        rows = decomposition.owner_timeline(timeline)
        self.assertEqual(len(rows), 7)
        stages = by_stage(rows)
        for field in ('handler_ns', 'append_persist_ns', 'append_wal_ns',
                      'control_queue_ns', 'appender_queue_ns', 'flusher_queue_ns'):
            row = stages['owner_' + field]
            expected = sum(owner[field] for owner in timeline['owner_records']) / 2 / 1000
            self.assertAlmostEqual(row['mean_us'], expected)
            self.assertEqual(row['count'], 2)
            self.assertEqual(row['exactness'], 'sampled_mean')
            self.assertEqual(row['excluded_shared'], 0)
            self.assertEqual(row['parent'], 'tx_queued → completion_harvested' if field == 'handler_ns' else 'owner_handler_ns')
            self.assertTrue(row['selection'])
            self.assertTrue(row['overlap'])
        residual = stages['exchange minus owner handler (wire, both network stacks, TLS, dispatch)']
        self.assertAlmostEqual(residual['mean_us'], (2006335 - 1912520 - 58084) / 1000)
        self.assertEqual(residual['count'], 1)
        self.assertEqual(residual['excluded_shared'], 2)
        self.assertEqual(residual['exactness'], 'derived_duration_difference')
        self.assertEqual(residual['parent'], 'tx_queued → completion_harvested')
        self.assertTrue(residual['selection'])
        self.assertTrue(residual['overlap'])

    def test_owner_timeline_partial_missing_and_unmatched_records(self):
        timeline = {'owner_records': [
            {'owner': 1, 'envelope_id': 7, 'batch_count': 1,
             'handler_ns': 1000, 'append_wal_ns': 0},
            {'owner': 2, 'envelope_id': 7, 'batch_count': 1,
             'handler_ns': 999999},
            {'owner': 1, 'envelope_id': 8, 'batch_count': 1,
             'handler_ns': 2000},
            {'owner': 1, 'envelope_id': 9, 'batch_count': 2,
             'handler_ns': 3000},
        ], 'records': [
            {'events': [
                {'stage': 'tx_queued', 'owner': 1, 'envelope_id': 7, 'elapsed_ns': 10000},
                {'stage': 'completion_harvested', 'owner': 1, 'envelope_id': 7, 'elapsed_ns': 15000}]},
            # An incomplete exchange still links an owner annotation, not a residual.
            {'events': [{'stage': 'tx_queued', 'owner': 1, 'envelope_id': 8, 'elapsed_ns': 20}]},
            # A shared batch is excluded even when only one caller was retained.
            {'events': [
                {'stage': 'tx_queued', 'owner': 1, 'envelope_id': 9, 'elapsed_ns': 30},
                {'stage': 'completion_harvested', 'owner': 1, 'envelope_id': 9, 'elapsed_ns': 9000}]},
            # A harvest alone is not evidence that this caller submitted an envelope.
            {'events': [{'stage': 'completion_harvested', 'owner': 2,
                         'envelope_id': 7, 'elapsed_ns': 200}]},
            {'events': [
                {'stage': 'tx_queued', 'owner': 3, 'envelope_id': 7, 'elapsed_ns': 40},
                {'stage': 'completion_harvested', 'owner': 3, 'envelope_id': 7, 'elapsed_ns': 100}]},
        ]}
        rows = by_stage(decomposition.owner_timeline(timeline))
        self.assertEqual(rows['owner_handler_ns']['count'], 3)
        self.assertAlmostEqual(rows['owner_handler_ns']['mean_us'], (1000 + 2000 + 3000) / 3 / 1000)
        self.assertEqual(rows['owner_append_wal_ns']['count'], 1)
        self.assertEqual(rows['owner_append_wal_ns']['mean_us'], 0)
        self.assertEqual(rows['owner_append_persist_ns']['count'], 0)
        self.assertIsNone(rows['owner_append_persist_ns']['mean_us'])
        residual = rows['exchange minus owner handler (wire, both network stacks, TLS, dispatch)']
        self.assertEqual(residual['count'], 1)
        self.assertEqual(residual['mean_us'], (15000 - 10000 - 1000) / 1000)
        self.assertEqual(residual['excluded_shared'], 1)
        empty = by_stage(decomposition.owner_timeline({}))
        for row in empty.values():
            self.assertEqual(row['count'], 0)
            self.assertIsNone(row['mean_us'])
            self.assertEqual(row['excluded_shared'], 0)

    def test_native_write_profile_uses_window_counters_not_lifetime(self):
        evidence = {'supported': True, 'runtime_observation_mode': 'runtime_selected_native_scalar_write_stages',
            'selection_policy': 'first N scalar writes',
            'overlap_semantics': 'barrier file clocks overlap barrier', 'selected_operations': 2,
            'stages': {'selected_operations': 2, 'wal_append_ns': 8000, 'index_apply_ns': 2000},
            'value_wal_persistence': {'status': 'sampled', 'profiled_barriers': 2,
                'file_calls': 4, 'file_ns': 20000, 'namespace_calls': 0, 'namespace_ns': 0}}
        rows = decomposition.write_profile(evidence)
        self.assertIsInstance(rows, list)
        stages = by_stage(rows)
        for name, total, count in [('wal_append', 8000, 2), ('index_apply', 2000, 2), ('barrier_file', 20000, 4)]:
            row = stages[name]
            self.assertEqual(row['count'], count)
            self.assertAlmostEqual(row['mean_us'], total / count / 1000)
            self.assertEqual(row['exactness'], 'sampled_mean')
            self.assertTrue(row['selection'])
            self.assertTrue(row['overlap'])

    def test_stalls_are_recomputed_from_successful_completion_gaps(self):
        samples = [sample(1, 1000000), sample(2, 2000000), sample(3, 8000001),
                   sample(4, 9000000), sample(5, 16000000)]
        # A failed completion inside a gap does not count as a successful put.
        failed = sample(6, 4000000)
        failed['outcome'] = 'error'
        samples.append(failed)
        completions = sorted(s['completed_ns'] for s in samples
                             if s['phase'] == 'measurement' and s['operation'] == 'put'
                             and s['outcome'] in ('ok', 'success'))
        gaps = [(b - a) for a, b in zip(completions, completions[1:]) if b - a > 5000000]
        timeline = {'records': [{'caller_sample_id': 3, 'events': [
            {'stage': 'caller_start', 'elapsed_ns': 0},
            {'stage': 'gate_acquired', 'elapsed_ns': 2000},
            {'stage': 'caller_finished', 'elapsed_ns': 9000}]},
            {'caller_sample_id': 5, 'events': [
            {'stage': 'caller_start', 'elapsed_ns': 0},
            {'stage': 'gate_acquired', 'elapsed_ns': 5000},
            {'stage': 'caller_finished', 'elapsed_ns': 6000}]}]}
        result = decomposition.stall(samples, timeline)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['count'], len(gaps))
        self.assertEqual([entry['duration_us'] for entry in result['stalls']], [gap / 1000 for gap in gaps])
        self.assertEqual([entry['cumulative_puts'] for entry in result['stalls']], [3, 5])
        self.assertEqual(result['spacing_puts'], [2])
        self.assertEqual(result['phase_counts'], {'gate_acquired -> caller_finished': 1,
                                                  'caller_start -> gate_acquired': 1})
        fit = result['duration_fit']
        slope = (gaps[1] - gaps[0]) / 1000 / (5 - 3)
        self.assertAlmostEqual(fit['slope_us_per_put'], slope)
        self.assertAlmostEqual(fit['intercept_us'], gaps[0] / 1000 - slope * 3)
        self.assertEqual(fit['count'], 2)


if __name__ == '__main__':
    unittest.main()
