"""Offline timing/population/provenance regression tests; no AWS or workload runs."""
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import analyze
import build


class TimingPopulations(unittest.TestCase):
    def profile(self):
        return dict(read_profile_enabled=1, read_profile_errors=0,
                    read_profile_server_available=1,
                    read_profile_transport_progress_available=1,
                    read_profile_transport_cq_available=0,
                    read_profile_completed=10, read_profile_inline_completed=6,
                    read_profile_actor_completed=4, read_profile_returned=3,
                    read_profile_wire_requests=20, read_profile_inline_ns=6000,
                    read_profile_actor_queue_ns=8000, read_profile_actor_service_ns=12000,
                    read_profile_actor_return_ns=9000, read_profile_exchange_ns=40000,
                    read_profile_server_authority_lookup_ns=60000,
                    read_profile_server_record_read_ns=80000,
                    read_profile_server_response_recheck_ns=10000,
                    read_profile_server_response_recheck_samples=5,
                    read_profile_transport_output_submit_ns=18000,
                    read_profile_transport_output_submit_samples=6,
                    read_profile_transport_cq_call_ns=0,
                    read_profile_transport_cq_timed_calls=0)

    def test_inline_actor_return_wire_and_output_populations_are_distinct(self):
        means = analyze.adapter_means(self.profile())
        for name, expected in [('inline', 1), ('actor_queue', 2), ('actor_service', 3),
                               ('actor_return', 3), ('exchange', 2),
                               ('server_authority_lookup', 3), ('server_record_read', 4),
                               ('server_response_recheck', 2), ('transport_output_submit', 3)]:
            self.assertEqual(means['read_profile_' + name]['mean_us'], expected)
        self.assertIsNone(means['read_profile_transport_cq_call']['mean_us'])
        self.assertEqual(means['read_profile_actor_return']['denominator'], 'read_profile_returned')

    def test_missing_actor_split_is_unavailable_not_all_completions(self):
        stats = self.profile()
        del stats['read_profile_actor_completed']
        self.assertIsNone(analyze.adapter_means(stats)['read_profile_actor_queue']['mean_us'])
        del stats['read_profile_wire_requests']
        self.assertIsNone(analyze.adapter_means(stats)['read_profile_exchange']['mean_us'])

    def test_errors_cannot_dilute_successful_actor_spans(self):
        stats = self.profile() | {'read_profile_errors': 1}
        means = analyze.adapter_means(stats)
        self.assertFalse(means['read_profile_actor_service']['available'])
        self.assertEqual(means['read_profile_inline']['mean_us'], 1)
        self.assertEqual(means['read_profile_exchange']['mean_us'], 2)

    def test_empty_missing_and_nonfinite_spans_are_not_zero(self):
        self.assertIsNone(analyze.mean_span({'n': 0, 't': 0}, 't', 'n')['mean_us'])
        self.assertIsNone(analyze.mean_span({'n': 4}, 't', 'n')['mean_us'])
        for stats in ({'n': 0, 't': 1}, {'n': 1, 't': float('nan')}, {'n': 1.5, 't': 1}):
            with self.assertRaises(ValueError):
                analyze.mean_span(stats, 't', 'n')

    def test_protocol_compatibility_means_are_unavailable_without_mutating_counters(self):
        fields = ('request_send', 'response_header', 'response_body', 'server_inventory_wait')
        stats = self.profile() | dict(write_profile_available=1, write_profile_client_requests=20)
        for prefix in ('read_profile_', 'write_profile_client_'):
            stats.update({prefix + field + '_ns': 0 for field in fields})
        original = stats.copy()
        means = analyze.adapter_means(stats)
        for prefix in ('read_profile_', 'write_profile_client_'):
            for field in fields:
                with self.subTest(prefix=prefix, field=field):
                    row = means[prefix + field]
                    self.assertFalse(row['available'])
                    self.assertIsNone(row['mean_us'])
                    self.assertTrue(row['reason'])
                    self.assertEqual(row['total_ns'], 0)
                    self.assertEqual(row['count'], 20)
                    self.assertEqual(row['numerator'], prefix + field + '_ns')
        self.assertEqual(stats, original)
        self.assertEqual(means['read_profile_exchange']['mean_us'], 2)

    def test_ram_profile_uses_same_compatibility_classification(self):
        stats = dict(requests=5, request_send_ns=0, response_header_ns=0,
                     response_body_ns=0, server_inventory_wait_ns=0,
                     request_encode_ns=0, exchange_ns=25000)
        for field in ('request_send_ns', 'response_header_ns', 'response_body_ns',
                      'server_inventory_wait_ns'):
            row = analyze.multiplexed_profile_mean(stats, field, 'requests')
            self.assertFalse(row['available'])
            self.assertIsNone(row['mean_us'])
            self.assertEqual((row['total_ns'], row['count']), (0, 5))
        zero = analyze.multiplexed_profile_mean(stats, 'request_encode_ns', 'requests')
        self.assertTrue(zero['available'])
        self.assertEqual(zero['mean_us'], 0)
        self.assertEqual(analyze.multiplexed_profile_mean(
            stats, 'exchange_ns', 'requests')['mean_us'], 5)

    def test_structural_zeros_are_not_compatibility_clocks(self):
        stats = self.profile() | dict(read_profile_transport_output_retire_ns=0,
                                     read_profile_transport_output_retire_samples=4,
                                     write_profile_available=1,
                                     write_profile_client_socket_read_ns=0,
                                     write_profile_client_socket_read_calls=0)
        means = analyze.adapter_means(stats)
        retirement = means['read_profile_transport_output_retire']
        self.assertTrue(retirement['available'])
        self.assertEqual((retirement['mean_us'], retirement['count']), (0, 4))
        tls = means['write_profile_client_socket_read']
        self.assertFalse(tls['available'])
        self.assertIsNone(tls['mean_us'])
        self.assertEqual((tls['total_ns'], tls['count']), (0, 0))
        self.assertEqual(stats['write_profile_client_socket_read_calls'], 0)

    def test_scalar_bulk_and_commit_means_use_stage_calls(self):
        stats = dict(remote_timing_available=1, read_calls=100, read_keys=1000,
                     scalar_read_ns=6000, scalar_read_calls=3, scalar_read_ns_available=1,
                     bulk_read_ns=20000, bulk_read_calls=4,
                     write_commit_ns=9000, write_commit_calls=3)
        means = analyze.adapter_means(stats)
        self.assertEqual(means['scalar_read']['mean_us'], 2)
        self.assertEqual(means['bulk_read']['mean_us'], 5)
        self.assertEqual(means['write_commit']['mean_us'], 3)
        self.assertIsNone(analyze.adapter_means({'remote_timing_available': 0})['scalar_read']['mean_us'])

    def test_missing_availability_does_not_preserve_stale_diagnostics(self):
        values = {f'tidehunter_adapter{{namespace="db",stat="{k}"}}': v for k, v in
                  {'shard_cache_hits': 8, 'read_profile_exchange_ns': 100}.items()}
        stats = analyze.adapter_stats(values)['db']
        self.assertIsNone(stats['shard_cache_hits'])
        self.assertIsNone(stats['read_profile_exchange_ns'])
        for enabled in (None, 0, 1):
            raw = dict(remote_timing_available=1, scalar_read_calls=3,
                       scalar_read_ns=9000, read_index_ns=6000, read_index_calls=3)
            if enabled is not None:
                raw.update(scalar_read_ns_available=enabled, read_index_ns_available=enabled)
            metrics = {f'tidehunter_adapter{{namespace="db",stat="{key}"}}': value
                       for key, value in raw.items()}
            decoded = analyze.adapter_stats(metrics)['db']
            means = analyze.adapter_means(decoded)
            for stage, expected in (('scalar_read', 3), ('read_index', 2)):
                if enabled == 1:
                    self.assertEqual(means[stage]['mean_us'], expected)
                else:
                    self.assertIsNone(decoded[stage + '_ns'])
                    self.assertIsNone(means[stage]['mean_us'])

    def test_sui_local_window_absence_is_not_server_drop_completeness(self):
        record = dict(schema=1, reason='profile-disable', window=1, peer=None,
                      data_posts=9, credit_posts=2, piggyback_advances=3)
        payload = b'TIDEHUNTER_FABRIC_LOCAL_DIAGNOSTICS ' + json.dumps(record).encode() + b'\n'
        blob = io.BytesIO()
        with tarfile.open(fileobj=blob, mode='w') as archive:
            for name, data in [('experiment/remote.log', payload),
                               ('experiment/native.log', b'native startup\n'),
                               ('experiment/storage-0.log', b'process terminated by SIGTERM\n')]:
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
        blob.seek(0)
        with tarfile.open(fileobj=blob, mode='r:') as archive:
            evidence = analyze.fabric_local_replay(archive, dict(read_profile_limit=128))
            observed = {row['source']: row for row in evidence['logs']}
            self.assertTrue(observed['experiment/remote.log']['available'])
            self.assertFalse(observed['experiment/storage-0.log']['available'])
            with self.assertRaisesRegex(ValueError, 'clean'):
                analyze.fabric_local_replay(archive, dict(read_profile_limit=0, write_profile_limit=0))

    def test_write_stages_use_their_own_persistence_populations(self):
        stats = dict(remote_timing_available=1, write_profile_available=1,
                     write_profile_sampled_requests=10,
                     write_profile_connection_append_inventory_wait_ns=20000,
                     write_profile_storage_append_calls=10,
                     write_profile_storage_append_total_ns=50000,
                     write_profile_storage_persist_calls=8,
                     write_profile_storage_persist_total_ns=80000,
                     write_profile_storage_file_persist_calls=4,
                     write_profile_storage_persist_wal_file_ns=40000,
                     write_profile_storage_namespace_persist_calls=1,
                     write_profile_storage_persist_namespace_ns=30000,
                     write_profile_storage_inventory_file_persist_calls=2,
                     write_profile_storage_persist_inventory_file_ns=14000,
                     write_profile_storage_inventory_namespace_persist_calls=1,
                     write_profile_storage_persist_inventory_namespace_ns=11000,
                     write_durable_receipt_publication_blocked_batches=2,
                     write_durable_receipt_publication_blocked_ns=12000,
                     write_checkpoint_count=3, write_checkpoint_ns=21000)
        means = analyze.adapter_means(stats)
        for stage, expected in (
                ('write_profile_connection_append_inventory_wait', 2),
                ('write_profile_storage_append_total', 5),
                ('write_profile_storage_persist_total', 10),
                ('write_profile_storage_persist_wal_file', 10),
                ('write_profile_storage_persist_namespace', 30),
                ('write_profile_storage_persist_inventory_file', 7),
                ('write_profile_storage_persist_inventory_namespace', 11),
                ('write_durable_receipt_publication_blocked', 6), ('write_checkpoint', 7)):
            self.assertEqual(means[stage]['mean_us'], expected)
        stats['write_profile_available'] = 0
        self.assertIsNone(analyze.adapter_means(stats)['write_profile_storage_append_total']['mean_us'])

    def test_partial_write_profiles_and_absent_cq_clocks_are_unavailable(self):
        for available in (0, 1):
            values = dict(write_profile_available=available,
                          write_profile_storage_append_total_ns=1000,
                          write_profile_client_transport_cq_timed_calls=0,
                          write_profile_client_transport_cq_call_ns=0,
                          write_profile_connection_receive_frame_including_idle_ns=0)
            stats = analyze.adapter_stats({
                f'tidehunter_adapter{{namespace="db",stat="{k}"}}': v
                for k, v in values.items()})['db']
            self.assertEqual(stats['write_profile_storage_append_total_ns'], 1000 if available else None)
            self.assertIsNone(stats['write_profile_client_transport_cq_call_ns'])
            self.assertIsNone(stats['write_profile_connection_receive_frame_including_idle_ns'])

    def test_native_persistence_uses_actual_file_and_namespace_populations(self):
        stats = dict(remote_timing_available=0, native_profile_available=1,
                     native_profile_barrier_calls=3, native_profile_barrier_ns=24000,
                     native_profile_file_calls=6, native_profile_file_ns=12000,
                     native_profile_namespace_calls=3, native_profile_namespace_ns=9000)
        means = analyze.adapter_means(stats)
        for name, expected in [('barrier', 8), ('file', 2), ('namespace', 3)]:
            self.assertEqual(means['native_profile_' + name]['mean_us'], expected)
        stats['native_profile_available'] = 0
        self.assertIsNone(analyze.adapter_means(stats)['native_profile_file']['mean_us'])


class HistogramPopulations(unittest.TestCase):
    def histogram(self):
        return analyze.parse_metrics('\n'.join([
            'rocksdb_get_latency_seconds_sum{cf_name="objects"} 0.0004',
            'rocksdb_get_latency_seconds_count{cf_name="objects"} 4',
            'rocksdb_get_latency_seconds_bucket{le="0.00001",cf_name="objects"} 2',
            'rocksdb_get_latency_seconds_bucket{cf_name="objects",le="0.00005"} 3',
            'rocksdb_get_latency_seconds_bucket{cf_name="objects",le="+Inf"} 4']))

    def test_histogram_bounds_are_not_exact_quantiles(self):
        row, = analyze.wrapper_latencies(self.histogram())
        self.assertEqual(row['mean_us'], 100)
        self.assertEqual(row['quantiles']['p50']['lower_us'], 0)
        self.assertEqual(row['quantiles']['p50']['upper_us'], 10)
        self.assertEqual(row['quantiles']['p95']['lower_us'], 50)
        self.assertIsNone(row['quantiles']['p95']['upper_us'])
        self.assertTrue(row['quantiles']['p95']['upper_unbounded'])

    def population_histogram(self, count):
        return analyze.parse_metrics('\n'.join([
            f'rocksdb_get_latency_seconds_sum{{cf_name="objects"}} {count * 0.00002}',
            f'rocksdb_get_latency_seconds_count{{cf_name="objects"}} {count}',
            f'rocksdb_get_latency_seconds_bucket{{le="0.00001",cf_name="objects"}} {count - 1}',
            f'rocksdb_get_latency_seconds_bucket{{le="0.00005",cf_name="objects"}} {count}',
            f'rocksdb_get_latency_seconds_bucket{{le="+Inf",cf_name="objects"}} {count}']))

    def test_p99_requires_100_observations_in_its_own_histogram(self):
        for count in (1, 3, 99, 100):
            with self.subTest(count=count):
                row, = analyze.wrapper_latencies(self.population_histogram(count))
                self.assertTrue(row['available'])
                self.assertAlmostEqual(row['mean_us'], 20)
                tail = row['quantiles']['p99']
                self.assertEqual(tail['available'], count >= 100)
                if count < 100:
                    self.assertIsNone(tail['lower_us'])
                    self.assertIsNone(tail['upper_us'])
                    self.assertFalse(tail['upper_unbounded'])
                    self.assertIsNotNone(tail['reason'])
                else:
                    self.assertEqual(tail['lower_us'], 0)
                    self.assertEqual(tail['upper_us'], 10)
                    self.assertIsNone(tail['reason'])
        row, = analyze.wrapper_latencies(self.population_histogram(99))
        for percentile in ('p50', 'p95'):
            self.assertTrue(row['quantiles'][percentile]['available'])
            self.assertEqual(row['quantiles'][percentile]['upper_us'], 10)

    def test_p99_does_not_pool_distinct_histogram_populations(self):
        metrics = self.population_histogram(99)
        metrics.update({
            name.replace('cf_name="objects"', 'cf_name="certified_checkpoints"'): value
            for name, value in self.population_histogram(3).items()
        })
        rows = analyze.wrapper_latencies(metrics)
        self.assertEqual(sorted(row['count'] for row in rows), [3, 99])
        for row in rows:
            self.assertTrue(row['available'])
            self.assertFalse(row['quantiles']['p99']['available'])
            self.assertIsNone(row['quantiles']['p99']['upper_us'])

    def test_missing_buckets_preserve_mean_but_not_quantiles(self):
        metrics = {k: v for k, v in self.histogram().items() if '_bucket' not in k}
        row, = analyze.wrapper_latencies(metrics)
        self.assertEqual(row['mean_us'], 100)
        self.assertFalse(row['quantiles']['p50']['available'])
        metrics = {k: v for k, v in self.population_histogram(100).items() if '_bucket' not in k}
        row, = analyze.wrapper_latencies(metrics)
        self.assertAlmostEqual(row['mean_us'], 20)
        self.assertFalse(row['quantiles']['p99']['available'])
        self.assertIsNone(row['quantiles']['p99']['upper_us'])
        self.assertIsNotNone(row['quantiles']['p99']['reason'])

    def test_empty_and_missing_count_histograms_are_unavailable(self):
        row, = analyze.wrapper_latencies(dict.fromkeys(self.histogram(), 0))
        self.assertIsNone(row['mean_us'])
        self.assertFalse(row['quantiles']['p99']['available'])
        self.assertIsNone(row['quantiles']['p99']['upper_us'])
        self.assertIsNotNone(row['quantiles']['p99']['reason'])
        row, = analyze.wrapper_latencies({k: v for k, v in self.histogram().items() if '_count' not in k})
        self.assertIsNone(row['mean_us'])
        self.assertFalse(row['quantiles']['p50']['available'])
        self.assertFalse(row['quantiles']['p99']['available'])
        self.assertIsNone(row['quantiles']['p99']['upper_us'])
        self.assertIsNotNone(row['quantiles']['p99']['reason'])

    def test_corrupt_buckets_and_duplicate_series_rejected(self):
        metrics = self.histogram()
        key = next(k for k in metrics if 'le="0.00005"' in k)
        metrics[key] = 1
        with self.assertRaisesRegex(ValueError, 'decreasing'):
            analyze.wrapper_latencies(metrics)
        with self.assertRaises(ValueError):
            analyze.parse_metrics('rocksdb_x 1\nrocksdb_x 2\n')


class CheckpointSequenceCacheEvidence(unittest.TestCase):
    def test_absent_is_unavailable_and_partial_or_ambiguous_counts_fail(self):
        self.assertEqual(analyze.checkpoint_sequence_cache_stats({}), {})
        partial = {'checkpoint_sequence_lookup_cache{stat="hits"}': 3}
        with self.assertRaises(ValueError):
            analyze.checkpoint_sequence_cache_stats(partial)
        complete = {f'checkpoint_sequence_lookup_cache{{stat="{name}"}}': 0
                    for name in ('hits', 'misses', 'backend_gets', 'busy_bypasses')}
        for value in (-1, 1.5):
            changed = complete | {'checkpoint_sequence_lookup_cache{stat="hits"}': value}
            with self.subTest(value=value), self.assertRaises(ValueError):
                analyze.checkpoint_sequence_cache_stats(changed)
        duplicate = complete | {'checkpoint_sequence_lookup_cache{stat="hits",copy="other"}': 0}
        with self.assertRaises(ValueError):
            analyze.checkpoint_sequence_cache_stats(duplicate)


class CampaignComparison(unittest.TestCase):
    def cells(self):
        cells = []
        for repetition, limit in enumerate([0, 4096, 4096, 0], 1):
            order = ['tcp', 'efa'] if repetition % 2 else ['efa', 'tcp']
            for transport in order:
                native = 2 if transport == 'tcp' else 3
                remote = native * (2 if not limit else 3)
                cells.append(dict(cell_id=f'r{repetition:02d}-{transport}', ordinal=len(cells),
                                  repetition=repetition, transport=transport, read_profile_limit=limit,
                                  population='profiled' if limit else 'clean', native_seconds=native,
                                  remote_seconds=remote, remote_over_native=remote/native))
        return cells

    def test_eight_cells_keep_clean_profiled_and_native_controls(self):
        result = analyze.population_comparison(self.cells())
        for transport in ('tcp', 'efa'):
            self.assertEqual(result[transport]['populations']['clean']['n'], 2)
            self.assertEqual(result[transport]['populations']['profiled']['n'], 2)
            self.assertEqual(result[transport]['observed_profile_effect']['remote_seconds']['observed_change_percent'], 50)
        self.assertEqual(result['tcp']['populations']['clean']['native_seconds']['samples'], [2, 2])
        self.assertEqual(result['efa']['populations']['clean']['native_seconds']['samples'], [3, 3])
        pairs = analyze.transport_comparison(self.cells())['pairs']
        self.assertEqual([p['transport_order'] for p in pairs], [['tcp', 'efa'], ['efa', 'tcp']] * 2)
    def test_triplets_label_each_pair_and_retain_each_native_control(self):
        cells = self.cells()
        for repetition in range(1, 5):
            cells.append(dict(cell_id=f'r{repetition:02d}-efa-direct', ordinal=len(cells),
                repetition=repetition, transport='efa-direct',
                read_profile_limit=4096 if repetition in (2, 3) else 0,
                population='profiled' if repetition in (2, 3) else 'clean',
                native_seconds=4, remote_seconds=4, remote_over_native=1))
        populations = analyze.population_comparison(cells)
        self.assertEqual(set(populations), {'tcp', 'efa', 'efa-direct'})
        self.assertEqual(populations['efa-direct']['populations']['clean']['native_seconds']['samples'], [4, 4])
        pairs = analyze.transport_comparison(cells)['pairs']
        self.assertEqual(len(pairs), 12)
        for repetition in range(1, 5):
            rows = [pair for pair in pairs if pair['repetition'] == repetition]
            self.assertEqual({(pair['reference_transport'], pair['candidate_transport']) for pair in rows},
                             {('tcp', 'efa'), ('tcp', 'efa-direct'), ('efa', 'efa-direct')})
            for pair in rows:
                reference = next(cell for cell in cells if cell['cell_id'] == pair['reference_cell'])
                candidate = next(cell for cell in cells if cell['cell_id'] == pair['candidate_cell'])
                self.assertEqual(pair['candidate_over_reference_elapsed'],
                                 candidate['remote_seconds'] / reference['remote_seconds'])
                self.assertEqual(pair['reference_native_seconds'], reference['native_seconds'])
                self.assertEqual(pair['candidate_native_seconds'], candidate['native_seconds'])
                self.assertEqual(pair['candidate_over_reference_native_normalized'],
                                 candidate['remote_over_native'] / reference['remote_over_native'])
        missing = [cell for cell in cells if cell['cell_id'] != 'r01-efa-direct']
        unavailable = [pair for pair in analyze.transport_comparison(missing)['pairs'] if not pair['available']]
        self.assertEqual(len(unavailable), 2)
        self.assertTrue(all(pair['candidate_transport'] == 'efa-direct' for pair in unavailable))

    def test_native_efa_keeps_its_own_controls_and_all_matched_pairs(self):
        cells = self.cells()
        for transport, native, remote in (('efa-direct', 4, 8), ('efa-native', 5, 5)):
            for repetition in range(1, 5):
                cells.append(dict(cell_id=f'r{repetition:02d}-{transport}', ordinal=len(cells),
                    repetition=repetition, transport=transport,
                    read_profile_limit=4096 if repetition in (2, 3) else 0,
                    population='profiled' if repetition in (2, 3) else 'clean',
                    native_seconds=native, remote_seconds=remote, remote_over_native=remote/native))
        pairs = analyze.transport_comparison(cells)['pairs']
        native_pairs = [pair for pair in pairs if pair['candidate_transport'] == 'efa-native']
        self.assertEqual(len(native_pairs), 12)
        self.assertTrue(all(pair['candidate_native_seconds'] == 5 for pair in native_pairs))
        direct_pairs = [pair for pair in native_pairs if pair['reference_transport'] == 'efa-direct']
        self.assertEqual([pair['candidate_over_reference_elapsed'] for pair in direct_pairs], [5/8] * 4)
        self.assertEqual([pair['candidate_over_reference_native_normalized'] for pair in direct_pairs], [1/2] * 4)
        missing = [cell for cell in cells if cell['cell_id'] != 'r01-efa-native']
        unavailable = [pair for pair in analyze.transport_comparison(missing)['pairs'] if not pair['available']]
        self.assertEqual(len(unavailable), 3)
        self.assertTrue(all(pair['candidate_transport'] == 'efa-native' for pair in unavailable))

    def test_duplicate_and_cross_source_or_population_comparisons_fail(self):
        cells = self.cells()
        with self.assertRaises(ValueError):
            analyze.transport_comparison(cells + [cells[0]])
        for field, value in (('source_range', {'first': 42}), ('source_archive_sha256', 'foreign'),
                             ('source_manifest_sha256', 'foreign'), ('build_manifest_sha256', 'foreign'),
                             ('checkpoint_digest', 'foreign'), ('durability', 'not-durable'),
                             ('table_counts', {'wrong': 1}), ('population', 'profiled')):
            changed = self.cells()
            changed[0][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'populations differ'):
                analyze.transport_comparison(changed)
            if field != 'population':
                with self.assertRaisesRegex(ValueError, 'source/workload populations'):
                    analyze.population_comparison(changed)


    def test_missing_profiled_population_is_not_zero_overhead(self):
        cells = [c for c in self.cells() if c['population'] == 'clean']
        effect = analyze.population_comparison(cells)['tcp']['observed_profile_effect']['remote_seconds']
        self.assertFalse(effect['available'])
        self.assertIsNone(effect['observed_change_percent'])
        self.assertFalse(analyze.transport_comparison(cells[:1])['pairs'][0]['available'])

    def test_transport_pairs_reject_different_profile_limits(self):
        for field in ('read_profile_limit', 'write_profile_limit', 'native_write_profile_limit'):
            cells = self.cells()
            cells[0][field] = 1
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'populations differ'):
                analyze.transport_comparison(cells)

    def test_different_cache_budgets_cannot_be_pooled_or_transport_paired(self):
        cells = self.cells()
        for cell in cells:
            cell['cache'] = dict(coordinator_bytes=8388608, shard_bytes=0)
        cells[2]['cache']['shard_bytes'] = 16 * 1024**2
        with self.assertRaises(ValueError):
            analyze.population_comparison(cells)
        with self.assertRaises(ValueError):
            analyze.transport_comparison(cells)

    def test_different_write_sampling_windows_cannot_be_pooled(self):
        cells = self.cells()
        for cell in cells:
            cell['write_profile_limit'] = 128 if cell['read_profile_limit'] else 0
        cells[4]['write_profile_limit'] = 256
        with self.assertRaisesRegex(ValueError, 'different bounded profiling limits'):
            analyze.population_comparison(cells)


class CampaignProvenance(unittest.TestCase):
    def cfg(self):
        return dict(run_id='th-test', coordinator_instance_id='controller',
                    nodes=[dict(instance_id=f'node-{i}', partition=i) for i in range(5)],
                    build_manifest_sha256='build', checkpoint_digest='tip')

    def closure(self, cfg):
        identities = ['th-test-controller'] + [f'th-test-storage-{i}' for i in range(5)]
        return dict(run_id=cfg['run_id'], verified_utc='2026-10-13T12:00:00Z',
                    instances=dict.fromkeys(['controller'] + [n['instance_id'] for n in cfg['nodes']], 'terminated'),
                    interfaces=[], volumes=[], bucket='404', security_group='InvalidGroup.NotFound',
                    placement_group='InvalidPlacementGroup.Unknown', roles=dict.fromkeys(identities, 'NoSuchEntity'),
                    profiles=dict.fromkeys(identities, 'NoSuchEntity'))

    def fixture(self, root):
        cfg = self.cfg()
        build.save(root / 'controller-config.json', cfg)
        build.save(root / 'cleanup-verified.json', self.closure(cfg))
        (root / 'retrieved').mkdir()
        payload = json.dumps(cfg).encode()
        with tarfile.open(root / 'retrieved/controller.tar.gz', 'w:gz') as archive:
            member = tarfile.TarInfo('results/campaign-config.json')
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))
        build.save(root / 'retrieved/receipt.json', dict(complete=True, run_id=cfg['run_id'],
                   sha256=build.digest(root / 'retrieved/controller.tar.gz'),
                   build_manifest_sha256='build', checkpoint_digest='tip'))
        return cfg

    def test_closure_requires_all_owned_resources_absent(self):
        cfg = self.cfg()
        closure = self.closure(cfg)
        analyze.validate_closure(closure, cfg)
        for field, value in [('run_id', 'other'), ('bucket', '403'), ('interfaces', ['eni-live']),
                             ('roles', {}), ('volumes', None), ('instances', {'controller': 'terminated'})]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                analyze.validate_closure(closure | {field: value}, cfg)

    def test_unclosed_campaign_is_rejected_before_replay_verifier(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = self.fixture(root)
            closure = self.closure(cfg)
            closure['instances']['controller'] = 'running'
            build.save(root / 'cleanup-verified.json', closure)
            with patch.object(analyze, 'verified_replay') as replay:
                with self.assertRaisesRegex(ValueError, 'closed before analysis'):
                    analyze.verified_campaign(root)
                replay.assert_not_called()

    def test_controller_archive_digest_is_checked_before_analysis(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            with (root / 'retrieved/controller.tar.gz').open('ab') as stream:
                stream.write(b'tampered')
            with self.assertRaisesRegex(ValueError, 'receipt/provenance differs'):
                analyze.verified_campaign(root)

    def test_local_configuration_cannot_relabel_archived_population(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = self.fixture(root)
            build.save(root / 'controller-config.json', cfg | {'read_profile_limits': [0, 4096, 4096, 0]})
            with self.assertRaisesRegex(ValueError, 'configuration differs'):
                analyze.verified_campaign(root)

    def test_native_internal_and_physical_populations_not_invented(self):
        self.assertFalse(analyze.storage_io({})['available'])
        self.assertFalse(analyze.adapter_means({'remote_timing_available': 0})['write_commit']['available'])


if __name__ == '__main__':
    unittest.main()
