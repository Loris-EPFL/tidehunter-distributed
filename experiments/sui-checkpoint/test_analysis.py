"""Offline timing/population/provenance regression tests; no AWS or workload runs."""
import unittest

import analyze


class TimingPopulations(unittest.TestCase):
    def profile(self):
        return dict(read_profile_enabled=1, read_profile_errors=0,
                    read_profile_server_available=1,
                    read_profile_transport_progress_available=1,
                    read_profile_transport_cq_available=0,
                    read_profile_completed=10, read_profile_inline_completed=6,
                    read_profile_actor_completed=4,
                    read_profile_nvme_completed=4, read_profile_nvme_flight_count=4,
                    read_profile_inline_ns=6000, read_profile_nvme_flight_ns=8000,
                    write_profile_available=1, write_profile_client_requests=20)

    def test_inline_and_flight_populations_are_distinct(self):
        means = analyze.adapter_means(self.profile())
        for name, expected in [('inline', 1), ('nvme_flight', 2)]:
            self.assertEqual(means['read_profile_' + name]['mean_us'], expected)
        self.assertEqual(means['read_profile_inline']['denominator'],
                         'read_profile_inline_completed')
        self.assertEqual(means['read_profile_nvme_flight']['denominator'],
                         'read_profile_nvme_flight_count')

    def test_missing_actor_split_is_unavailable_not_all_completions(self):
        stats = self.profile()
        del stats['read_profile_inline_completed']
        self.assertIsNone(analyze.adapter_means(stats)['read_profile_inline']['mean_us'])
        del stats['read_profile_nvme_flight_count']
        self.assertIsNone(analyze.adapter_means(stats)['read_profile_nvme_flight']['mean_us'])

    def test_sampled_errors_do_not_suppress_the_caller_inline_span(self):
        stats = self.profile() | {'read_profile_errors': 1}
        means = analyze.adapter_means(stats)
        self.assertEqual(means['read_profile_inline']['mean_us'], 1)
        self.assertEqual(means['read_profile_nvme_flight']['mean_us'], 2)

    def test_empty_missing_and_nonfinite_spans_are_not_zero(self):
        self.assertIsNone(analyze.mean_span({'n': 0, 't': 0}, 't', 'n')['mean_us'])
        self.assertIsNone(analyze.mean_span({'n': 4}, 't', 'n')['mean_us'])
        for stats in ({'n': 0, 't': 1}, {'n': 1, 't': float('nan')}, {'n': 1.5, 't': 1}):
            with self.assertRaises(ValueError):
                analyze.mean_span(stats, 't', 'n')

    def test_protocol_compatibility_means_are_unavailable_without_mutating_counters(self):
        fields = ('request_send', 'response_header', 'response_body', 'server_inventory_wait')
        stats = self.profile()
        stats.update({'write_profile_client_' + field + '_ns': 0 for field in fields})
        stats.update({'write_profile_client_exchange_ns': 40000})
        original = stats.copy()
        means = analyze.adapter_means(stats)
        for field in fields:
            with self.subTest(field=field):
                row = means['write_profile_client_' + field]
                self.assertFalse(row['available'])
                self.assertIsNone(row['mean_us'])
                self.assertTrue(row['reason'])
                self.assertEqual(row['total_ns'], 0)
                self.assertEqual(row['count'], 20)
                self.assertEqual(row['numerator'], 'write_profile_client_' + field + '_ns')
        self.assertEqual(stats, original)
        self.assertEqual(means['write_profile_client_exchange']['mean_us'], 2)

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
        stats = self.profile() | dict(write_profile_client_transport_output_retire_ns=0,
                                      write_profile_client_transport_output_retire_samples=4,
                                      write_profile_client_socket_read_ns=0,
                                      write_profile_client_socket_read_calls=0)
        means = analyze.adapter_means(stats)
        retirement = means['write_profile_client_transport_output_retire']
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
                  {'cache_hits': 8, 'read_profile_exchange_ns': 100}.items()}
        stats = analyze.adapter_stats(values)['db']
        self.assertEqual(stats['cache_hits'], 8)
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

    def test_wrapper_tables_label_percentiles_as_intervals(self):
        table = analyze.wrapper_latency_table(self.histogram())
        row, = table['rows']
        self.assertEqual(row['op'], 'get')
        self.assertEqual(row['labels'], {'cf_name': 'objects'})
        self.assertEqual(row['quantiles']['p50']['interval_us'], [0, 10])
        self.assertEqual(row['quantiles']['p95']['interval_us'], [50, None])
        self.assertTrue(row['quantiles']['p95']['upper_unbounded'])
        comparison = analyze.wrapper_latency_comparison(
            {'native': self.histogram(), 'remote': self.population_histogram(100)})
        row, = comparison['rows']
        self.assertEqual(row['op'], 'get')
        self.assertEqual(row['native']['quantiles']['p50']['interval_us'], [0, 10])
        self.assertEqual(row['remote']['quantiles']['p99']['interval_us'], [0, 10])


class DbOpRecordingGate(unittest.TestCase):
    def scrape(self, *, db_count=3, adapter_reads=5, adapter=''):
        lines = [
            'tidehunter_nvmeof_live_completed_read_ops_total 10',
            f'db_op_count{{op="get",ks="store"}} {db_count}',
            f'db_op_sum{{op="get",ks="store"}} {db_count * 10}',
            f'db_op_bucket{{op="get",ks="store",le="+Inf"}} {db_count}',
            'large_table_contention_count{ks="store"} 0',
            'large_table_contention_sum{ks="store"} 0',
        ]
        if adapter_reads is not None:
            lines.append('tidehunter_adapter{namespace="live/store",stat="read_calls"} '
                         + str(adapter_reads))
        if adapter:
            lines.append(adapter)
        return analyze.parse_metrics('\n'.join(lines) + '\n')

    def test_zero_db_op_with_adapter_reads_is_a_typed_defect(self):
        with self.assertRaises(analyze.DbOpNotRecorded) as raised:
            analyze.metric_cut(self.scrape(db_count=0), {'live/store'}, remote=True)
        self.assertEqual(raised.exception.fact['state'], 'zero')
        self.assertEqual(raised.exception.fact['adapter_read_calls'], 5)
        self.assertIn('db_op not recorded', str(raised.exception))

    def test_absent_db_op_with_adapter_reads_is_a_typed_defect(self):
        values = self.scrape(db_count=0)
        values = {k: v for k, v in values.items() if not k.startswith('db_op_')}
        with self.assertRaises(analyze.DbOpNotRecorded) as raised:
            analyze.metric_cut(values, {'live/store'}, remote=True)
        self.assertEqual(raised.exception.fact['state'], 'absent')

    def test_recorded_db_op_passes_the_gate(self):
        cut = analyze.metric_cut(self.scrape(), {'live/store'}, remote=True)
        self.assertTrue(cut['coordinator']['db_op']['recorded'])
        self.assertEqual(cut['coordinator']['db_op']['observations'], 3)

    def test_idle_scrape_without_reads_is_not_a_defect(self):
        cut = analyze.metric_cut(self.scrape(db_count=0, adapter_reads=None),
                                 {'live/store'}, remote=True)
        self.assertEqual(cut['coordinator']['db_op']['state'], 'zero')
        self.assertFalse(cut['coordinator']['db_op']['recorded'])

    def test_fraction_names_series_present_with_zero_observations(self):
        values = self.scrape(db_count=0)
        fraction = analyze.owner_fetch_fraction(
            analyze.live_nvmeof_stats(values), analyze.coordinator_stats(values),
            analyze.adapter_stats(values))
        self.assertTrue(fraction['available'])
        self.assertEqual(fraction['api_gets'], 5)
        self.assertEqual(fraction['source'],
                         'adapter read_calls; db_op series are registered with zero observations')

    def test_fraction_names_absent_series(self):
        values = self.scrape(db_count=0)
        values = {k: v for k, v in values.items() if not k.startswith('db_op_')}
        fraction = analyze.owner_fetch_fraction(
            analyze.live_nvmeof_stats(values), analyze.coordinator_stats(values),
            analyze.adapter_stats(values))
        self.assertEqual(fraction['api_gets'], 5)
        self.assertEqual(fraction['source'],
                         'adapter read_calls; no db_op series was published')


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


class CheckpointCompletionHistogram(unittest.TestCase):
    def values(self, count=200):
        return analyze.parse_metrics(
            f'checkpoint_completion_latency_bucket{{le="0.01"}} {count // 2}\n'
            f'checkpoint_completion_latency_bucket{{le="0.1"}} {count}\n'
            f'checkpoint_completion_latency_bucket{{le="+Inf"}} {count}\n'
            f'checkpoint_completion_latency_sum {count / 50}\n'
            f'checkpoint_completion_latency_count {count}\n')

    def test_completion_timer_has_its_own_actual_count_and_noninterpolated_bounds(self):
        result = analyze.checkpoint_completion_observation(self.values(), required=True)
        self.assertEqual(result['metric'], 'checkpoint_completion_latency')
        self.assertEqual(result['count'], 200)
        self.assertEqual(result['total_seconds'], 4)
        self.assertAlmostEqual(result['mean_us'], 20000)
        self.assertEqual(result['quantiles']['p50']['upper_us'], 10000)
        self.assertEqual(result['quantiles']['p99']['lower_us'], 10000)
        self.assertEqual(result['quantiles']['p99']['upper_us'], 100000)
        self.assertEqual(analyze.wrapper_latencies(self.values()), [])
        self.assertIn('pipeline wait', result['scope'])
        self.assertIn('not certification arrival', result['scope'])

    def test_historical_exec_timer_is_not_a_completion_latency_substitute(self):
        old = {key.replace('checkpoint_completion_latency', 'checkpoint_exec_latency'): value
               for key, value in self.values().items()}
        result = analyze.checkpoint_completion_observation(old)
        self.assertFalse(result['available'])
        self.assertIsNone(result['count'])
        with self.assertRaises(ValueError):
            analyze.checkpoint_completion_observation(old, required=True)
        self.assertFalse(analyze.checkpoint_completion_observation(self.values(0))['available'])

    def test_small_actual_population_does_not_become_a_checkpoint_count_or_p99_assumption(self):
        result = analyze.checkpoint_completion_observation(self.values(2), required=True)
        self.assertEqual(result['count'], 2)
        self.assertFalse(result['quantiles']['p99']['available'])
        self.assertIsNone(result['quantiles']['p99']['upper_us'])
        self.assertIn('not assumed equal', result['count_contract'])

    def test_nonfinite_values_and_incomplete_buckets_fail_strict_admission(self):
        for kind in ('nan', 'infinite', 'missing_inf', 'wrong_inf', 'decreasing'):
            with self.subTest(kind=kind):
                values = self.values()
                if kind == 'nan':
                    values['checkpoint_completion_latency_sum'] = float('nan')
                elif kind == 'infinite':
                    values['checkpoint_completion_latency_count'] = float('inf')
                elif kind == 'missing_inf':
                    del values['checkpoint_completion_latency_bucket{le="+Inf"}']
                elif kind == 'wrong_inf':
                    values['checkpoint_completion_latency_bucket{le="+Inf"}'] = 199
                else:
                    values['checkpoint_completion_latency_bucket{le="0.1"}'] = 1
                with self.assertRaises(ValueError):
                    analyze.checkpoint_completion_observation(values, required=True)

    def test_different_histogram_series_are_not_pooled_or_selected_for_completion_proof(self):
        values = {}
        for label in ('a', 'b'):
            for key, value in self.values(60).items():
                name = (key.replace('{', '{node="' + label + '",', 1)
                        if '{' in key else key + '{node="' + label + '"}')
                values[name] = value
        result = analyze.checkpoint_completion_observation(values)
        self.assertFalse(result['available'])
        self.assertIsNone(result['count'])
        with self.assertRaises(ValueError):
            analyze.checkpoint_completion_observation(values, required=True)


DATABASES = ['checkpoint', 'epoch', 'perpetual']


class CoordinatorPerDatabase(unittest.TestCase):
    def test_three_databases_do_not_pool_or_collide(self):
        values = {}
        for database in DATABASES:
            for kind, value in (('count', 2), ('sum', 20)):
                values[f'db_op_{kind}{{db="{database}",ks="live/store",op="get"}}'] = value
                values[f'lookup_mcs_{kind}{{db="{database}",ks="live/store",type="lookup"}}'] = value
                values[f'large_table_contention_{kind}{{db="{database}",ks="live/store"}}'] = value
                values[f'index_blob_cache_contention_{kind}{{db="{database}"}}'] = value
                values[f'flush_count{{db="{database}",ks="live/store"}}'] = value
        stats = analyze.coordinator_stats(values)
        self.assertTrue(stats['available'])
        self.assertEqual(sorted(stats['operations']), ['checkpoint', 'epoch', 'perpetual'])
        for database in stats['operations'].values():
            self.assertEqual(database['live/store']['get']['mean_us'], 10)
        self.assertEqual(sorted(stats['large_table_contention']), DATABASES)
        self.assertEqual(sorted(stats['index_blob_cache_contention']), DATABASES)
        self.assertEqual(sorted(stats['writes']), DATABASES)
        for database in stats['writes'].values():
            self.assertIn('flush_count', database['live/store'])
        for database in stats['large_table_contention'].values():
            self.assertEqual(database['live/store']['mean_us'], 10)
        self.assertEqual(stats['keyspaces'], ['live/store'])
        self.assertEqual(stats['databases'], ['checkpoint', 'epoch', 'perpetual'])
        fraction = analyze.owner_fetch_fraction(
            {'completed_read_ops': 6}, stats)
        self.assertTrue(fraction['available'])
        self.assertEqual(fraction['api_gets'], 6)
        self.assertEqual(fraction['fraction'], 1)
        with self.assertRaisesRegex(ValueError, 'duplicate db_op'):
            analyze.coordinator_stats(values | {
                'db_op_count{db="epoch",ks="live/store",op="get",node="x"}': 3})


class AggregateReplacement(unittest.TestCase):
    def test_aggregate_families_are_the_current_evidence(self):
        for name in ('live_nvmeof_stats', 'live_nvmeof_deltas', 'coordinator_stats',
                     'metric_cut', 'metric_cut_progressed'):
            self.assertTrue(callable(getattr(analyze, name)), name)
        for name in ('shard_read_stats', 'shard_read_deltas', 'operational_read_io_stats',
                     'current_metric_streams', 'validate_closure', 'compare_campaign',
                     'population_comparison', 'transport_comparison', 'storage_io',
                     'fabric_local_replay', 'checkpoint_matrix_helpers'):
            self.assertFalse(hasattr(analyze, name), name)

    def test_native_diagnostics_are_not_zero_latency(self):
        self.assertFalse(analyze.adapter_means({'remote_timing_available': 0})['write_commit']['available'])

    def test_per_database_live_family_is_pooled_to_the_host_total(self):
        values = analyze.parse_metrics('\n'.join([
            'tidehunter_nvmeof_live_completed_read_ops_total{db="checkpoint"} 7',
            'tidehunter_nvmeof_live_completed_read_ops_total{db="epoch"} 3',
            'tidehunter_nvmeof_live_target_observed_partitions{db="checkpoint"} 2',
            'tidehunter_nvmeof_live_target_observed_partitions{db="epoch"} 2',
            'tidehunter_nvmeof_live_lane_inflight_requests_max{db="checkpoint"} 4',
            'tidehunter_nvmeof_live_lane_inflight_requests_max{db="epoch"} 9',
        ]) + '\n')
        stats = analyze.live_nvmeof_stats(values)
        self.assertEqual(stats['counters']['completed_read_ops_total'], 10)
        self.assertEqual(stats['counters']['target_observed_partitions'], 4)
        self.assertEqual(stats['counters']['lane_inflight_requests_max'], 9)
        with self.assertRaisesRegex(ValueError, 'duplicate aggregate'):
            analyze.live_nvmeof_stats(values | {
                'tidehunter_nvmeof_live_completed_read_ops_total{partition="0"}': 4})


NAMESPACE = 'live/db'


def scrape(*, cache_mib=0, hits=0, misses=5, batches=6, live=True, protect=2):
    """One measured-node scrape with the adapter, keyspace and live families."""
    rows = [
        f'tidehunter_adapter{{namespace="{NAMESPACE}",stat="{name}"}} {value}'
        for name, value in (
            ('value_cache_hits', hits), ('value_cache_misses', misses),
            ('cache_hits', hits), ('cache_misses', misses),
            ('cache_entries', 0), ('cache_budget_bytes', cache_mib * 1024 * 1024),
            ('read_calls', misses), ('read_keys', misses), ('read_rpcs', misses),
            ('write_commit_calls', batches))]
    rows += [
        f'tidehunter_keyspace_read_cache{{keyspace="objects",keyspace_id="3",'
        f'namespace="{NAMESPACE}",stat="{name}"}} {value}'
        for name, value in (('hits', hits), ('misses', misses),
                            ('inserts', 0), ('evictions', 0))]
    if live:
        rows += [
            f'tidehunter_nvmeof_live_{name} {value}'
            for name, value in (
                ('completed_read_ops_total', 4), ('completed_write_ops_total', 5),
                ('namespace_connects_total', 2), ('target_observed_partitions', 2),
                ('target_fixture_partitions', 0), ('target_owned_namespaces', 2),
                ('target_exposed_total', 2), ('target_write_protect_total', protect),
                ('target_errors_total', 0), ('lease_exchanges_total', 3))]
    return analyze.parse_metrics('\n'.join(rows) + '\n')


class PerCellCriteria(unittest.TestCase):
    """A criterion is pass, fail, missing counter or explicitly not exported."""

    def result(self, cell_id, **overrides):
        value = dict(cell_id=cell_id, live_transport='rdma',
                     cache=dict(coordinator_bytes=0), checkpoint_limit=1,
                     hold=None)
        value.update(overrides)
        return value

    def checks(self, cell_id, values, **overrides):
        return analyze.cell_checks(self.result(cell_id, **overrides),
                                   {'remote': values}, [NAMESPACE],
                                   checkpoint_count=2)

    def test_the_admission_cell_proves_the_rdma_live_path_and_lists_the_rest(self):
        for cell_id in ('S1-0-native', 'S1-0-synchronous'):
            with self.subTest(cell=cell_id):
                checks = self.checks(cell_id, scrape())
                self.assertEqual(checks['failures'], [])
                admission = checks['checks']['admission']
                self.assertEqual(admission['rdma_live_path']['state'], 'pass')
                self.assertEqual(admission['write_protect_readback']['state'], 'pass')
                self.assertEqual(admission['lease_protocol']['state'], 'not_exported')
                self.assertEqual(admission['no_tcp_fallback']['state'], 'not_exported')
                self.assertEqual(admission['orphan_export']['state'], 'not_exported')
                for key in ('lease_protocol', 'no_tcp_fallback', 'orphan_export'):
                    self.assertIn(f'admission.{key}', checks['not_exported'])

    def test_a_missing_live_family_fails_the_admission_cell(self):
        checks = self.checks('S1-0-native', scrape(live=False))
        self.assertTrue(any('admission' in item for item in checks['failures']))
        self.assertEqual(checks['checks']['admission']['rdma_live_path']['state'],
                         'missing_counter')

    def test_a_fixture_target_never_passes_an_admission_cell(self):
        values = scrape()
        values['tidehunter_nvmeof_live_target_fixture_partitions'] = 2
        checks = self.checks('S1-0-synchronous', values)
        self.assertTrue(any('fixture target' in item for item in checks['failures']))
        self.assertEqual(checks['checks']['admission']['rdma_live_path']['state'], 'fail')

    def test_a_cache_off_cell_must_record_no_hit(self):
        self.assertEqual(self.checks('S1-7-cache0-native', scrape())['failures'], [])
        checks = self.checks('S1-7-cache0-native', scrape(hits=3))
        self.assertTrue(any('disabled coordinator read cache' in item
                            for item in checks['failures']))

    def test_a_cache_on_cell_needs_a_hit_and_a_budget(self):
        checks = self.checks('S1-7-cache1000-native',
                             scrape(hits=2, misses=3, cache_mib=1024),
                             cache=dict(coordinator_bytes=1024 * 1024))
        self.assertEqual(checks['failures'], [])
        self.assertEqual(checks['checks']['cache']['counters']['state'], 'pass')
        empty = self.checks('S1-7-cache1000-native',
                            scrape(hits=0, misses=3, cache_mib=1024),
                            cache=dict(coordinator_bytes=1024 * 1024))
        self.assertTrue(any('no value-cache hit' in item for item in empty['failures']))

    def test_value_identity_and_checkpoint_refill_are_not_exported(self):
        checks = self.checks('S1-7-cache0-native', scrape())
        self.assertEqual(checks['checks']['cache']['value_identity']['state'],
                         'not_exported')
        self.assertIn('cache.checkpoint_refill', checks['not_exported'])

    def test_a_missing_keyspace_population_fails_the_cache_cell(self):
        values = {metric: value for metric, value in scrape().items()
                  if not metric.startswith('tidehunter_keyspace_read_cache')}
        checks = self.checks('S1-7-cache0-native', values)
        self.assertEqual(checks['checks']['cache']['keyspace_population']['state'],
                         'missing_counter')
        self.assertTrue(any('cache' in item for item in checks['failures']))

    def test_a_missing_value_cache_counter_fails_the_m2_cell(self):
        values = scrape()
        del values[f'tidehunter_adapter{{namespace="{NAMESPACE}",stat="value_cache_hits"}}']
        checks = self.checks('M2-cache1000-native', values)
        self.assertTrue(any('value_cache' in item for item in checks['failures']))
        self.assertEqual(checks['checks']['value_cache']['state'], 'missing_counter')

    def test_m2_reports_the_hit_rate_over_the_observed_population(self):
        checks = self.checks('M2-cache1000-native', scrape(hits=3, misses=1))
        self.assertEqual(checks['failures'], [])
        row = checks['checks']['value_cache']['evidence']['accounting']['namespaces'][NAMESPACE]
        self.assertEqual((row['value_cache_hits'], row['value_cache_misses']), (3, 1))
        self.assertEqual(row['hit_rate'], 0.75)
        evidence = checks['checks']['value_cache']['evidence']
        self.assertIn('owner_fetch_fraction', evidence)
        self.assertIn('no hit counter', evidence['per_cell_value_cache'])

    def test_three_write_batches_per_checkpoint_is_required(self):
        self.assertEqual(self.checks('M1-c4-native', scrape(batches=6))['failures'], [])
        checks = self.checks('M1-c4-native', scrape(batches=4))
        self.assertTrue(any('reads 2.0, not 3' in item for item in checks['failures']))

    def test_a_lifecycle_cell_needs_the_reopen_evidence(self):
        checks = self.checks('S1-1-native', scrape())
        self.assertEqual(checks['checks']['lifecycle']['state'], 'missing_counter')
        self.assertTrue(any('lifecycle' in item for item in checks['failures']))
        lifecycle = dict(status='clean-reopen-verified', audit_reopen=dict(equal=True),
                         tail_replay=dict(state_unchanged=True),
                         reopen=dict(rpc_index_catchup=dict(
                             status='persisted-watermark-at-frozen-tip')))
        self.assertEqual(self.checks('S1-1-native', scrape(),
                                     lifecycle=lifecycle)['failures'], [])
        without = dict(lifecycle, reopen=dict(rpc_index_catchup=dict(status=None)))
        checks = self.checks('S1-1-native', scrape(), lifecycle=without)
        self.assertEqual(checks['checks']['lifecycle']['state'], 'fail')

    def test_the_hold_cell_needs_a_sampled_hold(self):
        hold = dict(requested_seconds=1800, seconds=1801,
                    disk_samples=[dict(databases={}), dict(databases={})],
                    final=dict(databases={NAMESPACE: {}}),
                    final_metrics_available=True,
                    triggers=dict(snapshot=dict(triggered=False),
                                  relocation=dict(triggered=False)))
        self.assertEqual(self.checks('M5-hold', scrape(), hold=hold)['failures'], [])
        self.assertIn('hold', [item.split(':')[0] for item in
                              self.checks('M5-hold', scrape())['failures']])

    def test_an_unreviewed_cell_records_checks_but_requires_none(self):
        checks = self.checks('sui-replay', scrape(live=False))
        self.assertEqual(checks['requires'], [])
        self.assertEqual(checks['failures'], [])


if __name__ == '__main__':
    unittest.main()
