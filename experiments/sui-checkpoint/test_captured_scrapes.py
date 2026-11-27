"""Coverage over real captured scrapes.

`rehearsal-*.prom` are trimmed files from
`scratch/Rehearsal5/runs/{m1-driver1,native-full}/experiment`; all series and
their real labels are kept, histogram buckets are thinned to two per series.
They retain the three Sui registry databases (`epoch`, `perpetual`,
`checkpoint`) that made single-keyspace keying ambiguous.

`attempt2-*.prom` and `attempt2-results.json` are the S1-0-native cloud
attempt 2 evidence (`gs://mysten-research-bench-artifacts/
campaign-hybrid-sui-2026-11-25/stage1/raw/S1-0-native/attempt-2`), trimmed the
same way (every family and label kept, one bucket per histogram series), with
`results.json` reduced to the keys the final-metrics validation and summary
read. The final and foreground scrapes were byte-identical in that attempt, so
the one scrape per mode serves both.

`s1-1s-adapter-final.prom`, `s1-1s-remote-foreground-cache.prom` and
`s1-0s-adapter-final.prom` are extractions of the S1-1-synchronous attempt 2
final scrape, its foreground checkpoint-cache counters, and the settled
S1-0-synchronous attempt 4 final scrape, each keeping every series of the
family named in its test. `reopen-native-attempt2.log` keeps the two lines the
S1-1-native attempt 2 reopen logged: the rpc-store bootstrap and the
framework's stated ingestion range.
"""
from pathlib import Path
import io
import json
import tempfile
import unittest
from unittest import mock

import analyze
import run

FIXTURES = Path(__file__).resolve().parent / 'fixtures'
NAMESPACES = ['live/store/epoch_0', 'live/store/perpetual', 'live/checkpoints']
DATABASES = ['checkpoint', 'epoch', 'perpetual']
ATTEMPT2 = 'attempt2-'
# Removed with the coordinator ordering-thread cutover; absent from the engine
# and therefore never required or read as a current counter.
STALE_ADAPTER_STATS = (
    'read_submit_calls', 'read_detached_wait_calls', 'read_finish_calls',
    'write_durable_batches', 'write_concurrent_completed',
    'write_peak_inflight_nodes', 'write_foreground_checkpoint_count',
    'write_foreground_checkpoint_ns', 'read_profile_wire_requests',
    'write_checkpoint_diagnostics_available')


class CapturedScrapes(unittest.TestCase):
    def load(self, name):
        return analyze.parse_metrics((FIXTURES / name).read_text())

    def test_every_scrape_reader_accepts_the_real_labels(self):
        for name in ('rehearsal-short-native.prom', 'rehearsal-full-native.prom'):
            with self.subTest(name=name):
                values = self.load(name)
                stats = analyze.coordinator_stats(values)
                self.assertTrue(stats['available'])
                self.assertEqual(sorted(stats['operations']), DATABASES)
                self.assertEqual(sorted(stats['lookups']), DATABASES)
                self.assertEqual(sorted(stats['large_table_contention']), DATABASES)
                self.assertEqual(sorted(stats['index_blob_cache_contention']), DATABASES)
                self.assertTrue(set(stats['writes']) <= set(DATABASES))
                for database in DATABASES:
                    self.assertTrue(stats['operations'][database])
                    self.assertTrue(stats['lookups'][database])
                    self.assertTrue(stats['large_table_contention'][database])
                if name == 'rehearsal-full-native.prom':
                    self.assertEqual(sorted(stats['writes']), DATABASES)
                    for database in DATABASES:
                        self.assertTrue(stats['writes'][database])
                for namespace in NAMESPACES:
                    self.assertTrue(analyze.coordinator_covers(stats, namespace))
                self.assertTrue(analyze.adapter_stats(values))
                analyze.live_nvmeof_stats(values)
                self.assertEqual(set(analyze.checkpoint_sequence_cache_stats(values)),
                                 {'hits', 'misses', 'backend_gets', 'busy_bypasses'})
                analyze.checkpoint_completion_observation(values)
                analyze.metric_cut(values, NAMESPACES, remote=False)

    def test_short_scrape_carries_the_executed_watermark(self):
        short = (FIXTURES / 'rehearsal-short-native.prom').read_text()
        full = (FIXTURES / 'rehearsal-full-native.prom').read_text()
        self.assertEqual(run.require_short_watermark(short, 7), 7)
        self.assertEqual(run.require_short_watermark(full, 1775), 1775)
        with self.assertRaisesRegex(RuntimeError, 'expected 8'):
            run.require_short_watermark(short, 8)


class RealClusterScrape(unittest.TestCase):
    """The real S1-0-native attempt 2 evidence must pass the offline path."""

    def setUp(self):
        self.result = json.loads((FIXTURES / f'{ATTEMPT2}results.json').read_text())
        self.modes = {mode: analyze.metrics(FIXTURES / f'{ATTEMPT2}{mode}-final.prom')
                      for mode in ('native', 'remote')}
        self.namespaces = sorted(self.result['deployment']['databases'])

    def test_final_metrics_validation_accepts_the_real_scrapes(self):
        counters, native = run.validate_final_metrics(
            self.modes['remote'], self.modes['native'],
            {mode: self.modes[mode] for mode in self.modes},
            schema=self.namespaces,
            databases=self.result['deployment']['databases'],
            read_limit=self.result['read_profile_limit'],
            write_limit=self.result['write_profile_limit'],
            native_write_limit=self.result['native_write_profile_limit'])
        self.assertEqual(sorted(counters), self.namespaces)
        self.assertEqual(sorted(native), self.namespaces)
        self.assertEqual(sorted(analyze.adapter_stats(self.modes['remote'])),
                         self.namespaces)
        # The native engine recorded db_op; the remote data path of this
        # attempt did not, so the coordinator gate rejects that cut as a typed
        # defect instead of an available zero-latency population.
        namespaces = self.result['remote']['final_metrics_freshness']['required_namespaces']
        analyze.metric_cut(self.modes['native'], namespaces, remote=False)
        with self.assertRaisesRegex(analyze.DbOpNotRecorded, '^db_op not recorded'):
            analyze.metric_cut(self.modes['remote'], namespaces, remote=True)

    def test_wrapper_latency_tables_compare_native_and_remote(self):
        table = {mode: analyze.wrapper_latency_table(values)
                 for mode, values in self.modes.items()}
        for mode, decoded in table.items():
            self.assertTrue(decoded['rows'], mode)
            self.assertEqual({row['op'] for row in decoded['rows']},
                             {'get', 'iter', 'multiget', 'put', 'write_batch_commit'})
            for row in decoded['rows']:
                self.assertEqual(row['labels'].keys() <= {'cf_name', 'db_name'}, True)
                for name in ('p50', 'p95', 'p99'):
                    quantile = row['quantiles'][name]
                    # A percentile is an interval or unavailable, never a point.
                    self.assertIsInstance(quantile, dict)
                    self.assertIn('interval_us', quantile)
                    self.assertIn('lower_us', quantile)
                    self.assertIn('upper_us', quantile)
        comparison = analyze.wrapper_latency_comparison(self.modes)
        self.assertEqual(comparison['modes'], ['native', 'remote'])
        self.assertTrue(comparison['rows'])
        shared = [row for row in comparison['rows']
                  if 'native' in row and 'remote' in row]
        self.assertTrue(shared)
        for row in comparison['rows']:
            self.assertTrue({'native', 'remote'} & set(row))
            for mode in ('native', 'remote'):
                if mode in row:
                    self.assertEqual(row[mode]['available'], row[mode]['count'] > 0)

    def test_owner_fetch_fraction_names_the_adapter_denominator(self):
        fraction = analyze.owner_fetch_fraction(
            analyze.live_nvmeof_stats(self.modes['remote']),
            analyze.coordinator_stats(self.modes['remote']),
            analyze.adapter_stats(self.modes['remote']))
        self.assertTrue(fraction['available'])
        self.assertEqual(fraction['source'],
                         'adapter read_calls; db_op series are registered with zero observations')
        self.assertEqual(fraction['api_gets'], 343)
        self.assertEqual(fraction['db_op']['state'], 'zero')
        self.assertEqual(fraction['db_op']['observations'], 0)
        self.assertGreater(fraction['db_op']['series'], 0)
        native = analyze.owner_fetch_fraction(
            analyze.live_nvmeof_stats(self.modes['native']),
            analyze.coordinator_stats(self.modes['native']),
            analyze.adapter_stats(self.modes['native']))
        self.assertFalse(native['available'])
        self.assertEqual(native['reason'], 'owner read operation population missing')
        self.assertTrue(native['db_op']['recorded'])

    def test_summarize_reads_the_real_scrapes(self):
        summary = analyze.summarize_verified(
            (None, self.result, {'databases': {}},
             {mode: self.modes[mode] for mode in self.modes}, self.modes))
        self.assertEqual(summary['cell_checks']['failures'], [])
        self.assertEqual(sorted(summary['modes']), ['native', 'remote'])
        self.assertTrue(summary['modes']['remote']['live_nvmeof']['available'])
        self.assertFalse(summary['modes']['native']['live_nvmeof']['available'])
        self.assertEqual(summary['cell_checks']['checks']['admission']
                         ['rdma_live_path']['state'], 'pass')
        remote, native = summary['modes']['remote'], summary['modes']['native']
        self.assertEqual(remote['coordinator']['db_op']['state'], 'zero')
        self.assertEqual(remote['owner_fetch_fraction']['source'],
                         'adapter read_calls; db_op series are registered with zero observations')
        self.assertEqual(native['coordinator']['db_op']['state'], 'recorded')
        self.assertTrue(remote['final_wrapper_latencies']['rows'])
        self.assertTrue(summary['wrapper_latency_comparison']['rows'])

    def test_stale_adapter_names_are_neither_required_nor_emitted(self):
        for name in STALE_ADAPTER_STATS:
            with self.subTest(name=name):
                self.assertNotIn(name, run.REQUIRED_ADAPTER_STATS)
        for mode, values in self.modes.items():
            stats = analyze.adapter_stats(values)
            for namespace in self.namespaces:
                for name in STALE_ADAPTER_STATS:
                    self.assertNotIn(name, stats[namespace], f'{mode} {namespace}')
        # The whole real scrape still validates after removing them, which is
        # the regression this fixture pins.
        run.validate_final_metrics(
            self.modes['remote'], self.modes['native'],
            {mode: self.modes[mode] for mode in self.modes},
            schema=self.namespaces,
            databases=self.result['deployment']['databases'],
            read_limit=self.result['read_profile_limit'],
            write_limit=self.result['write_profile_limit'],
            native_write_limit=self.result['native_write_profile_limit'])


class ContendedAdapterSample(unittest.TestCase):
    """A real final scrape taken while the engine's sampler was contended.

    `s1-1s-adapter-final.prom` keeps every `tidehunter_adapter` series of
    `campaign-hybrid-sui-2026-11-25/stage1/evidence/S1-1-synchronous/attempt-2/
    remote-final.prom`. Its `live/store/perpetual` namespace carries only the
    four sampler-status series: the engine copies each lock domain under
    `try_lock` and marks the whole snapshot unavailable rather than blocking a
    writer (`remote_db/concurrent.rs::try_visit_write_metrics`), and a
    synchronous-tier publisher held one for that tick. The S1-0-synchronous
    cell settled in all three namespaces; `s1-0s-adapter-final.prom` is the
    same extraction from its final scrape.
    """

    def load(self, name):
        return analyze.parse_metrics((FIXTURES / name).read_text())

    def test_the_contended_scrape_carries_only_the_sampler_status(self):
        counters = analyze.adapter_stats(self.load('s1-1s-adapter-final.prom'))
        self.assertEqual(sorted(counters), sorted(NAMESPACES))
        self.assertEqual(sorted(counters['live/store/perpetual']),
                         ['local_sample_age_ms', 'local_sample_available',
                          'local_sample_generation', 'local_sampler_stopped'])
        self.assertEqual(counters['live/store/perpetual']['local_sample_available'], 0)
        self.assertEqual(counters['live/store/perpetual']['local_sampler_stopped'], 0)
        self.assertEqual(len(counters['live/checkpoints']), 145)
        self.assertEqual(len(counters['live/store/epoch_0']), 145)

    def test_only_a_settled_sample_is_a_usable_adapter_population(self):
        self.assertEqual(
            run.adapter_sample_error(self.load('s1-1s-adapter-final.prom'), NAMESPACES),
            'live/store/perpetual: no usable adapter sample (last sample 1753 ms old)')
        self.assertIsNone(
            run.adapter_sample_error(self.load('s1-0s-adapter-final.prom'), NAMESPACES))

    def test_the_coverage_gate_names_the_database_and_the_missing_family(self):
        native = self.load('attempt2-native-final.prom')
        with self.assertRaisesRegex(
                ValueError,
                'coverage: live/store/perpetual lacks: bulk_read_calls'):
            run.validate_final_metrics(
                self.load('s1-1s-adapter-final.prom'), native,
                {'native': native, 'remote': self.load('s1-1s-remote-foreground-cache.prom')},
                schema=NAMESPACES, databases={}, read_limit=0, write_limit=0,
                native_write_limit=0)


class PreCertificationFreshness(unittest.TestCase):
    """The final cut compares with the last replay scrape, not with itself.

    `attempt4-*` are the real S1-0-native attempt 4 scrapes: the foreground
    scrape taken at the end of the full 1776-checkpoint replay, before the RPC
    index catchup, and the post-certification scrape the failed gate kept while
    it waited for an advance that a quiescent node never published.
    """

    TIP = 1775
    NAMESPACES = {'live/checkpoints', 'live/store/epoch_0', 'live/store/perpetual'}
    FOREGROUND = 'attempt4-remote-foreground.prom'
    FINAL = 'attempt4-remote-final.prom'

    def text(self, name):
        return (FIXTURES / name).read_text()

    # The committed scrape predates engine db_op recording in the distributed
    # data path. The current engine records the populations; this graft adds one
    # real-shaped observed get population so the freshness machinery can be
    # exercised. The unmodified scrape is rejected by
    # test_the_real_scrape_reports_db_op_not_recorded.
    RECORDED_GET = 'db_op_count{ks="active_jwks",op="get",db="epoch"}'
    RECORDED = {
        RECORDED_GET + ' 0': RECORDED_GET + ' 4',
        'db_op_sum{ks="active_jwks",op="get",db="epoch"} 0':
            'db_op_sum{ks="active_jwks",op="get",db="epoch"} 40',
        'db_op_bucket{ks="active_jwks",op="get",db="epoch",le="5"} 0':
            'db_op_bucket{ks="active_jwks",op="get",db="epoch",le="5"} 4',
    }

    def recorded(self, text):
        for old, new in self.RECORDED.items():
            self.assertIn(old, text)
            text = text.replace(old, new, 1)
        return text

    def wait(self, texts, *, baseline, budget=1, tip=None, observations=None):
        clock = [10.]
        def sleep(seconds):
            clock[0] += seconds
        responses = iter(texts)
        last = texts[-1]
        def scrape(*args, **kwargs):
            nonlocal last
            last = next(responses, last)
            return io.BytesIO(last.encode())
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(run.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(run.time, 'sleep', side_effect=sleep), \
                mock.patch.object(run, 'sample',
                                  return_value={'pid': 123, 'exited': False}), \
                mock.patch.object(run.urllib.request, 'urlopen', side_effect=scrape):
            destination = Path(directory) / 'final.prom'
            try:
                return run.wait_for_final_metrics(
                    object(), [], 'unused', self.NAMESPACES, remote=True,
                    end=10 + budget, destination=destination, baseline=baseline,
                    tip=self.TIP if tip is None else tip, observations=observations)
            finally:
                self.retained = destination.read_text() if destination.exists() else None

    def test_the_real_scrape_reports_db_op_not_recorded(self):
        baseline, final = self.text(self.FOREGROUND), self.text(self.FINAL)
        with self.assertRaises(analyze.DbOpNotRecorded) as raised:
            self.wait([final], baseline=baseline)
        fact = raised.exception.fact
        self.assertEqual(fact['state'], 'zero')
        self.assertGreater(fact['series'], 0)
        self.assertEqual(fact['observations'], 0)
        self.assertGreater(fact['adapter_read_calls'], 0)
        self.assertGreater(fact['adapter_write_commit_calls'], 0)
        self.assertTrue(str(raised.exception).startswith('db_op not recorded:'))

    def test_the_recorded_replay_baseline_qualifies_the_quiescent_node(self):
        baseline = self.recorded(self.text(self.FOREGROUND))
        final = self.recorded(self.text(self.FINAL))
        text, cut = self.wait([final], baseline=baseline)
        self.assertEqual(text, final)
        self.assertEqual(cut['baseline_scope'], analyze.PRE_CERTIFICATION_BASELINE)
        self.assertEqual(cut['baseline']['coordinator']['db_op']['state'], 'recorded')
        self.assertTrue(analyze.metric_cut_progressed(cut['baseline'], cut['final'],
                                                      remote=True))
        self.assertGreater(cut['final']['live']['completed_read_ops'],
                           cut['baseline']['live']['completed_read_ops'])
        analyze.validate_final_metric_cut(analyze.parse_metrics(final), self.NAMESPACES,
                                          cut, remote=True)

    def test_a_frozen_registry_never_qualifies(self):
        frozen = self.recorded(self.text(self.FINAL))
        observations = []
        with self.assertRaisesRegex(TimeoutError, 'unavailable'):
            self.wait([frozen], baseline=frozen, budget=0.5, observations=observations)
        self.assertEqual(self.retained, frozen)
        self.assertTrue(observations[0]['live_available'])
        self.assertFalse(observations[0].get('unavailable'))

    def test_a_regressed_aggregate_counter_fails_the_cut(self):
        baseline = self.recorded(self.text(self.FOREGROUND))
        lines = self.recorded(self.text(self.FINAL)).splitlines()
        counter = 'tidehunter_nvmeof_live_completed_write_ops_total{db="checkpoint"}'
        for index, line in enumerate(lines):
            if line.startswith(counter + ' '):
                lines[index] = counter + ' 1'
                break
        else:
            self.fail('the fixture lacks the aggregate write counter')
        with self.assertRaisesRegex(ValueError, 'regressed'):
            self.wait(['\n'.join(lines) + '\n'], baseline=baseline)

    def contended(self, text):
        """Replace the perpetual adapter block with the real contended sample."""
        kept = [line for line in text.splitlines()
                if not line.startswith('tidehunter_adapter{namespace="live/store/perpetual"')]
        real = [line for line in
                (FIXTURES / 's1-1s-adapter-final.prom').read_text().splitlines()
                if line.startswith('tidehunter_adapter{namespace="live/store/perpetual"')]
        self.assertEqual(len(real), 4)
        return '\n'.join(kept + real) + '\n'

    def test_the_real_contended_scrape_is_retried_until_the_sampler_settles(self):
        settled = self.recorded(self.text(self.FINAL))
        contended = self.recorded(self.contended(self.text(self.FINAL)))
        observations = []
        text, cut = self.wait([contended, settled],
                              baseline=self.recorded(self.text(self.FOREGROUND)),
                              observations=observations)
        self.assertEqual(text, settled)
        self.assertEqual(cut['final']['live']['completed_read_ops'],
                         analyze.metric_cut(analyze.parse_metrics(settled),
                                            self.NAMESPACES, remote=True)
                         ['live']['completed_read_ops'])
        self.assertIn('live/store/perpetual: no usable adapter sample',
                      observations[0]['adapter_unavailable'])
        self.assertEqual(self.retained, settled)

    def test_a_permanently_contended_sample_never_qualifies_the_cut(self):
        contended = self.recorded(self.contended(self.text(self.FINAL)))
        observations = []
        with self.assertRaisesRegex(TimeoutError, 'no usable adapter sample'):
            self.wait([contended], baseline=self.recorded(self.text(self.FOREGROUND)),
                      budget=0.5, observations=observations)
        self.assertIn('adapter_unavailable', observations[-1])

    def test_lifecycle_and_hold_cells_keep_a_valid_cut(self):
        final = self.recorded(self.text(self.FINAL))
        _, cut = self.wait([final], baseline=self.recorded(self.text(self.FOREGROUND)))
        lifecycle = dict(status='clean-reopen-verified', audit_reopen={'equal': True},
                         tail_replay={'state_unchanged': True}, durable_cut={'tracked': True},
                         no_new_export={'available': True}, tail_checkpoints=64,
                         reopen={'rpc_index_catchup': {
                             'status': 'persisted-watermark-at-frozen-tip',
                             'committer_batches': {}}})
        hold = dict(seconds=1800, requested_seconds=1800,
                    disk_samples=[{'namespace': 'a'}, {'namespace': 'a'}],
                    final={'databases': ['a']}, final_metrics_available=True)
        values = {'remote': analyze.parse_metrics(final)}
        for cell_id, result in (
                ('S1-1-native', dict(cell_id='S1-1-native', lifecycle=lifecycle)),
                ('S1-8-synchronous', dict(cell_id='S1-8-synchronous', lifecycle=lifecycle)),
                ('M5-hold', dict(cell_id='M5-hold', hold=hold))):
            with self.subTest(cell=cell_id):
                result |= dict(live_transport='rdma', cache={'coordinator_bytes': 0},
                               checkpoint_limit=self.TIP)
                result['final_metrics_freshness'] = cut
                self.assertEqual(analyze.lifecycle_check(result)['state'],
                                 'pass' if 'lifecycle' in result else 'missing_counter')
                self.assertEqual(analyze.hold_check(result)['state'],
                                 'pass' if 'hold' in result else 'missing_counter')
                checks = analyze.cell_checks(result, values, sorted(self.NAMESPACES),
                                             checkpoint_count=self.TIP + 1)
                self.assertEqual(checks['failures'], [])
                analyze.validate_final_metric_cut(
                    analyze.parse_metrics(final), self.NAMESPACES,
                    result['final_metrics_freshness'], remote=True)


if __name__ == '__main__':
    unittest.main()
