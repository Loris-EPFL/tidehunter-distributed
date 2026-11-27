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
"""
from pathlib import Path
import json
import unittest

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
        analyze.metric_cut(self.modes['remote'],
                           self.result['remote']['final_metrics_freshness']['required_namespaces'],
                           remote=True)

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


if __name__ == '__main__':
    unittest.main()
