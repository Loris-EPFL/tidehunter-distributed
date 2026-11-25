"""Analyzer coverage over real captured scrapes from rehearsal round 5.

The fixtures are trimmed `.prom` files from
`scratch/Rehearsal5/runs/{m1-driver1,native-full}/experiment`; all series and
their real labels are kept, histogram buckets are thinned to two per series.
They retain the three Sui registry databases (`epoch`, `perpetual`,
`checkpoint`) that made single-keyspace keying ambiguous.
"""
from pathlib import Path
import unittest

import analyze
import run

FIXTURES = Path(__file__).resolve().parent / 'fixtures'
NAMESPACES = ['live/store/epoch_0', 'live/store/perpetual', 'live/checkpoints']
DATABASES = ['checkpoint', 'epoch', 'perpetual']


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


if __name__ == '__main__':
    unittest.main()
