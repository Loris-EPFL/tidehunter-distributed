"""Contract boundary regressions; no Docker, HTTP wiring or synthetic workload."""
import os
from pathlib import Path
import tempfile
import unittest

import exporter
from stack import result_selection, target_group


def payload(stamp=100, status=0):
    return (f'# TYPE kv_comparison_snapshot_timestamp_seconds gauge\n'
            f'kv_comparison_snapshot_timestamp_seconds {stamp}\n'
            f'kv_comparison_run_status {status}\n'
            'kv_comparison_completed_total{backend="native",operation="get",phase="measure",outcome="ok"} 7\n').encode()


class SnapshotSemantics(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_no_run_and_waiting_have_no_performance_samples(self):
        for current in (exporter.snapshot(None, 15, 101), exporter.snapshot(self.root, 15, 101)):
            self.assertFalse(current.payload)
            self.assertNotIn(b'kv_comparison_completed_total', exporter.exposition(current))
        self.assertEqual(exporter.snapshot(None, 15, 101).state, 'no_run')
        self.assertEqual(exporter.snapshot(self.root, 15, 101).state, 'waiting')

    def test_selected_uncreated_leaf_waits_without_creating_it(self):
        selected = result_selection(self.root / 'future-run')
        self.assertEqual(selected, self.root / 'future-run')
        self.assertFalse(selected.exists())
        current = exporter.snapshot(selected, 15, 101)
        self.assertEqual(current.state, 'waiting')
        self.assertFalse(current.payload)
        self.assertFalse(selected.exists())

    def test_selected_leaf_rejects_symlink_and_parent_traversal(self):
        (self.root / 'link').symlink_to(self.root, target_is_directory=True)
        for candidate in (self.root / 'link', self.root / '..' / 'outside', Path('relative')):
            with self.subTest(path=candidate), self.assertRaises(ValueError):
                result_selection(candidate)

    def test_live_payload_is_preserved_not_rewritten(self):
        data = payload()
        (self.root / 'live.prom').write_bytes(data)
        current = exporter.snapshot(self.root, 15, 101)
        self.assertEqual(current.state, 'running')
        self.assertEqual(current.payload, data)
        self.assertTrue(exporter.exposition(current).endswith(data))

    def test_stale_live_omits_all_run_samples(self):
        (self.root / 'live.prom').write_bytes(payload())
        current = exporter.snapshot(self.root, 15, 116)
        self.assertEqual(current.state, 'stale')
        self.assertEqual(current.age, 16)
        self.assertFalse(current.payload)

    def test_old_final_is_retained_and_failure_never_becomes_success(self):
        (self.root / 'live.prom').write_bytes(payload(999))
        (self.root / 'metrics.prom').write_bytes(payload(100, 2))
        current = exporter.snapshot(self.root, 15, 1000)
        self.assertEqual(current.state, 'failed')
        self.assertTrue(current.final)
        self.assertEqual(current.age, 900)
        self.assertEqual(current.payload, payload(100, 2))

    def test_completed_final_does_not_need_scrape_history(self):
        (self.root / 'metrics.prom').write_bytes(payload(100, 1))
        self.assertEqual(exporter.snapshot(self.root, 15, 1000).state, 'completed')

    def test_invalid_final_never_falls_back_to_apparently_running_live(self):
        (self.root / 'metrics.prom').write_bytes(payload(100, 0))
        (self.root / 'live.prom').write_bytes(payload())
        self.assertEqual(exporter.snapshot(self.root, 15, 101).state, 'invalid')

    def test_symlink_fifo_and_oversized_files_are_rejected(self):
        target = self.root / 'outside.prom'
        target.write_bytes(payload())
        live = self.root / 'live.prom'
        live.symlink_to(target)
        self.assertEqual(exporter.snapshot(self.root, 15, 101).state, 'invalid')
        live.unlink()
        os.mkfifo(live)
        self.assertEqual(exporter.snapshot(self.root, 15, 101).state, 'invalid')
        live.unlink()
        with live.open('wb') as stream:
            stream.truncate(exporter.MAX_BYTES + 1)
        self.assertEqual(exporter.snapshot(self.root, 15, 101).state, 'invalid')

    def test_future_nonfinite_duplicate_and_reserved_scalars_are_rejected(self):
        for data in (payload(107), payload('NaN'), payload() + b'kv_comparison_run_status 1\n',
                     payload() + b'kv_observer_data_available 1\n', payload()[:-1]):
            with self.subTest(data=data):
                (self.root / 'live.prom').write_bytes(data)
                self.assertEqual(exporter.snapshot(self.root, 15, 101).state, 'invalid')


class TargetSemantics(unittest.TestCase):
    def test_explicit_loopback_target_and_metrics_path(self):
        self.assertEqual(target_group('http://localhost:9184/metrics'),
                         {'targets': ['127.0.0.1:9184'], 'labels': {'__metrics_path__': '/metrics'}})

    def test_no_external_dns_credentials_queries_or_traversal(self):
        for url in ('http://example.com:9100/metrics', 'http://127.0.0.2:9100/metrics',
                    'https://localhost:9100/metrics', 'http://localhost/metrics',
                    'http://user:password@localhost:9100/metrics',
                    'http://localhost:9100/metrics?x=1', 'http://localhost:9100/../secret',
                    'http://localhost:9100/%2e%2e/secret', 'http://localhost:99999/metrics'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                target_group(url)


if __name__ == '__main__':
    unittest.main()
