import importlib.util
from pathlib import Path
import sys
import unittest

OBS = Path(__file__).resolve().parent.parent / 'kv-comparison' / 'observability'
sys.path.insert(0, str(OBS))
spec = importlib.util.spec_from_file_location('campaign_exporter', OBS / 'campaign_exporter.py')
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)


class EvidenceTests(unittest.TestCase):
    def test_target_intervals_and_missing_values(self):
        evidence = {'version': 1, 'block_device': 'nvme0n1',
                    'block': {'start_monotonic_ns': 100, 'end_monotonic_ns': 1000000100,
                              'delta': {'reads_completed': 3, 'read_bytes': 4096}},
                    'cpu': {'start_monotonic_ns': 200, 'end_monotonic_ns': 2000000200,
                            'delta': {'user': 4}},
                    'trace': {'available': False}, 'clock_ticks_per_second': 100}
        text = exporter.nvmeof_target_metrics(evidence, {'run_id': 'r'})
        self.assertIn('counter="read_bytes"} 4096', text)
        self.assertIn('component="block",device="nvme0n1"} 1.0', text)
        self.assertIn('component="cpu",device="nvme0n1"} 2.0', text)
        self.assertIn('kv_nvmeof_target_trace_available', text)
        self.assertNotIn('counter="write_bytes"', text)
        self.assertEqual(exporter.nvmeof_target_metrics(None, {}), '')
        evidence['block']['end_monotonic_ns'] = 1
        with self.assertRaises(ValueError): exporter.nvmeof_target_metrics(evidence, {})


if __name__ == '__main__':
    unittest.main()
