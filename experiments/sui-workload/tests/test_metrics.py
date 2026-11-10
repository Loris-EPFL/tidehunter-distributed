import pathlib
import sys
import unittest
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from metrics import percentiles, complete_component_buckets

class PercentileTests(unittest.TestCase):
    def test_empty(self): self.assertEqual(percentiles([]), [''] * 7)
    def test_single(self): self.assertEqual(percentiles([3]), [1,3,3,3,3,3,3])
    def test_interpolation(self):
        result=percentiles([0,10])
        self.assertEqual(result[0],2)
        self.assertEqual(result[1],5)
        self.assertEqual(result[2],9)
        self.assertEqual(result[-1],5)

class RateBucketTests(unittest.TestCase):
    def test_complete_buckets_include_gaps_and_exclude_edges(self):
        checkpoints = [
            {'timestamp_ms': 1500, 'bytes': 100},
            {'timestamp_ms': 2100, 'bytes': 12},
            {'timestamp_ms': 4900, 'bytes': 20},
            {'timestamp_ms': 5500, 'bytes': 100},
        ]
        self.assertEqual(
            complete_component_buckets(checkpoints, 1, ['bytes']),
            [(2000, [12.0]), (3000, [0.0]), (4000, [20.0])],
        )

    def test_empty_and_short_window(self):
        self.assertEqual(complete_component_buckets([], 10, ['bytes']), [])
        self.assertEqual(complete_component_buckets([
            {'timestamp_ms': 1001, 'bytes': 1},
            {'timestamp_ms': 1999, 'bytes': 2},
        ], 1, ['bytes']), [])
