import unittest

import valkey_cloud


def native_output(operations=('PING_INLINE', 'PING_MBULK', 'SET', 'GET'), requests=2000):
    return ''.join(f'''====== {operation} ======
  {requests} requests completed in 1.00 seconds
Summary:
  throughput summary: 2000.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.130     0.016     0.111     0.151     0.183     0.503
''' for operation in operations)


class NativePopulationAdmission(unittest.TestCase):
    def test_retains_native_units_without_inventing_unreported_statistics(self):
        result = valkey_cloud.statistics(native_output(), 2000)
        self.assertEqual(result['GET']['latency_ms']['p99'], .183)
        self.assertEqual(result['SET']['calls_per_second'], 2000.)
        self.assertNotIn('population_stddev', result['GET']['latency_ms'])
        self.assertNotIn('p999', result['GET']['latency_ms'])

    def test_missing_duplicate_and_short_populations_are_not_complete(self):
        cases = (native_output(('PING_INLINE', 'PING_MBULK', 'SET')),
                 native_output() + native_output(('GET',)), native_output(requests=1999))
        for text in cases:
            with self.subTest(text=text), self.assertRaises(ValueError):
                valkey_cloud.statistics(text, 2000)


if __name__ == '__main__':
    unittest.main()
