"""Independent arithmetic and coverage checks for read residuals."""
import unittest

import decomposition


N = 4
LEAVES = {
    'index_lookup': (8000, 4), 'cache_lookup': (4000, 4),
    'cut_validation': (4000, 4), 'cut_capture': (4000, 4),
    'route_preparation': (8000, 4), 'pin_preparation': (4000, 4),
    'batch_preparation': (4000, 4), 'batch_wait': (8000, 4),
    'postread_cache_fill': (4000, 4), 'result_materialization': (4000, 4),
}


def profile():
    stages = {name: {'sum_ns': total, 'count': count} for name, (total, count) in LEAVES.items()}
    # These counters are explicitly uninstrumented, not measured zeroes.
    stages.update({name: {'sum_ns': None, 'count': 0} for name in ('index_lock_wait', 'cache_lock_wait')})
    return {
        'coordinator_resolution_valid': True,
        'coordinator_resolution': {'selected': N, 'errors': 0, 'member_errors': 0, 'stages': stages},
        'selected_operations': N, 'completed_operations': N,
        'counters': {'read_profile_errors': 0},
        'stages': {'inline': {'sum_ns': 80000, 'count': N},
                   'server_handler': {'sum_ns': 48000, 'count': N}},
    }


def rows(p, device=None, api=100, count=N, route='owner', cold=True, readers=None):
    return {row['stage']: row for row in decomposition.read_residuals(
        p, readers or [], device, api, count, route, cold)}


class ReadResidualTests(unittest.TestCase):
    def assert_derived(self, row, expected):
        self.assertAlmostEqual(row['mean_us'], expected)
        self.assertEqual(row['count'], N)
        self.assertEqual(row['exactness'], 'derived_mean_difference')
        self.assertTrue(row['selection'])
        self.assertTrue(row['source_stages'])

    def test_owner_residuals_use_whole_population_and_weight_device(self):
        p = profile()
        device = {'count': 2, 'mean_us': 6, 'exactness': 'device_counter'}
        actual = rows(p, device)
        coordinator = sum(total for total, _ in LEAVES.values()) / N / 1000
        inline = 80000 / N / 1000
        self.assert_derived(actual['client other'], 100 - coordinator - inline)
        self.assert_derived(actual['owner server software'], 48000 / N / 1000 - 2 * 6 / N)
        self.assertNotAlmostEqual(actual['owner server software']['mean_us'], 48000 / N / 1000 - 6)

    def test_owner_conditional_stages_use_measured_read_denominator(self):
        p = profile()
        p['coordinator_resolution']['stages']['cache_lookup'] = {'sum_ns': 6000, 'count': 2}
        p['stages']['inline'] = {'sum_ns': 30000, 'count': 2}
        coordinator_ns = sum(total for name, (total, _) in LEAVES.items()
                             if name != 'cache_lookup') + 6000
        expected = 100 - coordinator_ns / N / 1000 - 30000 / N / 1000
        self.assert_derived(rows(p, {'count': 2, 'mean_us': 6})['client other'], expected)

    def test_independent_selection_and_success_coverage_are_required(self):
        for mutation in ('resolution', 'selected', 'completed', 'error', 'resolution_error', 'member_error'):
            with self.subTest(mutation=mutation):
                p = profile()
                if mutation == 'resolution':
                    p['coordinator_resolution']['selected'] = N - 1
                elif mutation == 'selected':
                    p['selected_operations'] = N - 1
                elif mutation == 'completed':
                    p['completed_operations'] = N - 1
                elif mutation == 'error':
                    p['counters']['read_profile_errors'] = 1
                elif mutation == 'resolution_error':
                    p['coordinator_resolution']['errors'] = 1
                else:
                    p['coordinator_resolution']['member_errors'] = 1
                row = rows(p, {'count': N, 'mean_us': 3})['client other']
                self.assertIsNone(row['mean_us'])
                self.assertTrue(row['selection'])

    def test_missing_or_null_required_data_is_not_zero(self):
        for name in ('inline', 'server_handler'):
            for bad in (None, {'sum_ns': None, 'count': N}):
                with self.subTest(stage=name, value=bad):
                    p = profile()
                    if bad is None:
                        del p['stages'][name]
                    else:
                        p['stages'][name] = bad
                    stage = 'client other' if name == 'inline' else 'owner server software'
                    self.assertIsNone(rows(p, {'count': N, 'mean_us': 3})[stage]['mean_us'])
        p = profile()
        p['stages']['server_handler']['count'] = N - 1
        self.assertIsNone(rows(p, {'count': N, 'mean_us': 3})['owner server software']['mean_us'])
        for device in (None, {'count': None, 'mean_us': 3}, {'count': 2, 'mean_us': None}):
            with self.subTest(device=device):
                self.assertIsNone(rows(profile(), device)['owner server software']['mean_us'])
        p = profile()
        p['coordinator_resolution']['stages']['index_lookup']['sum_ns'] = None
        self.assertIsNone(rows(p, {'count': N, 'mean_us': 3})['client other']['mean_us'])
        self.assertIsNone(rows(profile(), {'count': N, 'mean_us': 3}, api=None)['client other']['mean_us'])

    def test_native_cold_uses_window_device_time_per_read(self):
        device = {'count': 2, 'mean_us': 10, 'exactness': 'device_counter'}
        actual = rows({}, device, api=40, route='native', cold=True)
        self.assert_derived(actual['native cold software'], 40 - 2 * 10 / N)
        self.assertIsNone(rows({}, None, api=40, route='native', cold=True)['native cold software']['mean_us'])
        self.assertNotIn('native cold software', rows({}, device, api=40, route='native', cold=False))

    def test_hybrid_reader_components_are_normalized_without_double_counting_device(self):
        reader = [{'stage': name, 'count': count, 'mean_us': mean}
                  for name, count, mean in (
                      ('admission', 4, 2), ('alignment', 4, 1),
                      ('queue', 2, 4), ('submit_to_cq', 4, 20),
                      ('extraction', 4, 1), ('verification', 4, 2))]
        device = {'count': 2, 'mean_us': 8}
        coordinator = sum(total for total, _ in LEAVES.values()) / N / 1000
        reader_time = sum(item['count'] * item['mean_us'] for item in reader) / N
        actual = rows(profile(), device, route='hybrid', readers=reader)
        self.assert_derived(actual['client other'], 100 - coordinator - reader_time)
        # Without a usable device counter the combined submit_to_cq interval stays in the reader time,
        # so the residual is still defined and equals the same difference.
        self.assert_derived(rows(profile(), None, route='hybrid', readers=reader)['client other'],
                            100 - coordinator - reader_time)
        self.assertIsNone(rows(profile(), device, route='hybrid', readers=reader[:-1])['client other']['mean_us'])
        invalid = [dict(item) for item in reader]
        invalid[0]['mean_us'] = None
        self.assertIsNone(rows(profile(), device, route='hybrid', readers=invalid)['client other']['mean_us'])

    def test_real_tcp_excerpt_independent_recomputation(self):
        # 0484 TCP read-diagnostics.json: trimmed coordinator and flight totals.
        totals = {'index_lookup': 354579, 'cache_lookup': 54406,
                  'route_preparation': 16581, 'pin_preparation': 132709}
        names = ('index_lookup', 'cache_lookup', 'cut_validation', 'cut_capture',
                 'route_preparation', 'pin_preparation', 'batch_preparation', 'batch_wait',
                 'postread_cache_fill', 'result_materialization')
        stages = {name: ({'count': 256, 'sum_ns': totals[name], 'available': True} if name in totals else
                         {'count': 0, 'sum_ns': None, 'available': False,
                          'unavailable_reason': 'no observations for this stage'}) for name in names}
        profile_data = {'coordinator_resolution_valid': True, 'selected_operations': 256,
                        'completed_operations': 256, 'counters': {'read_profile_errors': 0},
                        'coordinator_resolution': {'selected': 256, 'errors': 0, 'member_errors': 0,
                                                   'stages': stages},
                        'stages': {'inline': {'count': 175, 'sum_ns': 9453298},
                                   'server_handler': {'count': 256, 'sum_ns': 914993}}}
        result = rows(profile_data, {'count': 256, 'mean_us': 1}, api=100, count=256)
        self.assertAlmostEqual(result['client other']['mean_us'],
                               100 - (sum(totals.values()) + 9453298) / 256 / 1000)
        self.assertAlmostEqual(result['owner server software']['mean_us'], 914993 / 256 / 1000 - 1)


if __name__ == '__main__':
    unittest.main()
