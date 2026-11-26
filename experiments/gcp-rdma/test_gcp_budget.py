"""The campaign cap the create path enforces and the manifest publishes."""
import json
from pathlib import Path
import unittest

import gcp_api
import gcp_budget


class Store:
    """One bucket: the immutable-receipt surface a reservation needs."""

    def __init__(self):
        self.objects = {}
        self.generation = 0

    def put(self, key, data, *, if_generation_match=None, **kwargs):
        if if_generation_match == 0 and key in self.objects:
            raise gcp_api.ApiError(412, 'exists')
        self.generation += 1
        self.objects[key] = (str(self.generation), bytes(data))
        return {'generation': str(self.generation), 'size': len(data)}

    def head(self, key):
        if key not in self.objects:
            raise gcp_api.ApiError(404, key)
        generation, data = self.objects[key]
        return {'name': key, 'generation': generation, 'size': len(data)}

    def get(self, key, *, generation=None):
        if key not in self.objects:
            raise gcp_api.ApiError(404, key)
        return self.objects[key][1]

    def list(self, prefix):
        return iter([{'name': key, 'generation': generation}
                     for key, (generation, _data) in sorted(self.objects.items())
                     if key.startswith(prefix)])


class CampaignCap(unittest.TestCase):
    def test_the_reviewed_cap_and_spent_total_are_in_the_document(self):
        campaign = gcp_budget.load()['campaign']
        self.assertEqual(campaign['cap_usd'], 260.0)
        self.assertEqual(campaign['spent_usd'], 76.4)
        self.assertEqual(campaign['hosts'], 3)

    def test_a_fleet_ceiling_is_the_price_based_rounded_up_bound(self):
        # 3 hosts x h hours at $3.287109973 plus the 14500/11834 ratio, as the
        # campaign manifest publishes it.
        self.assertEqual(gcp_budget.fleet_ceiling_cents(1, 3), 1209)
        self.assertEqual(gcp_budget.fleet_ceiling_cents(2, 3), 2417)
        self.assertEqual(gcp_budget.fleet_ceiling_cents(5, 3), 6042)
        self.assertEqual(gcp_budget.fleet_ceiling_cents(6, 3), 7250)
        with self.assertRaises(ValueError):
            gcp_budget.fleet_ceiling_cents(0, 3)
        with self.assertRaises(ValueError):
            gcp_budget.fleet_ceiling_cents(6, 0)

    def test_each_reviewed_stage_window_is_admitted(self):
        for hours, ceiling in ((5, 60.42), (6, 72.50), (2, 24.17)):
            admitted = gcp_budget.admit_campaign(hours, 3)
            self.assertEqual(admitted['fleet_ceiling_usd'], ceiling)
            self.assertEqual(admitted['spent_usd'], 76.40)
            self.assertEqual(admitted['cap_usd'], 260.00)
            self.assertAlmostEqual(
                admitted['remaining_after_usd'], 183.60 - ceiling, places=2)

    def test_a_window_that_crosses_the_cap_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'campaign cap'):
            gcp_budget.admit_campaign(16, 3)

    def test_spent_plus_the_ceiling_above_the_cap_is_refused(self):
        path = Path(__file__).with_name('budget.json')
        value = json.loads(path.read_text())
        value['campaign']['spent_usd'] = 250.00
        with self.assertRaisesRegex(ValueError, 'campaign cap'):
            gcp_budget.admit_campaign(5, 3, value)

    def test_a_window_receipt_is_immutable_and_counted_by_the_next_admission(self):
        store = Store()
        entry = gcp_budget.reserve_window(store, 'bench-1', 'stage1', 5, 3)
        self.assertEqual(entry['ceiling_cents'], 6042)
        # A retry of the same run and stage reuses its own receipt, not a second one.
        admitted = gcp_budget.admit_campaign(5, 3, store=store,
                                             run_id='bench-1', stage='stage1')
        self.assertEqual(admitted['committed_usd'], 60.42)
        self.assertAlmostEqual(admitted['remaining_after_usd'], 183.60 - 60.42, places=2)
        # Another stage sums on top of the first.
        admitted = gcp_budget.admit_campaign(6, 3, store=store,
                                             run_id='bench-1', stage='stage2')
        self.assertEqual(admitted['committed_usd'], 60.42)
        self.assertAlmostEqual(admitted['remaining_after_usd'], 183.60 - 60.42 - 72.50, places=2)
        gcp_budget.reserve_window(store, 'bench-1', 'stage2', 6, 3)
        self.assertEqual(gcp_budget.committed_cents(store), 6042 + 7250)
        with self.assertRaisesRegex(ValueError, 'conflicts|different window'):
            gcp_budget.reserve_window(store, 'bench-1', 'stage1', 6, 3)

    def test_the_sum_of_receipts_refuses_a_window_above_the_cap(self):
        store = Store()
        gcp_budget.reserve_window(store, 'bench-1', 'stage1', 5, 3)
        gcp_budget.reserve_window(store, 'bench-1', 'stage2', 6, 3)
        gcp_budget.reserve_window(store, 'bench-1', 'stage3', 2, 3)
        with self.assertRaisesRegex(ValueError, 'campaign cap'):
            gcp_budget.admit_campaign(5, 3, store=store,
                                      run_id='bench-1', stage='extra')

    def test_delete_records_the_elapsed_estimate_or_keeps_the_ceiling(self):
        import datetime as dt
        store = Store()
        reserved = gcp_budget.reserve_window(
            store, 'bench-1', 'stage1', 5, 3,
            now=dt.datetime(2026, 11, 26, 10, 0, tzinfo=dt.timezone.utc))
        spend = gcp_budget.record_spend(
            store, 'bench-1', 'stage1',
            now=dt.datetime(2026, 11, 26, 12, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(spend['seconds'], 7200)
        self.assertLess(spend['estimated_cents'], reserved['ceiling_cents'])
        self.assertEqual(gcp_budget.committed_cents(store), spend['estimated_cents'])
        # The window is closed: a second fleet on the same run and stage cannot
        # reuse its receipt, and the closed window is not counted twice.
        with self.assertRaisesRegex(ValueError, 'already spent its window'):
            gcp_budget.reserve_window(store, 'bench-1', 'stage1', 5, 3)
        admitted = gcp_budget.admit_campaign(5, 3, store=store,
                                             run_id='bench-1', stage='stage1')
        self.assertEqual(admitted['committed_usd'], spend['estimated_cents'] / 100)
        self.assertEqual(admitted['fleet_ceiling_usd'], 60.42)
        # No reservation: the ceiling is already the conservative total.
        self.assertIsNone(gcp_budget.record_spend(Store(), 'bench-2', 'stage1'))

    def test_an_unreviewed_campaign_block_is_refused(self):
        path = Path(__file__).with_name('budget.json')
        value = json.loads(path.read_text())
        del value['campaign']['cap_usd']
        with self.assertRaisesRegex(ValueError, 'lacks cap_usd'):
            gcp_budget.admit_campaign(5, 3, value)


if __name__ == '__main__':
    unittest.main()
