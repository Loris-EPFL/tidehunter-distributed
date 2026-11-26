"""The campaign cap the create path enforces and the manifest publishes."""
import json
from pathlib import Path
import unittest

import gcp_budget


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

    def test_an_unreviewed_campaign_block_is_refused(self):
        path = Path(__file__).with_name('budget.json')
        value = json.loads(path.read_text())
        del value['campaign']['cap_usd']
        with self.assertRaisesRegex(ValueError, 'lacks cap_usd'):
            gcp_budget.admit_campaign(5, 3, value)


if __name__ == '__main__':
    unittest.main()
