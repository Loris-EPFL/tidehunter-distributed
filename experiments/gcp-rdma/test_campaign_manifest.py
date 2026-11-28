"""The campaign manifest and the initial ledger written before any rented host."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'sui-checkpoint'))
import campaign_ledger
import campaign_manifest as manifest


class ManifestBindings(unittest.TestCase):
    def test_owner_decision_budget_and_stage_ceilings_agree(self):
        value = manifest.manifest()
        self.assertEqual(value['campaign']['id'], 'campaign-hybrid-sui-2026-11-25')
        self.assertEqual(value['owner']['decided_utc'], '2026-11-26')
        self.assertEqual(value['budget']['cap_usd'], 500.00)
        self.assertEqual(value['budget']['spent_usd'], 76.40)
        self.assertEqual(value['budget']['remaining_usd'], 423.60)
        self.assertEqual(value['owner']['cap_usd'], value['budget']['cap_usd'])
        ceilings = {stage['id']: stage['ceiling_usd'] for stage in value['stages']}
        self.assertEqual(ceilings, {'stage1': 60.42, 'stage2': 72.50, 'stage3': 24.17})
        # The manifest row is the ceiling gcp_budget.admit_campaign enforces.
        self.assertEqual({str(stage['window_hours']): stage['ceiling_usd']
                          for stage in value['stages']},
                         {'5': value['budget']['fleet_ceilings_usd']['5'],
                          '6': value['budget']['fleet_ceilings_usd']['6'],
                          '2': value['budget']['fleet_ceilings_usd']['2']})
        self.assertAlmostEqual(
            value['budget']['cap_usd'] - value['budget']['spent_usd']
            - sum(ceilings.values()), value['stages'][-1]['entry_remaining_usd']
            - value['stages'][-1]['ceiling_usd'], places=2)
        for stage in value['stages']:
            self.assertLess(stage['ceiling_usd'], stage['stop_usd'])

    def test_revision_bindings_name_the_frozen_campaign_build(self):
        revisions = manifest.manifest()['revisions']
        self.assertEqual(revisions['engine'],
                         '2bff0e8a4628aa65363bc0e3b1d24817ee90585e')
        self.assertEqual(revisions['sui'],
                         '11b95aec95f386b29d9a074c9ad7e642d416f55c')
        self.assertEqual(revisions['bundle_sha256'],
                         'c7b69b8bd942a119bcced33e0e5dbc4222ee47650306f03471c8b42ad0230fa9')
        self.assertEqual(revisions['schema_digest'],
                         '03f5401b4b795879c1236b2d3607bd06dfa3de1d6b426597547758518836ff09')
        self.assertEqual(set(revisions['binaries']), {
            'sui-node-native', 'sui-node-remote', 'rocksdb-audit', 'storage-node',
            'state-audit'})
        self.assertEqual(revisions['fixture']['checkpoint_limit'], 1775)
        self.assertEqual(revisions['build_features'], dict(
            fabric=False, protocol=9, native='typed-store/tidehunter',
            remote='sui-node/sharded-wal'))

    def test_price_quote_is_the_rechecked_launcher_constant(self):
        price = manifest.manifest()['price']
        self.assertEqual(price['machine'], 'z4m-highmem-16')
        self.assertEqual(price['nanodollars_per_host_hour'], 3287109973)
        self.assertEqual(price['usd_per_host_hour'], 3287109973 / 1e9)
        self.assertAlmostEqual(price['ceiling_factor'], 14500 / 11834)
        self.assertEqual(price['observed_utc'], '2026-11-02T12:38:36Z')

    def test_case_list_matches_the_plan_counts(self):
        cases = manifest.manifest()['cases']
        self.assertEqual([case['id'] for case in cases['stage1']],
                         [f'S1-{index}' for index in range(9)])
        # The stage-2 rows are the cells the drivers build, not the plan's older
        # per-keyspace count, and their minutes fit the 6 h window. M8 is the
        # stage-1 S1-5S alias, not a stage-2 run.
        self.assertEqual(sum(case['cells'] for case in cases['stage2']), 33)
        self.assertEqual(sum(case['est_minutes'] for case in cases['stage2']), 206)
        self.assertEqual({case['id']: case['cells'] for case in cases['stage2']},
                         {'M1': 7, 'M2': 4, 'M3': 2, 'M4': 2, 'M5': 1, 'M6': 9,
                          'M7': 5, 'M8': 0, 'M9': 2, 'M13': 1})
        alias = next(case for case in cases['stage2'] if case['id'] == 'M8')
        self.assertEqual((alias['alias_of'], alias['alias_stage']), ('S1-5S', 'stage1'))
        self.assertEqual(sum(case['cells'] for case in cases['stage1']), 34)
        self.assertEqual({case['id']: case['driver'] for case in cases['stage1']},
                         {'S1-0': 'manual_sui.py', 'S1-1': 'manual_sui.py',
                          'S1-2': 'fault_cells.py', 'S1-3': 'fault_cells.py',
                          'S1-4': 'manual_sui.py --controller-loss',
                          'S1-5': 'fault_cells.py', 'S1-6': 'fault_cells.py',
                          'S1-7': 'manual_sui.py', 'S1-8': 'manual_sui.py'})
        controller_loss = [case for case in cases['stage1'] + cases['stage2']
                           if case['id'] in ('S1-4', 'M7')]
        self.assertEqual([case['cells'] for case in controller_loss], [5, 5])
        hold = next(case for case in cases['stage2'] if case['id'] == 'M5')
        self.assertEqual(hold['hold_minutes'], 30)
        stage3 = cases['stage3']
        distributed = sum(case['cells'] for case in stage3['distributed'])
        native = sum(case['cells'] for case in stage3['native_reference'])
        cache_on = {case['arm']: case['cells'] for case in stage3['cache_on_reference']}
        self.assertEqual(distributed + cache_on['distributed'], 55)
        self.assertEqual(native + cache_on['native'], 17)
        self.assertEqual(distributed + native + sum(cache_on.values()),
                         stage3['cell_total'])
        self.assertEqual(stage3['totals'], {'distributed_cells': 55, 'native_cells': 17,
                                            'total_cells': 72})

    def test_a_rebuilt_bundle_can_rebind_the_three_build_hashes(self):
        value = manifest.manifest(revisions={'bundle_sha256': 'f' * 64})
        self.assertEqual(value['revisions']['bundle_sha256'], 'f' * 64)
        self.assertEqual(value['revisions']['engine'],
                         manifest.REVISIONS['engine'])


class BucketPrefix(unittest.TestCase):
    def test_the_campaign_prefix_carries_the_reviewed_bucket(self):
        self.assertEqual(manifest.parse_bucket_prefix(
            'gs://mysten-research-bench-artifacts/campaign-hybrid-sui-2026-11-25'),
            ('mysten-research-bench-artifacts', 'campaign-hybrid-sui-2026-11-25'))
        self.assertEqual(manifest.bucket_prefix(),
                         'gs://mysten-research-bench-artifacts/campaign-hybrid-sui-2026-11-25')
        self.assertEqual(manifest.ledger_key('p'), 'p/campaign-ledger.json')
        self.assertEqual(manifest.evidence_key('p', 'stage1', 'S1-1', 'evidence.tar.gz'),
                         'p/stage1/evidence/S1-1/attempt-1/evidence.tar.gz')
        self.assertEqual(manifest.evidence_key('p', 'stage1', 'S1-1', 'results.json', 2),
                         'p/stage1/evidence/S1-1/attempt-2/results.json')
        # Per-sample files expire with the bucket rule on raw/.
        for name in ('samples.csv', 'resources.csv', 'disk.csv',
                     'native-foreground.prom', 'native-samples.jsonl',
                     'measure/run/samples.csv'):
            with self.subTest(name=name):
                self.assertIn('/raw/', manifest.evidence_key(
                    'p', 'stage2', 'M1', name))
        for name in ('summary.json', 'run.json', 'receipt.json',
                     'measure/run/manifest.json'):
            with self.subTest(name=name):
                self.assertIn('/evidence/', manifest.evidence_key(
                    'p', 'stage2', 'M1', name))

    def test_another_bucket_or_an_empty_path_is_refused(self):
        for value in ('mysten-research-bench-artifacts/campaign',
                      'gs://other-bucket/campaign',
                      'gs://mysten-research-bench-artifacts',
                      'gs://mysten-research-bench-artifacts/',
                      'gs://mysten-research-bench-artifacts//campaign'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                manifest.parse_bucket_prefix(value)
        for name in ('', '/absolute', '../escape'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                manifest.evidence_key('p', 'stage1', 'S1-1', name)
        for attempt in (0, -1, '1', True, 1.0):
            with self.subTest(attempt=attempt), self.assertRaises(ValueError):
                manifest.evidence_key('p', 'stage1', 'S1-1', 'results.json', attempt)


class LocalFiles(unittest.TestCase):
    def test_writes_the_manifest_and_an_empty_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt = manifest.write(tmp)
            directory = Path(tmp)
            value = json.loads((directory / 'campaign-manifest.json').read_text())
            self.assertEqual(value['campaign']['id'], manifest.CAMPAIGN_ID)
            self.assertEqual(value['storage']['ledger_key'],
                             f'{manifest.CAMPAIGN_ID}/campaign-ledger.json')
            self.assertEqual(value['storage']['state'].split(';')[0], 'local only')
            self.assertEqual(campaign_ledger.Ledger(directory / 'campaign-ledger.json')
                             .load().entries, {})
            self.assertEqual(receipt['manifest_sha256'],
                             campaign_ledger.sha256_file(
                                 directory / 'campaign-manifest.json'))
            self.assertEqual(receipt['ledger_sha256'],
                             campaign_ledger.sha256_file(
                                 directory / 'campaign-ledger.json'))

    def test_a_started_ledger_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            manifest.write(directory)
            ledger = campaign_ledger.Ledger(directory / 'campaign-ledger.json')
            ledger.begin(stage='stage1', cell_id='S1-0', config_hash='h')
            ledger.save()
            before = (directory / 'campaign-ledger.json').read_bytes()
            with self.assertRaisesRegex(ValueError, 'already records 1 cell'):
                manifest.write(directory)
            self.assertEqual((directory / 'campaign-ledger.json').read_bytes(), before)
            with self.assertRaisesRegex(ValueError, 'already records 1 cell'):
                manifest.write(directory, revisions={'bundle_sha256': 'b' * 64})
            self.assertEqual((directory / 'campaign-ledger.json').read_bytes(), before)

    def test_an_empty_ledger_is_rewritten_in_place(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            manifest.write(directory)
            first = (directory / 'campaign-ledger.json').read_bytes()
            manifest.write(directory, revisions={'bundle_sha256': 'a' * 64})
            self.assertEqual((directory / 'campaign-ledger.json').read_bytes(), first)
            value = json.loads((directory / 'campaign-manifest.json').read_text())
            self.assertEqual(value['revisions']['bundle_sha256'], 'a' * 64)

    def test_print_upload_prints_generation_pinned_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest.write(tmp)
            commands = manifest.upload_commands(tmp)
            self.assertEqual(len(commands), 2)
            for command, name in zip(commands, ('campaign-manifest.json',
                                                'campaign-ledger.json')):
                self.assertTrue(command.startswith('gcloud storage cp '))
                self.assertIn(str(Path(tmp) / name), command)
                self.assertIn(
                    f' gs://mysten-research-bench-artifacts/{manifest.CAMPAIGN_ID}/{name} ',
                    command)
                self.assertIn('--impersonate-service-account=' + manifest.ACCOUNT, command)
                self.assertTrue(command.endswith('--if-generation-match=0'))

    def test_another_campaign_id_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            manifest.write(tmp, campaign_id='campaign-other')


if __name__ == '__main__':
    unittest.main()


class BuildBinding(unittest.TestCase):
    def test_a_finished_build_rebinds_every_revision_and_binary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'build-receipt.json').write_text(json.dumps(dict(
                bundle_sha256='b' * 64, build_manifest_sha256='m' * 64,
                source_archive_sha256='s' * 64)))
            (root / 'build-manifest.json').write_text(json.dumps(dict(
                binaries={'storage-node': '1' * 64, 'sui-node-native': '2' * 64})))
            (root / 'source-manifest.json').write_text(json.dumps(dict(
                sui=dict(revision='3' * 40),
                **{'tidehunter-sharded-wal': dict(revision='4' * 40)})))
            revisions = manifest.build_revisions(root)
            value = manifest.manifest(revisions=revisions)
            self.assertEqual(value['revisions']['engine'], '4' * 40)
            self.assertEqual(value['revisions']['sui'], '3' * 40)
            self.assertEqual(value['revisions']['bundle_sha256'], 'b' * 64)
            self.assertEqual(value['revisions']['binaries']['storage-node'], '1' * 64)
            self.assertEqual(value['revisions']['engine_base'],
                             manifest.REVISIONS['engine_base'])
