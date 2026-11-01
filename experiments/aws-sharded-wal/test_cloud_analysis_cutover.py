"""Current and explicitly archived cloud analysis schemas remain distinct."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import analyze_cloud as analysis
import checkpoint_async
import cluster_ram
import scalar_transport
import stage_timing
import transport_floor


class AnalysisCutover(unittest.TestCase):
    def test_historical_plan_requires_complete_recorded_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = checkpoint_async.historical_plan()[0]
            path = root / (old['case_id'] + '-plan.json')
            path.write_text(json.dumps({k: v for k, v in old.items() if k != 'shard_cache'}))
            self.assertEqual(analysis.selected_plan(root, checkpoint_async), checkpoint_async.plan())
            path.write_text(json.dumps(old))
            (root / 'cloud-build-current.json').write_text(
                json.dumps({'source_tree_sha256': cluster_ram.BASELINE_SOURCE_SHA256}))
            self.assertEqual(analysis.selected_plan(root, checkpoint_async), checkpoint_async.historical_plan())
            self.assertIn(old['case_id'] + '-tcp', analysis.expected_cases('checkpoint-async', root=root))

    def test_legacy_checkpoint_and_ram_require_preserved_source(self):
        for profile, source in ((checkpoint_async, cluster_ram.BASELINE_SOURCE_SHA256),
                                (cluster_ram, scalar_transport.BASELINE_SOURCE_SHA256)):
            with self.subTest(profile=profile.__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                old = profile.historical_plan()[0]
                (root / (old['case_id'] + '-plan.json')).write_text(json.dumps(old))
                receipt = root / 'cloud-build-current.json'
                receipt.write_text(json.dumps({'source_tree_sha256': '0' * 64}))
                with self.assertRaisesRegex(ValueError, 'frozen source'):
                    analysis.selected_plan(root, profile)
                receipt.write_text(json.dumps({'source_tree_sha256': source}))
                self.assertEqual(analysis.selected_plan(root, profile), profile.historical_plan())

    def test_frozen_scalar_schema_requires_its_recorded_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = scalar_transport.historical_plan()[0]
            (root / (old['case_id'] + '-plan.json')).write_text(json.dumps(old))
            receipt = root / 'cloud-build-current.json'
            receipt.write_text(json.dumps({'source_tree_sha256': '0' * 64}))
            with self.assertRaisesRegex(ValueError, 'frozen source'):
                analysis.selected_plan(root, scalar_transport)
            receipt.write_text(json.dumps({'source_tree_sha256': transport_floor.BASELINE_SOURCE_SHA256}))
            self.assertEqual(analysis.selected_plan(root, scalar_transport), scalar_transport.historical_plan())

    def test_historical_remote_binds_transport_endpoints_and_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nodes = [dict(partition=i, private_ip=f'10.0.0.{i + 1}') for i in range(5)]
            spec = dict(transport='efa', lanes=1, cache=0)
            endpoints = ''.join(f"- partition: {n['partition']}\n  address: {n['private_ip']}:7700\n"
                                for n in nodes)
            text = ('storage:\n  partitions: [0, 1, 2, 3, 4]\nendpoints:\n' + endpoints +
                    'tls:\n  transport: efa\nread_lanes: 1\nread_cache_bytes: 0\n')
            path = root / 'case-remote.yaml'
            path.write_text(text)
            analysis.validate_historical_remote(root, 'case', spec, {'nodes': nodes})
            for before, after in (('transport: efa', 'transport: tcp'),
                                  ('10.0.0.1:7700', '10.0.0.9:7700'),
                                  ('read_cache_bytes: 0', 'read_cache_bytes: 1')):
                path.write_text(text.replace(before, after))
                with self.subTest(field=before), self.assertRaises(ValueError):
                    analysis.validate_historical_remote(root, 'case', spec, {'nodes': nodes})

    def test_stage_build_dispatch_keeps_current_and_historical_distinct(self):
        with patch.object(stage_timing, 'validate_build_identity', return_value='current') as current, \
                patch.object(stage_timing, 'historical_validate_build_identity', return_value='historical') as historical:
            self.assertEqual(analysis.validate_profile_build('root', stage_timing, 'current', False), 'current')
            self.assertEqual(analysis.validate_profile_build('root', stage_timing, 'current', True), 'historical')
            current.assert_called_once_with('root', 'current')
            historical.assert_called_once_with('root', 'current')

    def test_current_owner_policy_rejects_retired_fields_and_checks_workers(self):
        spec = dict(build='current', workers=2, member='async')
        launch = 'node:\n  read_workers: 2\n  wal_member_read: async\ntls:\n  transport: tcp\n'
        analysis.validate_owner_launch(launch, spec)
        for suffix in ('  read_cache_bytes: 0\n', '  read_cache_metadata_bytes: 0\n', '  resident_reads: false\n'):
            bad = launch.replace('tls:\n', suffix + 'tls:\n')
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                analysis.validate_owner_launch(bad, spec)
        with self.assertRaises(ValueError):
            analysis.validate_owner_launch(launch + '  resident_reads: false\n', spec)
        with self.assertRaises(ValueError):
            analysis.validate_owner_launch(launch.replace('read_workers: 2', 'read_workers: 1'), spec)
        for bad in ('read_workers: 2\nwal_member_read: async\ntls: {}\n',
                    launch.replace('tls:\n  transport: tcp', 'tls: null')):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, 'node and tls mappings'):
                analysis.validate_owner_launch(bad, spec)

    def test_pressure_comparison_uses_current_cache_zero_control(self):
        specs = checkpoint_async.plan()
        pressure = next(s for s in specs if s['family'] == 'pressure-f131072-tcp')
        core = next(s for s in specs if s['family'] == 'core-f0-tcp' and s['build'] == 'current'
                    and s['value'] == pressure['value'] and s['rep'] == pressure['rep'])
        cases = analysis.plan_cases([pressure, core])
        metric = ('summary', 'point_get', 'p50_ns')
        runs = {name: (info, {metric: 20 if info[0] == pressure['family'] else 10})
                for name, info in cases.items() if info[6] == 'tcp'}
        row = next(r for r in analysis.compare_checkpoint(runs) if r['family'] == pressure['family'])
        self.assertEqual(row['old_median'], 10)
        self.assertEqual(row['new_over_old'], 2)
        self.assertIsNone(row['per_shard_cache_bytes'])


if __name__ == '__main__':
    unittest.main()
