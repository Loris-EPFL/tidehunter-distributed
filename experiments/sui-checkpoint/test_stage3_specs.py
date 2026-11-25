"""Stage-3 spec set: 72 cells at the admitted 256 B and 8,192 B sizes.

Every case declares its coordinator value cache in its `-cacheN` tag. The 70
grid cells and the native baseline declare 0; the two reference cells (one per
tier, read-only C=16) declare 1,000. The cluster set carries the external
NVMe-oF deployment of the cell's tier; the rehearsal set keeps the single-host
trusted TCP fixture.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

import deployment
import stage3_specs


def preparation(root, tier, *, hosts=('10.20.0.2', '10.20.0.3'), guest=None):
    """A synthetic `prepare_external.py` output for one acknowledgement tier."""
    root = Path(root)
    root.parent.mkdir(parents=True, exist_ok=True)
    schema = {'checkpoints': {'key_spaces': []}}
    guest = Path(guest) if guest is not None else root / 'guest'
    _, nodes = deployment.prepare(
        root, schema, root / 'remote',
        shape_yamls={name: 'key_spaces: []\n' for name in schema},
        hosts=list(hosts), owners=len(hosts), port=24000, transport='tcp',
        coordinator_cache=0, shard_cache=0, guest_root=guest,
        owner_tier=tier, live_transport='rdma')
    records = []
    for partition, host in enumerate(hosts):
        selected = [node for node in nodes if node['partition'] == partition]
        archive = root / f'host-{partition}.tar.gz'
        with tarfile.open(archive, 'w:gz') as handle:
            for node in selected:
                handle.add(node['launch'],
                           arcname=str(Path(node['launch']).relative_to(root)))
        records.append(dict(
            host=host, partition=partition, archive=str(archive),
            archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
            commands=[[str(guest / 'storage-node'), '--experimental-storage-node',
                       str(guest / Path(node['launch']).relative_to(root))]
                      for node in selected],
            ready_files=[node['ready'] for node in selected],
            stop_files=[node['stop'] for node in selected]))
    manifest = dict(schema=1, protocol=9, transport={'kind': 'tcp'},
                    owners=len(hosts), durability_tier=tier, live_transport='rdma',
                    owner_wal='harness',
                    config_sha256=hashlib.sha256(
                        (root / 'config.json').read_bytes()).hexdigest(),
                    storage_binary_sha256='0' * 64, hosts=records)
    (root / 'external-manifest.json').write_text(json.dumps(manifest, indent=2))
    return root


class StageThreeSpecs(unittest.TestCase):
    def test_case_list_matches_the_plan(self):
        cases = stage3_specs.cells()
        self.assertEqual(len(cases), 72)
        self.assertEqual(len({case['case_id'] for case in cases}), 72)
        counts = {}
        for case in cases:
            family = case['case_id'].split('-')[1][:2]
            counts[family] = counts.get(family, 0) + 1
        # A1 carries the two cache-on reference cells on top of its eight.
        self.assertEqual(counts, {'A1': 10, 'A2': 8, 'A3': 8, 'A4': 6, 'A5': 6,
                                  'A6': 6, 'A7': 4, 'A8': 4, 'A9': 4, 'B1': 8, 'B2': 8})

    def test_every_non_reference_spec_declares_the_value_cache_off(self):
        reference = []
        for case in stage3_specs.cells():
            self.assertEqual(stage3_specs.declared_cache(case['case_id']), case['cache'])
            if case['reference']:
                reference.append(case)
            else:
                self.assertEqual(case['cache'], 0, case['case_id'])
                self.assertIn('-cache0', case['case_id'])
        self.assertEqual(len(reference), 2)
        for case in reference:
            self.assertEqual(case['cache'], 1000)
            self.assertIn('-cache1000', case['case_id'])
            self.assertEqual((case['arm'], case['mix'], case['size'],
                              case['concurrency']), ('A', 'read', 8192, 16))
        self.assertEqual({case['tier'] for case in reference},
                         {'native', 'synchronous'})

    def test_only_admitted_sizes_and_group_widths(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = stage3_specs.write(tmp)
            self.assertEqual(len(paths), 72)
            for path in paths:
                document = json.loads(path.read_text())
                self.assertIn(document['dataset']['value_size'], (256, 8192))
                self.assertEqual(document['dataset']['keys'], 65536)
                self.assertEqual(document['seed'], 1775)
                self.assertEqual(document['limits']['max_operations'], 3_000_000)
                self.assertEqual(document['namespace'], 'stage3-' + document['case_id'])
                cache = stage3_specs.declared_cache(document['case_id'])
                self.assertIn(f'-cache{cache}', document['namespace'])
                self.assertEqual(document['backend']['options']['value_cache_size'], cache)
                if '-cache1000' not in document['case_id']:
                    self.assertEqual(cache, 0)
                # The cache is declared in the label; the engine document keeps
                # only fields kv-research admits (no unknown cache key).
                self.assertEqual(set(document), {
                    'schema_version', 'case_id', 'backend', 'seed', 'namespace',
                    'dataset', 'workload', 'execution', 'phases', 'diagnostics',
                    'qualification_only', 'source_revision', 'limits'})
                mix = document['workload']['mix']
                if 'put_batch' in path.name or 'get_many' in path.name or 'A7' in path.name or 'A8' in path.name:
                    self.assertEqual(document['workload']['batch_size'], 26)
                    self.assertTrue(mix['put_batch'] or mix['get_many'])
                if path.name.startswith('S3-B'):
                    self.assertEqual(document['backend']['kind'], 'native')
                else:
                    self.assertEqual(document['backend']['options']['owners'], 2)
                    self.assertIn(document['backend']['options']['durability_tier'],
                                  ('native', 'synchronous'))
                    # Rehearsal keeps the single-host trusted TCP fixture.
                    self.assertEqual(document['backend']['options']['transport'],
                                     {'kind': 'tcp-trusted'})
                    self.assertNotIn('external_remote_config',
                                     document['backend']['options'])
                self.assertIn(document['workload']['distribution'],
                              ('uniform', {'zipf': {'exponent': 1.0}}))

    def test_every_document_passes_runconfig_admission(self):
        binary = os.environ.get('KV_RESEARCH_BIN')
        if not binary:
            self.skipTest('set KV_RESEARCH_BIN to validate RunConfig admission')
        with tempfile.TemporaryDirectory() as tmp:
            for path in stage3_specs.write(tmp):
                subprocess.run([binary, '--config', str(path), '--validate-config',
                                '--output', str(Path(tmp) / 'admission')],
                               check=True, stdout=subprocess.DEVNULL, timeout=60)


class Counts(unittest.TestCase):
    def test_family_counts_drive_the_documents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            counts = dict(read=21_000, mixed=22_000, write=23_000,
                          get_many=24_000, put_batch=25_000)
            by_family = {}
            for path in stage3_specs.write(root, counts=counts):
                document = json.loads(path.read_text())
                by_family[document['case_id']] = document['phases']['measurement_operations']
                self.assertEqual(document['phases']['warmup_operations'],
                                 int(document['phases']['measurement_operations'] * 0.1))
            self.assertEqual(by_family['S3-A1-c2-native-cache0'], 21_000)
            self.assertEqual(by_family['S3-A9-get_many-c16-native-cache0'], 24_000)
            self.assertEqual(by_family['S3-A9-put_batch-c16-native-cache0'], 25_000)

    def test_one_count_for_every_family_is_the_pre_calibration_default(self):
        self.assertEqual(set(stage3_specs.operation_counts()),
                         set(stage3_specs.FAMILIES))
        self.assertEqual(set(stage3_specs.operation_counts() .values()),
                         {stage3_specs.MEASUREMENT_OPERATIONS})
        with self.assertRaisesRegex(ValueError, 'unknown operation family'):
            stage3_specs.operation_counts(overrides={'delete': 20_000})
        with self.assertRaisesRegex(ValueError, 'out of range'):
            stage3_specs.operation_counts(overrides={'read': 0})
        with self.assertRaisesRegex(ValueError, 'out of range'):
            stage3_specs.operation_counts(overrides={'read': stage3_specs.CEILING + 1})

    def test_calibration_is_forty_seconds_bounded(self):
        counts = stage3_specs.calibration_counts(
            {'read': 1_000, 'mixed': 1, 'write': 100_000})
        self.assertEqual(counts['read'], 40_000)
        self.assertEqual(counts['get_many'], 40_000)
        self.assertEqual(counts['mixed'], stage3_specs.FLOOR)
        self.assertEqual(counts['write'], stage3_specs.CEILING)
        self.assertEqual(counts['put_batch'], stage3_specs.CEILING)
        with self.assertRaisesRegex(ValueError, 'no measured rate'):
            stage3_specs.calibration_counts({'read': 1.0})

    def test_the_probe_is_one_arm_b_synchronous_write_cell(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = stage3_specs.write_probe(root, native_root=Path('/mnt/store'))
            document = json.loads(path.read_text())
            self.assertEqual(document['case_id'], stage3_specs.PROBE_CASE)
            self.assertEqual(document['backend']['kind'], 'native')
            self.assertEqual(document['backend']['options']['durability'],
                             'value_wal_barrier')
            self.assertTrue(document['backend']['options']['path'].startswith(
                '/mnt/store/stage3/db/'))
            self.assertEqual(document['execution']['concurrency'], 8)
            self.assertEqual(document['workload']['mix']['put'], 100)
            self.assertEqual(document['phases']['measurement_operations'],
                             stage3_specs.PROBE_OPERATIONS)
            self.assertEqual(document['dataset']['value_size'], 8192)
            self.assertNotIn('external_remote_config',
                             document['backend']['options'])
            stage3_specs.write_probe(root, native_root=Path('/mnt/store'))
            with self.assertRaisesRegex(ValueError, 'differs from these settings'):
                stage3_specs.write_probe(root, native_root=Path('/mnt/other'))


class ClusterSpecs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.preparations = {
            tier: stage3_specs.load_external(
                preparation(self.base / tier, tier), tier=tier)
            for tier in stage3_specs.TIERS}

    def documents(self, **kwargs):
        root = self.base / 'root'
        paths = stage3_specs.write(root, 2_000, **kwargs)
        return root, {path.stem: json.loads(path.read_text()) for path in paths}

    def test_distributed_cells_carry_the_external_nvmeof_deployment(self):
        root, documents = self.documents(external=self.preparations)
        distributed = [document for document in documents.values()
                       if document['backend']['kind'] == 'tidehunter']
        self.assertEqual(len(distributed), 56)
        for document in distributed:
            options = document['backend']['options']
            tier = options['durability_tier']
            self.assertEqual(options['transport'], {'kind': 'tcp'})
            self.assertNotIn('index', options)
            self.assertNotIn('local_geometry', options)
            self.assertNotIn('durable_cut', options)
            self.assertEqual(options['coordinator_read_cache_bytes'], 0)
            self.assertEqual(options['owners'], 2)
            remote = options['external_remote_config']
            self.assertEqual(remote, self.preparations[tier]['document'])
            self.assertEqual(remote['live_nvme']['leased_durability'], tier)
            self.assertEqual(remote['read_cache_bytes'], 0)
            self.assertEqual([endpoint['partition'] for endpoint in remote['endpoints']],
                             [0, 1])
            self.assertEqual(
                options['external_owner_durability_receipts'],
                [str(stage3_specs.receipt_path(root, tier, partition))
                 for partition in (0, 1)])
            self.assertEqual(
                options['evidence'],
                str(root / 'stage3' / 'db' / document['case_id'] / 'evidence'))
            self.assertEqual(options['value_cache_size'],
                             stage3_specs.declared_cache(document['case_id']))

    def test_arm_b_keeps_the_native_store_root_of_the_owner_host(self):
        native = Path('/mnt/tidehunter/campaign')
        _, documents = self.documents(external=self.preparations, native_root=native)
        native_cells = [document for document in documents.values()
                        if document['backend']['kind'] == 'native']
        self.assertEqual(len(native_cells), 16)
        for document in native_cells:
            options = document['backend']['options']
            self.assertTrue(options['path'].startswith(f'{native}/stage3/db/'))
            self.assertEqual(options['durability'],
                             'value_wal_barrier'
                             if 'synchronous' in document['case_id'] else 'page_cache')
        reachable = [document for document in documents.values()
                     if document['backend']['kind'] == 'tidehunter']
        for document in reachable:
            self.assertTrue(document['backend']['options']['path'].startswith(
                str(self.base / 'root')))

    def test_a_preparation_must_match_the_cell_tier_and_the_control_transport(self):
        native = preparation(self.base / 'mismatch', 'native')
        with self.assertRaisesRegex(ValueError, 'is not'):
            stage3_specs.load_external(native, tier='synchronous')
        manifest_path = native / 'external-manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['transport'] = {'kind': 'tcp-trusted'}
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, 'mTLS TCP'):
            stage3_specs.load_external(native, tier='native')
        with self.assertRaisesRegex(ValueError, 'missing synchronous'):
            stage3_specs.external_map([
                ('native', preparation(self.base / 'only', 'native'))])

    def test_a_changed_setting_is_never_overwritten(self):
        root, _ = self.documents(external=self.preparations)
        stage3_specs.write(root, 2_000, external=self.preparations)
        with self.assertRaisesRegex(ValueError, 'differs from these settings'):
            stage3_specs.write(root, 3_000, external=self.preparations)
        with self.assertRaisesRegex(ValueError, 'differs from these settings'):
            stage3_specs.write(root, 2_000)

    def test_the_cluster_set_passes_runconfig_admission(self):
        binary = os.environ.get('KV_RESEARCH_BIN')
        if not binary:
            self.skipTest('set KV_RESEARCH_BIN to validate RunConfig admission')
        root, _ = self.documents(external=self.preparations,
                                 native_root=Path('/mnt/tidehunter/campaign'))
        probe = stage3_specs.write_probe(root, native_root=Path('/mnt/tidehunter/campaign'),
                                         external=self.preparations)
        for path in [*sorted((root / 'stage3' / 'spec').iterdir()), probe]:
            subprocess.run([binary, '--config', str(path), '--validate-config',
                            '--output', str(root / 'stage3' / 'admission')],
                           check=True, stdout=subprocess.DEVNULL, timeout=60)


if __name__ == '__main__':
    unittest.main()
