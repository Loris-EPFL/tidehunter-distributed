"""The rehearsal's re-create and prefix-purge helpers.

A re-create under the derived run id publishes a second package over the dead
attempt's prefix. `second_package` derives that archive (same admitted tree,
other bytes) and `purge_control_objects` applies `create`'s control-object
predicate before the publish. Both run here without Docker or a cloud store.
"""
import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

import rehearse_fleet


def bundle_bytes(path):
    with tarfile.open(path, 'r:gz') as archive:
        return {member.name: archive.extractfile(member).read()
                for member in archive.getmembers()}


def write_bundle(path, members, level=9):
    with tarfile.open(path, 'w:gz', compresslevel=level) as archive:
        for name, data in sorted(members.items()):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 1700000000
            archive.addfile(info, io.BytesIO(data))


class Package:
    def __init__(self, root):
        self.root = root
        (root / 'package').mkdir(parents=True)
        (root / 'qualification').mkdir()
        (root / 'qualification/receipt.json').write_text('{"status": "completed"}')
        (root / 'package/manifest.json').write_text(json.dumps({'files': {}}))
        write_bundle(root / 'package/bundle.tar.gz',
                     {'manifest.json': b'{"files": {}}',
                      'runtime/bin/kv-research': b'executable'})
        self.receipt = {'sha256': hashlib.sha256(
                            (root / 'package/bundle.tar.gz').read_bytes()).hexdigest(),
                        'archive_path': str((root / 'package/bundle.tar.gz').resolve()),
                        'source_sha256': 'a' * 64, 'source_revision': 'b' * 40,
                        'binary_sha256': 'c' * 64, 'abi': {'needed': []}}
        (root / 'package-receipt.json').write_text(json.dumps(self.receipt))


class SecondPackage(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        self.package = Package(self.source)
        self.destination = self.root / 'second'

    def test_second_package_keeps_the_tree_and_changes_the_archive(self):
        result = rehearse_fleet.second_package(self.source, self.destination)
        self.assertEqual(result, self.destination)
        self.assertEqual((result / 'package/manifest.json').read_bytes(),
                         (self.source / 'package/manifest.json').read_bytes())
        self.assertEqual((result / 'qualification/receipt.json').read_bytes(),
                         (self.source / 'qualification/receipt.json').read_bytes())
        second = result / 'package/bundle.tar.gz'
        self.assertEqual(bundle_bytes(second),
                         bundle_bytes(self.source / 'package/bundle.tar.gz'))
        receipt = json.loads((result / 'package-receipt.json').read_text())
        self.assertEqual(receipt['sha256'],
                         hashlib.sha256(second.read_bytes()).hexdigest())
        self.assertNotEqual(receipt['sha256'], self.package.receipt['sha256'])
        self.assertEqual(receipt['archive_path'], str(second.resolve()))
        for field in ('source_sha256', 'source_revision', 'binary_sha256', 'abi'):
            self.assertEqual(receipt[field], self.package.receipt[field])

    def test_second_package_does_not_touch_the_first_archive(self):
        before = (self.source / 'package/bundle.tar.gz').read_bytes()
        rehearse_fleet.second_package(self.source, self.destination)
        self.assertEqual((self.source / 'package/bundle.tar.gz').read_bytes(), before)
        self.assertNotEqual(
            (self.source / 'package/bundle.tar.gz').stat().st_ino,
            (self.destination / 'package/bundle.tar.gz').stat().st_ino)

    def test_second_package_refuses_an_existing_destination(self):
        self.destination.mkdir()
        with self.assertRaises(rehearse_fleet.Failure):
            rehearse_fleet.second_package(self.source, self.destination)


class Store:
    def __init__(self, names):
        self.names = list(names)
        self.deleted = []

    def list(self, prefix):
        return [dict(name=name, generation=str(index + 1))
                for index, name in enumerate(self.names)
                if name.startswith(prefix)]

    def delete(self, key, *, if_generation_match):
        self.deleted.append((key, int(if_generation_match)))


class PurgeControlObjects(unittest.TestCase):
    def test_purge_removes_only_the_attempt_control_objects(self):
        names = [
            'runs/bench-1/package.tar',
            'runs/bench-1/package.json',
            'runs/bench-1/runtime.json',
            'runs/bench-1/transfer/chunk-0',
            'runs/bench-1/qualification/bench-1-0/gate.json',
            'runs/bench-1/qualification/bench-1-0/worker-ready.json',
            'runs/bench-1/stage1/evidence/S1-0-native/attempt-1/receipt.json',
            'runs/bench-2/package.tar',
        ]
        store = Store(names)
        removed = rehearse_fleet.purge_control_objects(store, 'bench-1')
        self.assertEqual(sorted(key for key, _ in store.deleted), removed)
        self.assertEqual(removed, sorted([
            'runs/bench-1/package.tar',
            'runs/bench-1/package.json',
            'runs/bench-1/runtime.json',
            'runs/bench-1/transfer/chunk-0',
            'runs/bench-1/qualification/bench-1-0/gate.json',
            'runs/bench-1/qualification/bench-1-0/worker-ready.json',
        ]))
        for _, generation in store.deleted:
            self.assertGreater(generation, 0)

    def test_purge_leaves_evidence_and_other_runs(self):
        store = Store([
            'runs/bench-1/stage2/evidence/M5-hold/attempt-1/receipt.json',
            'runs/bench-2/runtime.json',
        ])
        self.assertEqual(rehearse_fleet.purge_control_objects(store, 'bench-1'), [])
        self.assertEqual(store.deleted, [])


class WallScope(unittest.TestCase):
    """A cell passes only on evidence its own output tree produced."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.campaign = Path(self.temp.name)

    def rehearsal(self, cell):
        rehearsal = rehearse_fleet.Rehearsal.__new__(rehearse_fleet.Rehearsal)
        rehearsal.current_cell = cell
        rehearsal.campaign_mount = lambda: self.campaign
        return rehearsal

    def log_command(self, cell):
        captured = []

        class Result:
            stdout = 'host kernel: nvmet is not on mounted configfs'

        def fake_run(argv, **kwargs):
            captured.append(argv[-1])
            return Result()

        rehearsal = self.rehearsal(cell)
        original = rehearse_fleet.run
        rehearse_fleet.run = fake_run
        try:
            text = rehearsal.experiment_logs()
        finally:
            rehearse_fleet.run = original
        self.assertEqual(len(captured), 1)
        self.assertIn('nvmet', text)
        return captured[0]

    def test_experiment_logs_read_only_the_current_cell(self):
        command = self.log_command('fault_cells_M6-w1-c16')
        self.assertIn('/c/out/fault_cells_M6-w1-c16', command)
        self.assertNotIn('/c/out/*', command)
        self.assertNotIn('/c/out/manual_sui', command)

    def test_stage3_reads_only_its_own_root(self):
        command = self.log_command('root')
        self.assertIn('/c/root', command)
        self.assertNotIn('/c/out', command)

    def test_another_cells_marker_does_not_pass_a_cell(self):
        rehearsal = self.rehearsal('fault_cells_M6-w1-c16')
        rehearsal.owner_logs = lambda: [('H1', 'owner.log', 'owner started')]
        rehearsal.experiment_logs = lambda: ''
        result = type('R', (), {'stdout': 'kv-research: invalid type: map, '
                                          'expected path string', 'stderr': ''})()
        ok, detail = rehearsal.wall('fault_cells', result)
        self.assertFalse(ok)
        self.assertIn('no kernel NVMe-oF failure', detail)


if __name__ == '__main__':
    unittest.main()
