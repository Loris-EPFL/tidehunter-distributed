import hashlib
from datetime import datetime, timezone
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import gcp_sui


def put(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data) if isinstance(data, dict) else data)


class Store:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def download(self, key, path, *, generation, max_bytes):
        self.calls.append((key, generation))
        Path(path).write_bytes(self.data)


class SuiAdmissionTests(unittest.TestCase):
    def test_archive_preflight_and_extraction_share_symlink_safety(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / 'unsafe.tar.gz'
            with tarfile.open(archive, 'w:gz') as stream:
                link = tarfile.TarInfo('fixture/link')
                link.type = tarfile.SYMTYPE
                link.linkname = '../../escape'
                stream.addfile(link)
            with self.assertRaisesRegex(ValueError, 'symlink escapes'):
                gcp_sui.inspect_archive(archive)
            with self.assertRaisesRegex(ValueError, 'symlink escapes'):
                gcp_sui.extract(archive, root / 'unpack')

    def test_local_ssd_mount_is_required(self):
        run_id = 'th-0123456789abcdef'
        with patch.object(Path, 'is_symlink', return_value=True), \
             patch.object(Path, 'resolve', return_value=Path('/dev/nvme0n1')), \
             patch.object(gcp_sui.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout='/dev/nvme0n1')) as findmnt:
            self.assertEqual(gcp_sui.storage_root(run_id), Path('/mnt/tidehunter-kv-comparison') / run_id)
            self.assertIn(str(Path('/mnt/tidehunter-kv-comparison') / run_id), findmnt.call_args.args[0])
        with patch.object(Path, 'is_symlink', return_value=True), \
             patch.object(gcp_sui.subprocess, 'run', return_value=SimpleNamespace(returncode=1, stdout='')):
            with self.assertRaisesRegex(ValueError, 'not mounted'):
                gcp_sui.storage_root(run_id)

    def test_owner_deadline_respects_fleet_and_supervisor_limits(self):
        now = 1_700_000_000
        stamp = lambda seconds: datetime.fromtimestamp(seconds, timezone.utc).isoformat()
        self.assertEqual(datetime.fromisoformat(gcp_sui.owner_deadline(stamp(now + 14400), now)).timestamp(), now + 7000)
        self.assertEqual(datetime.fromisoformat(gcp_sui.owner_deadline(stamp(now + 600), now)).timestamp(), now + 600)
        with self.assertRaises(TimeoutError):
            gcp_sui.owner_deadline(stamp(now + 60), now)

    def test_generation_and_hash_are_both_required(self):
        data = b'checkpoint archive bytes'
        store = Store(data)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'fixture'
            desc = dict(key='fixtures/real.tar.gz', generation='42', sha256=hashlib.sha256(data).hexdigest())
            gcp_sui.fetch(store, desc, path)
            self.assertEqual(store.calls, [('fixtures/real.tar.gz', '42')])
            with self.assertRaises(ValueError):
                gcp_sui.fetch(store, {**desc, 'sha256': '0' * 64}, path)
            with self.assertRaises(ValueError):
                gcp_sui.fetch(store, {**desc, 'generation': None}, path)

    def test_unsupported_transports_are_explicitly_rejected(self):
        self.assertEqual(gcp_sui.selected_transports({}), ['tcp'])
        self.assertEqual(gcp_sui.selected_transports({'sui_transports': ['tcp', 'tidehunter_verbs']}), ['tcp', 'tidehunter_verbs'])
        for backend in ('nvme-tcp', 'nvme-rdma', 'efa', 'fabric-tcp', 'fabric-tcp-native'):
            with self.assertRaisesRegex(ValueError, 'unsupported Sui transport'):
                gcp_sui.selected_transports({'sui_transports': [backend]})

    def test_revision_mismatch_fails_before_build_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            put(root / 'sources.tar.gz', 'source')
            put(root / 'source-manifest.json', {'sui': {'revision': 'sui-sha'}, 'tidehunter-sharded-wal': {'revision': 'different'}})
            plan = {'protocol': 15, 'source_archive_sha256': gcp_sui.sha(root / 'sources.tar.gz'),
                    'source_manifest_sha256': gcp_sui.sha(root / 'source-manifest.json'), 'fabric': False}
            put(root / 'plan.json', plan)
            put(root / 'build-manifest.json', {**plan, 'binaries': dict.fromkeys(gcp_sui.BINARIES, '0' * 64)})
            with self.assertRaisesRegex(ValueError, 'revision differs'):
                gcp_sui.admit_build(root, {'sui_revision': 'sui-sha'}, 'expected-engine')

    def test_local_audit_is_bound_to_fixture_and_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture, verification, build = root / 'fixture', root / 'verification', root / 'build'
            put(fixture / 'producer/state', 'real producer')
            prior = {'source_frozen': True, 'failure': None, 'checkpoint_limit': gcp_sui.TIP,
                     'checkpoint_digest': gcp_sui.CHECKPOINT, 'verified_source_range': {'start': 0, 'end': gcp_sui.TIP}}
            put(fixture / 'experiment/results.json', prior)
            expected = {str(p.relative_to(fixture)): gcp_sui.sha(p) for p in fixture.rglob('*') if p.is_file()}
            put(fixture / 'fixture-manifest.json', {'files': expected, 'checkpoint_limit': gcp_sui.TIP,
                'checkpoint_digest': gcp_sui.CHECKPOINT, 'source_range': prior['verified_source_range']})
            put(build / 'build-manifest.json', {'build': 'identity'})
            manifest = {'source_archive_sha256': 'a' * 64, 'binaries': {'node': 'b' * 64}}
            audit = {'all_equal': True, 'databases': {'a': {}}, 'rocksdb': {'a': {}}}
            result = {'complete': True, 'build_manifest_sha256': gcp_sui.sha(build / 'build-manifest.json'),
                'source_archive_sha256': manifest['source_archive_sha256'], 'binaries': manifest['binaries'],
                'checkpoint_digest': gcp_sui.CHECKPOINT, 'checkpoint_limit': gcp_sui.TIP,
                'source_range': prior['verified_source_range'], 'topology': 'loopback', 'audit': audit,
                'fixture_files': expected, 'native': {'completed_checkpoint_digest': gcp_sui.CHECKPOINT},
                'remote': {'completed_checkpoint_digest': gcp_sui.CHECKPOINT}}
            put(verification / 'experiment/results.json', result)
            put(verification / 'experiment/audit.json', audit)
            put(verification / 'experiment/native-schema/db/shape.yaml', 'schema')
            with tarfile.open(verification / 'evidence.tar.gz', 'w:gz') as tar:
                tar.add(verification / 'experiment', arcname='experiment')
            put(verification / 'receipt.json', {'complete': True, 'evidence_sha256': gcp_sui.sha(verification / 'evidence.tar.gz'),
                'build_manifest_sha256': result['build_manifest_sha256'], 'source_archive_sha256': manifest['source_archive_sha256']})
            gcp_sui.admit_fixture(fixture, verification, manifest, build)
            put(fixture / 'producer/state', 'altered')
            with self.assertRaisesRegex(ValueError, 'fixture content differs'):
                gcp_sui.admit_fixture(fixture, verification, manifest, build)
            put(fixture / 'producer/state', 'real producer')
            audit['all_equal'] = False
            put(verification / 'experiment/audit.json', audit)
            with self.assertRaisesRegex(ValueError, 'audit is incomplete'):
                gcp_sui.admit_fixture(fixture, verification, manifest, build)

    def test_remote_failure_does_not_qualify(self):
        with self.assertRaises(RuntimeError):
            gcp_sui.check_receipt({'exit_code': 1, 'stderr': 'audit failed'})


if __name__ == '__main__':
    unittest.main()
