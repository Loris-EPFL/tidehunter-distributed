import tempfile
import json
import io
import tarfile
import subprocess
from pathlib import Path
import unittest
from unittest.mock import patch

import prepare


class CheckoutAdmissionTest(unittest.TestCase):
    def test_clean_exact_revision_is_accepted(self):
        revision = 'a' * 40
        with tempfile.TemporaryDirectory() as directory, patch.object(prepare.cloud_package, 'command', side_effect=[revision, '']):
            prepare.checkout(Path(directory), revision)

    def test_dirty_or_different_revision_is_rejected(self):
        revision = 'a' * 40
        for actual, status in [(revision, ' M Cargo.lock'), ('b' * 40, '')]:
            with self.subTest(actual=actual, status=status), tempfile.TemporaryDirectory() as directory:
                with patch.object(prepare.cloud_package, 'command', side_effect=[actual, status]), self.assertRaises(ValueError):
                    prepare.checkout(Path(directory), revision)

    def test_revision_must_be_full_commit(self):
        with self.assertRaises(ValueError):
            prepare.checkout(Path('.'), 'abc')

    def test_selected_industry_features_are_required(self):
        spec = {'suite': 'kv', 'backends': ['garnet', 'kvrocks']}
        prepare.admit_selected_features(spec, {'features': ['garnet', 'kvrocks']})
        for features in ([], ['garnet'], ['kvrocks']):
            with self.subTest(features=features), self.assertRaisesRegex(ValueError, 'lacks selected'):
                prepare.admit_selected_features(spec, {'features': features})
        prepare.admit_selected_features({'suite': 'kv', 'backends': ['native']}, {'features': []})
        prepare.admit_selected_features({'suite': 'sui', 'backends': ['garnet']}, {'features': []})


    def test_container_rejects_unresolved_image_and_ambiguous_mount(self):
        with patch.object(prepare.cloud_package, 'command', return_value='builder'):
            with self.assertRaisesRegex(ValueError, 'immutable'):
                prepare.container_build(Path('/engine'), Path('/output'), 'builder', 10, 9000, False)
        with patch.object(prepare.cloud_package, 'command', return_value='sha256:' + 'a' * 64):
            with self.assertRaisesRegex(ValueError, 'commas'):
                prepare.container_build(Path('/engine'), Path('/out,put'), 'builder', 10, 9000, False)

    def test_rocky_abi_rejects_newer_runtime_symbols(self):
        abi = {'version_requirements': {'GLIBC': ['2.34'], 'GLIBCXX': ['3.4.29']}}
        prepare.admit_rocky9_abi(abi)
        for family, version in [('GLIBC', '2.35'), ('GLIBCXX', '3.4.30')]:
            bad = {'version_requirements': dict(abi['version_requirements'], **{family: [version]})}
            with self.subTest(family=family), self.assertRaisesRegex(ValueError, family):
                prepare.admit_rocky9_abi(bad)

class BuildContainerOwnershipTest(unittest.TestCase):
    def inspect_result(self, owner):
        container = {'Id': 'a' * 64, 'Name': '/owned',
                     'Config': {'Labels': {'io.tidehunter.gcp-build.owner': owner}}}
        return subprocess.CompletedProcess([], 0, json.dumps([container]), '')

    def test_foreign_container_is_preserved(self):
        with patch.object(prepare.subprocess, 'run', return_value=self.inspect_result('foreign')) as docker:
            with self.assertRaisesRegex(ValueError, 'unowned'):
                prepare.remove_build_container('owned', 'ours')
            self.assertEqual(docker.call_count, 1)

    def test_daemon_failure_does_not_claim_absence(self):
        result = subprocess.CompletedProcess([], 1, '', 'Cannot connect to the Docker daemon')
        with patch.object(prepare.subprocess, 'run', return_value=result):
            with self.assertRaisesRegex(RuntimeError, 'ownership'):
                prepare.remove_build_container('owned', 'ours')

    def test_container_exit_race_is_already_clean(self):
        absent = subprocess.CompletedProcess([], 1, '', 'No such container: ' + 'a' * 64)
        with patch.object(prepare.subprocess, 'run', side_effect=[self.inspect_result('ours'), absent]):
            prepare.remove_build_container('owned', 'ours')

class TransferAdmissionTest(unittest.TestCase):
    def test_rejects_qualification_from_another_binary_before_rebinding(self):
        revision = 'a' * 40
        verified = {'source_sha256': 'b' * 64, 'binary_sha256': 'c' * 64, 'binary_blake2b512': 'd' * 128,
                    'manifest_sha256': 'e' * 64, 'abi': {'needed': [], 'version_requirements': {}}}
        source = {'revision': revision, 'dirty': False, 'sha256': {'Cargo.lock': 'e' * 64}}
        receipt = verified | {'source_revision': revision, 'sha256': 'f' * 64,
                              'qualification': {'engine_sha': revision, 'binary_sha256': '0' * 64,
                                                'binary_blake2b512': verified['binary_blake2b512']}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'package-receipt.json').write_text(json.dumps(receipt))
            (root / 'package').mkdir()
            (root / 'package/bundle.tar.gz').write_bytes(b'fixture')
            with patch.object(prepare, 'checkout'), \
                 patch.object(prepare.cloud_package, 'verify_directory', return_value=verified), \
                 patch.object(prepare.cloud_package, 'verify_archive', return_value=verified), \
                 patch.object(prepare.cloud_package, 'admit_source', return_value=source), \
                 patch.object(prepare.cloud_package, 'inventory', return_value=source['sha256']), \
                 patch.object(prepare.ci_prepare, 'sha', return_value='f' * 64), \
                 self.assertRaisesRegex(ValueError, 'qualification'):
                prepare.admit_transfer(root, revision)

    def test_sui_transfer_rejects_unsafe_outer_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = root / 'build.tar.gz'
            verification = root / 'verification.tar.gz'
            with tarfile.open(bundle, 'w:gz') as archive:
                member = tarfile.TarInfo('../escape')
                member.size = 1
                archive.addfile(member, io.BytesIO(b'x'))
            with tarfile.open(verification, 'w:gz'):
                pass
            with self.assertRaises(ValueError):
                prepare.admit_sui_archives(bundle, verification)


if __name__ == '__main__':
    unittest.main()
