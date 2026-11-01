import tempfile
import json
import io
import tarfile
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

class TransferAdmissionTest(unittest.TestCase):
    def test_rejects_qualification_from_another_binary_before_rebinding(self):
        revision = 'a' * 40
        verified = {'source_sha256': 'b' * 64, 'binary_sha256': 'c' * 64, 'binary_blake2b512': 'd' * 128}
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
