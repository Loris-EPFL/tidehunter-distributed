import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import gcp_image_prepare as prepare


class OfflinePayloadTests(unittest.TestCase):
    def test_requires_explicit_activation_before_touching_manifest(self):
        with self.assertRaisesRegex(ValueError, 'explicit --activate'):
            prepare.install(Path('/nonexistent'), '0' * 64, False)

    def test_payload_must_match_pinned_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'driver.rpm').write_bytes(b'payload')
            entry = {'path': 'driver.rpm', 'sha256': hashlib.sha256(b'payload').hexdigest()}
            self.assertEqual(prepare.payload(root, entry), root / 'driver.rpm')
            entry['sha256'] = '0' * 64
            with self.assertRaisesRegex(ValueError, 'digest mismatch'):
                prepare.payload(root, entry)

    def test_payload_cannot_escape_manifest_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / 'bundle'
            child.mkdir()
            (root / 'outside').write_bytes(b'x')
            entry = {'path': '../outside', 'sha256': hashlib.sha256(b'x').hexdigest()}
            with self.assertRaisesRegex(ValueError, 'payload path'):
                prepare.payload(child, entry)

    def test_hpc_manifest_accepts_reuse_only_when_inventory_exact(self):
        image = 'example/repo:v1@sha256:' + 'a' * 64
        manifest = {'schema_version': 2, 'mode': 'hpc-rocky9',
                    'kernel_release': prepare.os.uname().release, 'rpm_packages': [],
                    'repositories': [], 'images': [{'reference': image}]}
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(prepare.gcp_image, 'required_containers', return_value={image}):
            result = prepare._hpc_inputs(Path(directory) / 'manifest.json', manifest)
            self.assertEqual(result[0], [])
            self.assertEqual(result[3], [image])
            manifest['images'] = []
            with self.assertRaisesRegex(ValueError, 'inventory'):
                prepare._hpc_inputs(Path(directory) / 'manifest.json', manifest)

    def test_hpc_manifest_rejects_kernel_and_driver_packages(self):
        base = {'schema_version': 2, 'mode': 'hpc-rocky9',
                'kernel_release': prepare.os.uname().release, 'repositories': [], 'images': []}
        for name in ('kernel-0:5.14.0-1.el9.x86_64', 'kmod-idpf-irdma-0:1-1.el9.x86_64'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'upgrades are forbidden'):
                prepare._hpc_inputs(Path('/tmp/manifest.json'), {**base, 'rpm_packages': [name]})

    def test_hpc_repositories_bind_official_origin_and_signing_fingerprint(self):
        fingerprint = '21CB256AE16FC54C6E652949702D426D350D275D'
        image = 'example/repo:v1@sha256:' + 'a' * 64
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'key').write_bytes(b'public-key')
            manifest = {'schema_version': 2, 'mode': 'hpc-rocky9',
                        'kernel_release': prepare.os.uname().release,
                        'rpm_packages': ['python3.11-0:3.11.13-10.el9_8.x86_64'],
                        'signing_keys': [{'fingerprint': fingerprint, 'payload': {
                            'path': 'key', 'sha256': hashlib.sha256(b'public-key').hexdigest()}}],
                        'repositories': [{'id': 'baseos', 'baseurl': 'https://dl.rockylinux.org/pub/rocky/9/BaseOS/x86_64/os/', 'fingerprint': fingerprint}],
                        'images': [{'reference': image}]}
            with patch.object(prepare, 'run', return_value='fpr:::::::::' + fingerprint + ':'), \
                 patch.object(prepare.gcp_image, 'required_containers', return_value={image}):
                prepare._hpc_inputs(root / 'manifest.json', manifest)
                manifest['repositories'][0]['baseurl'] = 'https://attacker.example/repo'
                with self.assertRaisesRegex(ValueError, 'official HTTPS'):
                    prepare._hpc_inputs(root / 'manifest.json', manifest)
                manifest['repositories'][0]['baseurl'] = 'https://dl.rockylinux.org/pub/rocky/9/BaseOS/x86_64/os/\ngpgcheck=0'
                with self.assertRaisesRegex(ValueError, 'official HTTPS'):
                    prepare._hpc_inputs(root / 'manifest.json', manifest)

    def test_kabi_exception_requires_loaded_matching_weak_module(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {}
            for name in ('irdma', 'idpf'):
                original = root / 'extra' / (name + '.ko')
                original.parent.mkdir(exist_ok=True)
                original.write_bytes(b'module')
                link = root / 'weak-updates' / (name + '.ko')
                link.parent.mkdir(exist_ok=True)
                link.symlink_to(original)
                running = root / 'sys' / name
                running.mkdir(parents=True)
                (running / 'srcversion').write_text('ABC123\n')
                files[name] = link
            def fake_path(value):
                return root / 'sys' if str(value) == '/sys/module' else Path(value)
            def fake_run(argv, timeout=300):
                if argv[:3] == ['modinfo', '-F', 'vermagic']:
                    return 'old-kernel SMP'
                if argv[:2] == ['modinfo', '-n']:
                    return str(files[argv[2]])
                if argv[:3] == ['modinfo', '-F', 'srcversion']:
                    return 'ABC123'
                if argv[:2] == ['rpm', '-qf']:
                    return 'kmod-idpf-irdma-base-1'
                raise AssertionError(argv)
            with patch.object(prepare, 'Path', side_effect=fake_path), patch.object(prepare, 'run', side_effect=fake_run):
                with self.assertRaisesRegex(ValueError, 'weak-update'):
                    prepare.inspect_modules('new-kernel')
                self.assertEqual(set(prepare.inspect_modules('new-kernel', allow_loaded_kabi=True)), {'irdma', 'idpf'})
                (root / 'sys' / 'irdma' / 'srcversion').write_text('DIFFERENT\n')
                with self.assertRaisesRegex(ValueError, 'weak-update'):
                    prepare.inspect_modules('new-kernel', allow_loaded_kabi=True)

    def test_hpc_requires_explicit_activation_and_network(self):
        with self.assertRaisesRegex(ValueError, 'explicit --activate'):
            prepare.install_hpc(Path('/nonexistent'), '0' * 64, False, False, False, False, False)
        with patch.object(prepare.os, 'geteuid', return_value=0), \
             self.assertRaisesRegex(ValueError, 'allow-registry-pull'):
            prepare.install_hpc(Path('/nonexistent'), '0' * 64, True, False, False, False, False)


if __name__ == '__main__':
    unittest.main()
