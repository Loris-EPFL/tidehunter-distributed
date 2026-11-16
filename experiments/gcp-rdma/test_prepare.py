import tempfile
import json
import io
import tarfile
import subprocess
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import sys
import gcp_sui

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

    def test_dirty_requires_explicit_manual_opt_in_and_exact_revision(self):
        revision = 'a' * 40
        with patch.object(prepare.cloud_package, 'command', side_effect=[revision, ' M Cargo.lock']):
            prepare.checkout(Path('.'), revision, allow_dirty=True)
        with patch.object(prepare.cloud_package, 'command', side_effect=['b' * 40, ' M Cargo.lock']):
            with self.assertRaises(ValueError):
                prepare.checkout(Path('.'), revision, allow_dirty=True)

    def test_manual_dirty_preparation_rejects_unreviewed_spec(self):
        with tempfile.TemporaryDirectory() as directory:
            spec = Path(directory) / 'spec.json'
            spec.write_text('{}')
            with patch.object(prepare, 'checkout'), patch.object(prepare.cloud_package, 'inventory', return_value={}):
                with self.assertRaisesRegex(ValueError, 'exact reviewed manual spec'):
                    prepare.prepare(prepare.ROOT, Path(directory) / 'out', 'a' * 40, None, 1800,
                                    spec, manual_allow_dirty=True)

    def test_revision_must_be_full_commit(self):
        with self.assertRaises(ValueError):
            prepare.checkout(Path('.'), 'abc')

    def test_selected_industry_features_are_required(self):
        backends = ['tikv', 'cassandra', 'foundationdb', 'garnet', 'kvrocks']
        spec = {'suite': 'kv', 'backends': backends}
        prepare.admit_selected_features(spec, {'features': backends})
        for features in ([], *([name for name in backends if name != missing] for missing in backends)):
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

    def test_tools_spec_enables_container_admission_build(self):
        import manual_campaign
        with tempfile.TemporaryDirectory() as directory:
            spec = Path(directory) / 'spec.json'
            spec.write_text(json.dumps(manual_campaign.REVIEWED_SPECS['nvmeof_f9_tools']))
            with patch.object(prepare, 'checkout'), \
                 patch.object(prepare.cloud_package, 'inventory', return_value={}), \
                 patch.object(prepare.cloud_package, 'snapshot', return_value={'sha256': {}}), \
                 patch.object(prepare, 'container_build', side_effect=RuntimeError('captured')) as build:
                with self.assertRaisesRegex(ValueError, 'requires --build-container'):
                    prepare.prepare(prepare.ROOT, Path(directory) / 'host', 'a' * 40, None, 1800, spec)
                with self.assertRaisesRegex(RuntimeError, 'captured'):
                    prepare.prepare(prepare.ROOT, Path(directory) / 'out', 'a' * 40, None, 1800,
                                    spec, build_container='builder')
                self.assertEqual(build.call_args.kwargs, {'focused': True, 'admission_options': True})

    def test_admission_flag_reaches_container_packager(self):
        import os
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            setup = root / '.github/scripts/setup.sh'
            setup.parent.mkdir(parents=True)
            setup.write_text(':\n')
            output = root / 'output'
            fake_bin = output / 'container-home/cargo/bin'
            fake_bin.mkdir(parents=True)
            python = fake_bin / 'python3'
            python.write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$CAPTURE"\n')
            python.chmod(0o755)
            capture = root / 'argv'
            env = {**os.environ, 'TIDEHUNTER_GCP_BUILD_CHILD': '1', 'CAPTURE': str(capture)}
            script = prepare.ROOT / 'experiments/gcp-rdma/build-container.sh'
            subprocess.run(['bash', str(script), str(root), str(output), '600', '0', '1', '1'],
                           env=env, check=True, timeout=10, capture_output=True)
            argv = capture.read_text().splitlines()
            self.assertIn('--focused', argv)
            self.assertIn('--admission-options', argv)
            self.assertEqual(argv[:2], [str(root / 'experiments/kv-comparison/cloud_package.py'), 'build'])
            with patch.object(prepare.cloud_package, 'command', return_value='sha256:' + 'a' * 64), \
                 patch.object(prepare.cloud_package, 'dump'), \
                 patch.object(prepare.ci_prepare, 'bounded') as bounded, \
                 patch.object(prepare, 'remove_build_container'):
                prepare.container_build(root, output, 'builder', 600, 9000, False,
                                        focused=True, admission_options=True)
            self.assertEqual(bounded.call_args.args[0][-2:], ['1', '1'])

    def test_rocky_abi_rejects_newer_runtime_symbols(self):
        abi = {'version_requirements': {'GLIBC': ['2.34'], 'GLIBCXX': ['3.4.29']}}
        prepare.admit_rocky9_abi(abi)
        for family, version in [('GLIBC', '2.35'), ('GLIBCXX', '3.4.30')]:
            bad = {'version_requirements': dict(abi['version_requirements'], **{family: [version]})}
            with self.subTest(family=family), self.assertRaisesRegex(ValueError, family):
                prepare.admit_rocky9_abi(bad)

class SuiNvmePreparationTest(unittest.TestCase):
    def test_prepare_command_defaults_and_nvme(self):
        revision = 'ef6701911cc07d9102f55e52f5a0632e5ddb0a86'
        for nvmeof in (False, True):
            with self.subTest(nvmeof=nvmeof), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                (root/'package-receipt.json').write_text(json.dumps({'source_revision': 'engine'}))
                ci = SimpleNamespace(unpack_fixture=lambda fixture, output: root/'fixture',
                    local_replay_transport=lambda: {}, write_local_replay_transport=lambda path: None)
                spec = SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None))
                captured = []
                def capture(command, deadline):
                    captured.append(command)
                    raise RuntimeError('stop after command')
                with patch.object(prepare, 'checkout'), patch.object(gcp_sui, 'inspect_archive'), \
                     patch.object(prepare.importlib.util, 'spec_from_file_location', return_value=spec), \
                     patch.object(prepare.importlib.util, 'module_from_spec', return_value=ci), \
                     patch.object(prepare.ci_prepare, 'bounded', side_effect=capture):
                    with self.assertRaisesRegex(RuntimeError, 'stop after command'):
                        prepare.prepare_sui(root, root, revision, root/'fixture.tar.gz', root/'target', 1800, nvmeof=nvmeof)
                expected = [sys.executable, prepare.ROOT/'experiments/sui-checkpoint/build.py', 'prepare',
                            '--sui', root, '--engine', prepare.ROOT, '--output', root/'sui/build',
                            '--fabric', '--jobs', '2']
                self.assertEqual(captured[0], expected + (['--nvmeof'] if nvmeof else []))

    def test_preparation_runs_each_contract_before_bundling(self):
        revision = 'ef6701911cc07d9102f55e52f5a0632e5ddb0a86'
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root/'package-receipt.json').write_text(json.dumps({'source_revision': 'engine'}))
            ci = SimpleNamespace(unpack_fixture=lambda fixture, output: root/'fixture',
                local_replay_transport=lambda: {}, write_local_replay_transport=lambda path: None)
            spec = SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None))
            captured = []
            def capture(command, deadline):
                captured.append(command)
                if len(captured) == 4:
                    raise RuntimeError('both replay commands captured')
            with patch.object(prepare, 'checkout'), patch.object(gcp_sui, 'inspect_archive'), \
                 patch.object(prepare.importlib.util, 'spec_from_file_location', return_value=spec), \
                 patch.object(prepare.importlib.util, 'module_from_spec', return_value=ci), \
                 patch.object(prepare.ci_prepare, 'bounded', side_effect=capture):
                with self.assertRaisesRegex(RuntimeError, 'both replay commands captured'):
                    prepare.prepare_sui(root, root, revision, root/'fixture.tar.gz', root/'target', 1800)
            self.assertEqual(captured[2][captured[2].index('--output')+1], root/'sui/replay')
            self.assertEqual(captured[3][captured[3].index('--output')+1], root/'sui/replay-original-default')
            self.assertEqual(captured[3][captured[3].index('--contract')+1], 'original_default')

    def test_static_config_gate_runs_before_packaging(self):
        revision = 'ef6701911cc07d9102f55e52f5a0632e5ddb0a86'
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root/'package-receipt.json').write_text(json.dumps({'source_revision': 'engine'}))
            ci = SimpleNamespace(unpack_fixture=lambda fixture, output: root/'fixture',
                local_replay_transport=lambda: {}, write_local_replay_transport=lambda path: None,
                verification_bundle=lambda replay, destination, transport: destination)
            spec = SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None))
            captured = []
            def capture(command, deadline):
                captured.append(command)
                if len(captured) == 5:
                    raise RuntimeError('static config gate captured')
            with patch.object(prepare, 'checkout'), patch.object(gcp_sui, 'inspect_archive'), \
                 patch.object(gcp_sui, 'verification_for_contract'), \
                 patch.object(prepare.importlib.util, 'spec_from_file_location', return_value=spec), \
                 patch.object(prepare.importlib.util, 'module_from_spec', return_value=ci), \
                 patch.object(prepare.ci_prepare, 'bounded', side_effect=capture):
                with self.assertRaisesRegex(RuntimeError, 'static config gate captured'):
                    prepare.prepare_sui(root, root, revision, root/'fixture.tar.gz', root/'target', 1800, nvmeof=True)
            gate = captured[4]
            self.assertEqual(gate[1].name, 'validate_deployment.py')
            self.assertEqual(gate[gate.index('--verification-root')+1], root/'sui')
            self.assertIn('--nvmeof', gate)
            self.assertFalse((root/'sui/sui-package.tar.gz').exists())

    def test_package_admission_requires_both_contract_schemas(self):
        from test_gcp_sui import write_verification
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            build_dir = root/'build'
            (build_dir/'binaries').mkdir(parents=True)
            with tarfile.open(build_dir/'sources.tar.gz', 'w:gz'):
                pass
            binaries = {}
            for name in gcp_sui.BINARIES:
                binary = build_dir/'binaries'/name
                binary.write_text(name)
                binaries[name] = gcp_sui.sha(binary)
            (build_dir/'build-manifest.json').write_text(json.dumps({'binaries': binaries}))
            with tarfile.open(root/'build-bundle.tar.gz', 'w:gz') as archive:
                for path in build_dir.rglob('*'):
                    if path.is_file():
                        archive.add(path, arcname=str(path.relative_to(build_dir)))
            bundle = root/'sui-package.tar.gz'
            with tarfile.open(bundle, 'w:gz') as archive:
                archive.add(root/'build-bundle.tar.gz', arcname='build-bundle.tar.gz')
            proofs = root/'proofs'
            write_verification(proofs, 'harness', 'mutexes: 1')
            verification = root/'verification.tar.gz'
            def archive_proofs():
                with tarfile.open(verification, 'w:gz') as archive:
                    for path in proofs.iterdir():
                        archive.add(path, arcname=path.name)
            archive_proofs()
            with self.assertRaisesRegex(ValueError, 'schema missing.*original_default'):
                prepare.admit_sui_archives(bundle, verification)
            write_verification(proofs, 'original_default', 'mutexes: 1024')
            archive_proofs()
            with patch.object(prepare.cloud_package, 'elf_identity', return_value={}) as identity, \
                 patch.object(prepare.cloud_package, 'abi_requirements', return_value={'abi': 'checked'}):
                self.assertEqual(prepare.admit_sui_archives(bundle, verification), {'abi': 'checked'})
                self.assertEqual(identity.call_count, len(binaries))


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
