"""Credential-free CI admission, qualification and artifact transport regressions."""
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('kv_ci_prepare', Path(__file__).with_name('ci_prepare.py'))
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.inputs = self.root / 'inputs'
        self.inputs.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, name, value):
        path = self.inputs / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value) if not isinstance(value, str) else value)
        return path

    def test_current_clean_commit_and_trusted_branch_are_required(self):
        repo = self.root / 'repo'
        repo.mkdir()
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        (repo / 'source.py').write_text('# committed\n')
        subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Preparation Test', '-c',
                        'user.email=prepare@example.invalid', 'commit', '-qm', 'fixture'], check=True)
        commit = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
        environment = dict(GITHUB_REPOSITORY='Loris-EPFL/tidehunter-distributed', GITHUB_REF='refs/heads/bench/aws-efa',
                           GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_SHA=commit)
        with patch.dict(os.environ, environment, clear=True):
            self.assertEqual(ci.trusted_checkout(repo), commit)
            with self.assertRaises(ValueError):
                ci.trusted_checkout(repo, 'a' * 40)
            with patch.dict(os.environ, {'GITHUB_REF': 'refs/heads/main'}), self.assertRaises(ValueError):
                ci.trusted_checkout(repo)
            (repo / 'source.py').write_text('# changed\n')
            with self.assertRaisesRegex(ValueError, 'clean committed'):
                ci.trusted_checkout(repo)

    def proof(self):
        ready = dict(engine_sha='a' * 40, binary_sha256='b' * 64)
        for backend in ci.BACKENDS:
            prefix = 'qualification/' + backend
            artifacts = {}
            for name in ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom'):
                artifacts[name] = ci.sha(self.write(prefix + '/result/' + name, 'actual retained ' + name))
            self.write(prefix + '/normalized-config.json', {'qualification_only': True, 'source_revision': ready['engine_sha']})
            self.write(prefix + '/receipt.json', dict(job={'backend': backend}, status='completed',
                       binary_sha256=ready['binary_sha256'], claim_boundary='API qualification only', artifacts_sha256=artifacts))
        ready['qualification_files'] = ci.proof_inventory(self.inputs / 'qualification')
        return ready

    def test_all_real_software_adapter_receipts_and_raw_hashes_are_required(self):
        ready = self.proof()
        ci.admit_qualification(self.inputs / 'qualification', ready)
        receipt = self.inputs / 'qualification/tikv/receipt.json'
        data = json.loads(receipt.read_text())
        for changes in ({'status': 'failed'}, {'binary_sha256': 'c' * 64}, {'cleanup_errors': ['not stopped']},
                        {'artifacts_sha256': {}}, {'claim_boundary': 'timing'}):
            receipt.write_text(json.dumps(data | changes))
            ready['qualification_files'] = ci.proof_inventory(self.inputs / 'qualification')
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                ci.admit_qualification(self.inputs / 'qualification', ready)
        receipt.write_text(json.dumps(data))
        ready['qualification_files'] = ci.proof_inventory(self.inputs / 'qualification')
        (self.inputs / 'qualification/tikv/result/summary.json').write_text('changed raw bytes')
        with self.assertRaises(ValueError):
            ci.admit_qualification(self.inputs / 'qualification', ready)

    def test_source_and_binary_bindings_are_independently_checked(self):
        bundle = self.write('bundle.tar.gz', 'sealed runtime')
        ready = dict(schema_version=1, complete=True, suite='kv-comparison', engine_sha='a' * 40,
                     source_commit='a' * 40, qualification_complete=True, bundle_sha256=ci.sha(bundle),
                     source_sha256='source', binary_sha256='binary', source_tree_sha256='tree', manifest_sha256='manifest')
        self.write('READY.json', ready)
        source = dict(revision='a' * 40, dirty=False, sha256={'source.py': 'digest'}, tree_sha256='tree')
        verified = dict(source_sha256='source', binary_sha256='binary', manifest_sha256='manifest')
        with patch.object(ci, 'trusted_checkout', return_value='a' * 40), \
                patch.object(ci.cloud_package, 'verify_directory', return_value=verified), \
                patch.object(ci.cloud_package, 'admit_source', return_value=source), \
                patch.object(ci.cloud_package, 'inventory', return_value={'source.py': 'digest'}), \
                patch.object(ci, 'admit_qualification'):
            self.assertEqual(ci.admit_inputs(self.inputs), ready)
            for changes in ({'source_commit': 'b' * 40}, {'binary_sha256': 'other'}, {'suite': 'sui-transport'},
                            {'qualification_complete': False}, {'complete': False}):
                self.write('READY.json', ready | changes)
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    ci.admit_inputs(self.inputs)
            self.write('READY.json', ready)
            source['dirty'] = True
            with self.assertRaises(ValueError):
                ci.admit_inputs(self.inputs)
            source['dirty'] = False
            source['sha256'] = {'source.py': 'changed committed source'}
            with self.assertRaises(ValueError):
                ci.admit_inputs(self.inputs)

    def test_archive_roundtrip_keeps_modes_and_excludes_build_databases(self):
        ready = self.proof()
        bundle = self.write('bundle.tar.gz', 'sealed runtime')
        self.write('READY.json', ready | {'bundle_sha256': ci.sha(bundle)})
        executable = self.write('qualification/native/helper.sh', '#!/bin/sh\nexit 0\n')
        executable.chmod(0o750)
        self.write('build/database/never-export', 'database')
        archive = self.root / 'prepared.tar.gz'
        restored = self.root / 'restored'
        with patch.object(ci, 'admit_inputs', return_value=ready), patch.object(ci, 'trusted_checkout'), \
                patch.object(ci.cloud_package, 'verify_archive') as verify:
            result = ci.package_inputs(self.inputs, archive)
            ci.restore_inputs(archive, result['archive_sha256'], restored)
        self.assertEqual((restored / 'qualification/native/helper.sh').stat().st_mode & 0o777, 0o750)
        self.assertFalse((restored / 'build').exists())
        verify.assert_called_once_with(restored / 'bundle.tar.gz', ci.sha(bundle), restored / 'package')
        with patch.object(ci, 'trusted_checkout'), self.assertRaises(ValueError):
            ci.restore_inputs(archive, '0' * 64, self.root / 'wrong-hash')

    def test_unsafe_links_duplicates_roots_modes_and_expansion_fail_before_writes(self):
        for kind in ('traversal', 'link', 'duplicate', 'root', 'setuid', 'oversize'):
            archive = self.root / (kind + '.tar.gz')
            with tarfile.open(archive, 'w:gz') as stream:
                member = tarfile.TarInfo('../escape' if kind == 'traversal' else ('unexpected' if kind == 'root' else 'READY.json'))
                member.size = 2
                if kind == 'link':
                    member.type, member.linkname, member.size = tarfile.SYMTYPE, '/outside', 0
                if kind == 'setuid':
                    member.mode = 0o4755
                stream.addfile(member, io.BytesIO(b'{}'))
                if kind == 'duplicate':
                    stream.addfile(member, io.BytesIO(b'{}'))
            output = self.root / (kind + '-restored')
            with patch.object(ci, 'trusted_checkout'), patch.object(ci, 'MAX_EXPANDED', 1 if kind == 'oversize' else 1 << 30):
                with self.subTest(kind=kind), self.assertRaises(ValueError):
                    ci.restore_inputs(archive, ci.sha(archive), output)
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
