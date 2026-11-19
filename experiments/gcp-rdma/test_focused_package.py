import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import prepare

package = prepare.cloud_package
ci_prepare = prepare.ci_prepare


class FocusedPackageTest(unittest.TestCase):
    def test_focused_valkey_container_propagates_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(package, 'command', return_value='sha256:' + 'a' * 64), \
                    patch.object(ci_prepare, 'bounded') as bounded, \
                    patch.object(prepare, 'remove_build_container'):
                prepare.container_build(root, root, 'builder', 3600, 100000, True,
                                        focused_valkey=True)
            self.assertEqual(bounded.call_args.args[0][-2:], ['1', '2'])

    def test_focused_valkey_cli_propagates_profile(self):
        argv = ['cloud_package.py', 'build', '--snapshot', 'snapshot', '--output', 'build',
                '--fabric-prefix', 'fabric', '--valkey-source', 'valkey', '--focused-valkey']
        with patch('sys.argv', argv), patch.object(package, 'build', return_value={}) as build, \
                patch('builtins.print'):
            package.main()
        self.assertTrue(build.call_args.kwargs['focused_valkey'])
        self.assertFalse(build.call_args.kwargs['focused'])
        self.assertIsNone(build.call_args.args[2])

    def test_focused_valkey_requires_exact_features_and_staged_artifacts(self):
        spec = {'suite': 'kv', 'mode': 'focused', 'selection': 'valkey_rdma_reads',
                'backends': ['valkey_rdma']}
        files = dict.fromkeys(('runtime/bin/valkey-server', 'runtime/bin/valkey-cli',
                               'runtime/bin/valkey-benchmark', 'runtime/valkey-rdma.json',
                               'source/experiments/kv-comparison/nvme_campaign_owner.py'), {})
        manifest = {'build_profile': 'focused_valkey',
                    'features': package.FOCUSED_VALKEY_FEATURES, 'files': files}
        prepare.admit_selected_features(spec, manifest)
        for change in ({'build_profile': 'focused'}, {'features': package.FEATURES + ['valkey-rdma']},
                       {'features': package.FOCUSED_FEATURES}, {'files': {}}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                prepare.admit_selected_features(spec, {**manifest, **change})

    def test_focused_valkey_build_receipt_binds_profile_and_dependency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'kv-research').write_bytes(b'binary')
            (root / 'build.log').write_bytes(b'build')
            binary = {'identity': package.identity(root / 'kv-research'), 'needed': ['libfabric.so.1']}
            source = {'snapshot_sha256': 'a' * 64, 'tree_sha256': 'b' * 64}
            valkey = {'source_commit': package.VALKEY_COMMIT}
            receipt = {'schema_version': 1, 'source_sha256': source['snapshot_sha256'],
                       'source_tree_sha256': source['tree_sha256'],
                       'features': package.FOCUSED_VALKEY_FEATURES, 'valkey_rdma': valkey,
                       'target': package.TARGET, 'cpu': 'x86-64', 'runtime_efa': package.EFA,
                       'binary': binary, 'fdb': None, 'build_log': package.identity(root / 'build.log'),
                       'build_profile': 'focused_valkey',
                       'build_command': ['cargo', '--no-default-features', '--features',
                                         ','.join(package.FOCUSED_VALKEY_FEATURES)]}
            with patch.object(package, 'admit_source', return_value=source), \
                    patch.object(package, 'elf_identity', return_value=binary), \
                    patch.object(package, 'valkey_source_identity', return_value=valkey):
                (root / 'build.json').write_text(json.dumps(receipt))
                package.admit_build(root, root, None, root, focused_valkey=True)
                for change in ({'features': package.FEATURES + ['valkey-rdma']},
                               {'build_profile': 'focused'}, {'fdb': {}}, {'valkey_rdma': None}):
                    (root / 'build.json').write_text(json.dumps({**receipt, **change}))
                    with self.subTest(change=change), self.assertRaises(ValueError):
                        package.admit_build(root, root, None, root, focused_valkey=True)
                with self.assertRaises(ValueError):
                    package.admit_build(root, root, None, focused_valkey=True)

    def test_focused_build_receipt_rejects_other_features_or_fdb(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'kv-research').write_bytes(b'binary')
            (root / 'build.log').write_bytes(b'build')
            binary = {'identity': package.identity(root / 'kv-research'), 'needed': ['libfabric.so.1']}
            source = {'snapshot_sha256': 'a' * 64, 'tree_sha256': 'b' * 64}
            receipt = {'schema_version': 1, 'source_sha256': source['snapshot_sha256'],
                       'source_tree_sha256': source['tree_sha256'], 'features': package.FOCUSED_FEATURES,
                       'valkey_rdma': None, 'target': package.TARGET, 'cpu': 'x86-64', 'runtime_efa': package.EFA,
                       'binary': binary, 'fdb': None, 'build_log': package.identity(root / 'build.log'),
                       'build_profile': 'focused',
                       'build_command': ['cargo', '--no-default-features', '--features', ','.join(package.FOCUSED_FEATURES)]}
            with patch.object(package, 'admit_source', return_value=source), \
                    patch.object(package, 'elf_identity', return_value=binary):
                (root / 'build.json').write_text(json.dumps(receipt))
                package.admit_build(root, root, None, focused=True)
                for change in ({'features': package.FEATURES}, {'fdb': {'sha256': 'a' * 64}},
                               {'build_command': ['cargo', '--features', 'foundationdb']}):
                    (root / 'build.json').write_text(json.dumps({**receipt, **change}))
                    with self.assertRaises(ValueError):
                        package.admit_build(root, root, None, focused=True)

    def test_focused_manifest_binds_features_elf_and_no_fdb_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('source.zip', 'source.json', 'build.log', 'runtime/bin/kv-research'):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(name.encode())
            binary = root / 'runtime/bin/kv-research'
            binary.chmod(0o755)
            families = {name: [] for name in ('GLIBC', 'GLIBCXX', 'CXXABI', 'OPENSSL')}
            def elf(path, *args, **kwargs):
                return {'identity': package.identity(path), 'needed': ['libfabric.so.1'],
                        'version_requirements': families, 'version_definitions': families,
                        'soname': None, 'rpaths': []}
            source = {'snapshot_sha256': 'a' * 64, 'tree_sha256': 'b' * 64,
                      'revision': 'c' * 40, 'branch': 'main', 'dirty': False,
                      'sha256': dict.fromkeys(package.FOCUSED_SOURCE_CLOSURE, 'd' * 64)}
            build = {'schema_version': 1, 'source_sha256': source['snapshot_sha256'],
                     'source_tree_sha256': source['tree_sha256'], 'features': package.FOCUSED_FEATURES,
                     'target': package.TARGET, 'cpu': 'x86-64', 'runtime_efa': package.EFA,
                     'binary': elf(binary), 'fdb': None, 'valkey_rdma': None,
                     'build_profile': 'focused',
                     'build_command': ['cargo', '--no-default-features', '--features', ','.join(package.FOCUSED_FEATURES)]}
            (root / 'build.json').write_text(json.dumps(build))
            files = {p.relative_to(root).as_posix(): package.identity(p) for p in root.rglob('*') if p.is_file()}
            manifest = {'schema_version': 1, 'revision': source['revision'], 'branch': source['branch'],
                        'dirty': False, 'source_sha256': source['snapshot_sha256'],
                        'source_tree_sha256': source['tree_sha256'], 'binary_sha256': package.identity(binary)['sha256'],
                        'binary_blake2b512': package.identity(binary)['blake2b512'],
                        'features': package.FOCUSED_FEATURES, 'build_profile': 'focused', 'runtime_efa': package.EFA,
                        'paths': {'repository_root': 'source', 'binary': 'runtime/bin/kv-research',
                                  'fdb_library_dir': 'runtime/lib'}, 'files': files}
            (root / 'manifest.json').write_text(json.dumps(manifest))
            linkage = {'resolved': {'/usr/lib/libfabric.so.1': {}}, 'ldd': 'libfabric.so.1'}
            with patch.object(package, 'admit_source', return_value=source), \
                    patch.object(package, 'elf_identity', side_effect=elf), \
                    patch.object(package, 'linked_libraries', return_value=linkage), \
                    patch.object(package, 'command', return_value='fi_info 2.6.0'), \
                    patch.object(package, 'fdb_client_version', side_effect=AssertionError('FDB probe')):
                proof = package.verify_directory(root, runtime=True, provider='gcp')
                self.assertEqual(proof['build_profile'], 'focused')
                self.assertNotIn('fdb_client_version', proof)
                altered = copy.deepcopy(manifest)
                altered['features'] = package.FEATURES
                (root / 'manifest.json').write_text(json.dumps(altered))
                with self.assertRaisesRegex(ValueError, 'schema/features'):
                    package.verify_directory(root)
                (root / 'manifest.json').write_text(json.dumps(manifest))
                def fdb_elf(path, *args, **kwargs):
                    return {**elf(path), 'needed': ['libfabric.so.1', 'libfdb_c.so']}
                with patch.object(package, 'elf_identity', side_effect=fdb_elf):
                    with self.assertRaisesRegex(ValueError, 'ELF dependencies'):
                        package.verify_directory(root)
                with patch.object(package, 'linked_libraries', return_value={'resolved': {'/lib/libfdb_c.so': {}}, 'ldd': 'fdb'}):
                    with self.assertRaisesRegex(ValueError, 'loaded FoundationDB'):
                        package.verify_directory(root, runtime=True, provider='gcp')

    def test_focused_qualification_requires_exact_same_binary_native_adapter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for backend in ('native',):
                case = root / backend
                (case / 'result').mkdir(parents=True)
                (case / 'receipt.json').write_text(json.dumps({'job': {'backend': backend}, 'status': 'completed',
                    'binary_sha256': 'a' * 64, 'claim_boundary': 'API qualification only',
                    'artifacts_sha256': {name: 'digest' for name in ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom')}}))
                (case / 'normalized-config.json').write_text(json.dumps({'qualification_only': True, 'source_revision': 'b' * 40}))
            ready = {'binary_sha256': 'a' * 64, 'engine_sha': 'b' * 40, 'qualification_files': {}}
            with patch.object(ci_prepare, 'proof_inventory', return_value={}), \
                    patch.object(ci_prepare, 'sha', return_value='digest'):
                ci_prepare.admit_qualification(root, ready, focused=True)
                with self.assertRaisesRegex(ValueError, 'every software-admissible'):
                    ci_prepare.admit_qualification(root, ready)
                extra = root / 'foundationdb'
                extra.mkdir()
                (extra / 'receipt.json').write_text(json.dumps({'job': {'backend': 'foundationdb'}}))
                with self.assertRaisesRegex(ValueError, 'same-binary'):
                    ci_prepare.admit_qualification(root, ready, focused=True)
