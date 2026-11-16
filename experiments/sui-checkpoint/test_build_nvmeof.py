import os
import shutil
import json
import tempfile
import tomllib
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import build


# Captured feature sections from the Sui typed-store and sui-node manifests.
# Staging must work on a clean checkout without a developer's sibling Sui tree.
SUI_FEATURE_MANIFESTS = {
    'crates/typed-store/Cargo.toml': '''[features]
testing = []
distributed-tidehunter-bulk = []
distributed-tidehunter-durability = ["testing"]
distributed-tidehunter-session = ["testing"]
sharded-wal = ["tidehunter", "distributed-tidehunter-bulk"]
sharded-wal-fabric = ["sharded-wal", "tidehunter/experimental-fabric"]
''',
    'crates/sui-node/Cargo.toml': '''[features]
default = ["jemalloc"]
jemalloc = ["tikv-jemallocator"]
testing = []
distributed-tidehunter-session = ["typed-store/distributed-tidehunter-session"]
sharded-wal = ["typed-store/sharded-wal", "distributed-tidehunter-session"]
sharded-wal-fabric = ["sharded-wal", "typed-store/sharded-wal-fabric"]
''',
}


class NvmeStagingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.sources = Path(self.tmp.name) / 'sources'
        self.paths = list(SUI_FEATURE_MANIFESTS)
        for name, manifest in SUI_FEATURE_MANIFESTS.items():
            target = self.sources / 'sui' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(manifest)
        engine = self.sources / 'tidehunter-sharded-wal/tidehunter/Cargo.toml'
        engine.parent.mkdir(parents=True)
        engine.write_text('[features]\nexperimental-nvmeof = []\n')
        self.identities = {'sui': {'files': {}}}

    def test_add_and_repeat(self):
        build.add_staged_nvmeof_features(self.sources, self.identities)
        before = {}
        for relative in self.paths:
            path = self.sources / 'sui' / relative
            before[relative] = path.read_bytes()
            self.assertEqual(before[relative].count(b'sharded-wal-nvmeof = '), 1)
            self.assertEqual(self.identities['sui']['files'][relative],
                             {'sha256': build.digest(path), 'mode': path.stat().st_mode & 0o777})
        build.add_staged_nvmeof_features(self.sources, self.identities)
        self.assertEqual(before, {name: (self.sources / 'sui' / name).read_bytes() for name in self.paths})

    def test_invalid_manifests(self):
        typed = self.sources / 'sui' / self.paths[0]
        original = typed.read_text()
        typed.write_text(original.replace('sharded-wal-fabric = ', 'changed = ', 1))
        with self.assertRaisesRegex(ValueError, 'anchor'):
            build.add_staged_nvmeof_features(self.sources, self.identities)
        typed.write_text(original.replace('sharded-wal-fabric = ', 'sharded-wal-nvmeof = []\nsharded-wal-fabric = ', 1))
        with self.assertRaisesRegex(ValueError, 'feature differs'):
            build.add_staged_nvmeof_features(self.sources, self.identities)
        typed.write_text(original)
        (self.sources / 'tidehunter-sharded-wal/tidehunter/Cargo.toml').write_text('[features]\n')
        with self.assertRaisesRegex(ValueError, 'staged engine has no NVMe-oF feature'):
            build.add_staged_nvmeof_features(self.sources, self.identities)

    def test_feature_selection_and_fabric_gate(self):
        self.assertEqual(build.metadata_features('sui', True, True)[1], 'sui-node/sharded-wal-nvmeof')
        self.assertIn('tidehunter/experimental-nvmeof', build.metadata_features('tidehunter-sharded-wal', True, True))
        self.assertEqual(build.metadata_features('audit', True, True), ['nvmeof'])
        self.assertEqual(build.metadata_features('sui', True, False)[1], 'sui-node/sharded-wal-fabric')
        self.assertEqual(build.metadata_features('audit', False, False), [])
        with self.assertRaisesRegex(ValueError, 'NVMe-oF build requires the fabric build'):
            build.prepare(SimpleNamespace(nvmeof=True, fabric=False))

    def test_plan_records_feature_selection(self):
        engine = self.sources / 'tidehunter-sharded-wal'
        audit = engine / 'experiments/sui-checkpoint/audit'
        audit.mkdir(parents=True)
        for workspace in (self.sources/'sui', engine, audit):
            (workspace/'Cargo.lock').write_text('')
        for nvmeof in (False, True):
            output = Path(self.tmp.name) / f'out-{nvmeof}'
            def snapshot(source, target):
                shutil.copytree(source, target)
                return {'files': {}}
            def metadata(command, *, cwd, stdout, stderr, timeout):
                stdout.write('{}')
            with patch.object(build, 'snapshot', side_effect=snapshot), \
                 patch.object(build, 'verify_staged'), patch.object(build, 'bind_staged_engine', return_value={}), \
                 patch.object(build, 'verify_engine_metadata'), patch.object(build, 'bounded', side_effect=metadata), \
                 patch.object(build, 'staged_protocol', return_value=1):
                build.prepare(SimpleNamespace(sui=self.sources/'sui', engine=engine, output=output,
                                              fabric=True, nvmeof=nvmeof, jobs=2))
            plan = json.loads((output/'plan.json').read_text())
            self.assertEqual(plan['nvmeof'], nvmeof)
            self.assertEqual(plan['remote_features'], ['sui-node/sharded-wal-' + ('nvmeof' if nvmeof else 'fabric')])


@unittest.skipUnless(os.environ.get('SUI_CHECKOUT'),
                     'set SUI_CHECKOUT to check upstream Sui feature drift')
class SuiFeatureDriftTests(unittest.TestCase):
    def test_real_sui_features_match_captured_contract(self):
        checkout = Path(os.environ['SUI_CHECKOUT'])
        for relative, manifest in SUI_FEATURE_MANIFESTS.items():
            with self.subTest(manifest=relative):
                expected = tomllib.loads(manifest)['features']
                text = (checkout / relative).read_text()
                actual = tomllib.loads(text)['features']
                # Staging requires this exact line, not only equivalent TOML.
                anchor = next(line for line in manifest.splitlines()
                              if line.startswith('sharded-wal-fabric = '))
                self.assertEqual(text.splitlines().count(anchor), 1)
                for name in ('sharded-wal', 'sharded-wal-fabric'):
                    self.assertEqual(actual[name], expected[name])
