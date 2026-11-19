"""Same-build API qualification evidence admission."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

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



if __name__ == '__main__':
    unittest.main()
