"""Static external-config gate coverage; no target or cloud access."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import yaml

import build
import validate_deployment


class DeploymentValidationTest(unittest.TestCase):
    def fixture(self, root):
        binary = root / 'build/binaries/storage-node'
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b'fixture binary; subprocess is intercepted')
        (root/'build/build-manifest.json').write_text(json.dumps(
            {'binaries': {'storage-node': build.digest(binary)}}))
        for proof in ('verification', 'verification-original-default'):
            for name in ('live/checkpoints', 'live/store/epoch_0', 'live/store/perpetual'):
                directory = root/proof/'experiment/native-schema'/name
                directory.mkdir(parents=True)
                (directory/'manifest.json').write_text(json.dumps({'namespace': name, 'backend': 'native'}))
                (directory/'shape.json').write_text('{}')
                (directory/'shape.yaml').write_text('fixture-shape')
        return binary

    def test_both_contracts_and_topologies_pass_real_generated_files_to_binary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = self.fixture(root)
            with patch.object(validate_deployment.subprocess, 'run') as run:
                receipt = validate_deployment.validate(root/'build', root, root/'gate', nvmeof=True)
            self.assertTrue(receipt['complete'])
            self.assertEqual(len(receipt['cells']), 16)
            self.assertEqual(run.call_count, 16)
            self.assertEqual({(cell['contract'], cell['owner_wal']) for cell in receipt['cells']},
                             {(contract, wal) for contract in ('harness', 'original_default')
                              for wal in ('harness', 'engine_default')})
            for call in run.call_args_list:
                command = call.args[0]
                self.assertEqual(command[:2], [str(binary), '--experimental-validate-config'])
                self.assertEqual(len(command[3:]), 15)
                self.assertTrue(call.kwargs['check'])
                self.assertEqual(call.kwargs['timeout'], 30)
                config = json.loads(Path(command[2]).read_text())
                self.assertEqual(len(config['databases']), 3)
                self.assertTrue(all(Path(path).is_file() for path in command[3:]))
            self.assertFalse((root/'gate/guest').exists())
            self.assertFalse((root/'gate/remote').exists())

    def test_admission_rejects_cross_owner_collisions_and_identity_mismatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root)
            with patch.object(validate_deployment.subprocess, 'run'):
                validate_deployment.validate(root/'build', root, root/'gate', nvmeof=True)
            directory = root/'gate/harness-engine_default-2vm-nvmeof'
            config_path = directory/'config.json'
            config_text = config_path.read_text()
            nodes = json.loads((directory/'nodes.json').read_text())
            first_path, second_path = (Path(node['launch']) for node in nodes[:2])
            first = yaml.safe_load(first_path.read_text())
            second_text = second_path.read_text()
            for field in ('root', 'listen', 'state_dir', 'port_id',
                          'commissioning_port_id', 'service', 'commissioning_service', 'host_nqn'):
                with self.subTest(field=field):
                    second = yaml.safe_load(second_text)
                    if field == 'listen':
                        second[field] = first[field]
                    elif field == 'root':
                        second['node'][field] = first['node'][field]
                    else:
                        second['node']['live_target'][field] = first['node']['live_target'][field]
                    second_path.write_text(yaml.safe_dump(second))
                    with self.assertRaises(ValueError):
                        validate_deployment.validate_owner_resources(directory)
                    second_path.write_text(second_text)
            # Match a shared NQN on both sides: this must fail for collision,
            # not just for an owner/coordinator mismatch.
            config = json.loads(config_text)
            second = yaml.safe_load(second_text)
            shared = first['node']['live_target']['host_nqn']
            second['node']['live_target']['host_nqn'] = shared
            db = config['databases'][nodes[1]['namespace']]
            db['live_nvme']['owner_identities'][str(nodes[1]['partition'])]['host_nqn'] = shared
            config_path.write_text(json.dumps(config))
            second_path.write_text(yaml.safe_dump(second))
            with self.assertRaisesRegex(ValueError, 'duplicate owner host NQN'):
                validate_deployment.validate_owner_resources(directory)

    def test_rust_validator_failure_prevents_complete_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root)
            with patch.object(validate_deployment.subprocess, 'run',
                              side_effect=subprocess.CalledProcessError(1, 'storage-node')):
                with self.assertRaises(subprocess.CalledProcessError):
                    validate_deployment.validate(root/'build', root, root/'gate', nvmeof=True)
            self.assertFalse((root/'gate/receipt.json').exists())


if __name__ == '__main__':
    unittest.main()
