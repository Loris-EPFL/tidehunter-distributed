"""Pure contract tests; these do not simulate or claim NVMe hardware coverage."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from types import SimpleNamespace

import campaign_targets as campaign


class CampaignTargetsTests(unittest.TestCase):
    def test_private_fleet_address(self):
        for address in ('10.0.0.1', '172.16.2.3', '192.168.1.9'):
            self.assertEqual(campaign.private_address(address), address)
        for address in ('127.0.0.1', '172.32.0.1', '169.254.1.1', '8.8.8.8', '::1'):
            with self.subTest(address=address), self.assertRaises(ValueError):
                campaign.private_address(address)

    def test_paths_are_absolute_and_canonical(self):
        self.assertEqual(str(campaign.absolute('/root/run')), '/root/run')
        for path in ('relative/run', '/root/../run', '/root//run', '/root/run/'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                campaign.absolute(path)

    def test_host_identity_is_stable_and_per_export(self):
        run = str(uuid.uuid4())
        first = campaign.host_identity(run, str(uuid.uuid4()))
        second = campaign.host_identity(run, str(uuid.uuid4()))
        self.assertNotEqual(first[0], second[0])
        self.assertNotEqual(first[1], second[1])
        self.assertEqual(first[1], str(uuid.uuid5(uuid.NAMESPACE_URL, first[0])))
        ident = str(uuid.uuid4())
        self.assertEqual(campaign.host_identity(run, ident), campaign.host_identity(run, ident))
        self.assertNotEqual(campaign.host_identity(run, ident), campaign.host_identity(str(uuid.uuid4()), ident))

    def test_connected_receipt_contains_reader_bindings_only(self):
        entry = {'export_uuid': list(range(16)), 'device': '/dev/nvme2n1'}
        state = {'kind': 'campaign-host', 'phase': 'connected', 'transport': 'tcp', 'exports': [entry]}
        self.assertEqual(campaign.receipt(state)['bindings'], [
            {'export_uuid': list(range(16)), 'namespace_path': '/dev/nvme2n1', 'transport': 'tcp'}])
        state['transport'] = 'rdma'
        self.assertEqual(campaign.receipt(state)['bindings'][0]['transport'], 'rdma')
        state['phase'] = 'partial'
        self.assertNotIn('bindings', campaign.receipt(state))

    def test_cleanup_proof_requires_exact_disconnected_campaign(self):
        entry = {'uuid': 'u', 'nqn': 'n', 'hostnqn': 'h', 'hostid': 'i',
                 'address': '10.0.0.1', 'service': 4420}
        target = {'campaign_id': 'run', 'exports': [entry]}
        proof = {'version': 1, 'kind': 'campaign-host', 'campaign_id': 'run',
                 'phase': 'disconnected', 'readers_drained': True,
                 'exports': [dict(entry, status='disconnected')]}
        campaign.validate_disconnect_proof(target, proof)
        for change in ({'campaign_id': 'other'}, {'readers_drained': False},
                       {'phase': 'connected'}, {'exports': []},
                       {'exports': [dict(entry, status='connected')]},
                       {'exports': [dict(entry, status='disconnected', hostid='other')]}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                campaign.validate_disconnect_proof(target, dict(proof, **change))

    @unittest.skipUnless(os.geteuid() == 0 and Path('/run').is_dir(),
                         'requires root and a trusted /run directory')
    def test_large_catalog_uses_raw_hash_not_receipt_content(self):
        with tempfile.TemporaryDirectory(prefix='nvmeof-large-state-', dir='/run') as root:
            root = Path(root)
            exports = root / 'exports'
            manifest_dir = exports / '0'
            manifest_dir.mkdir(parents=True)
            path = manifest_dir / 'manifest.json'
            manifest = {'version': 1, 'nqn': 'nqn.2026-10.org.tidehunter:test',
                        'nsid': 1, 'image_len': 4096, 'block_size': 4096,
                        'export_uuid': list(range(16)),
                        'catalog': [{'record': 'x' * 520}] * 65536}
            with path.open('w') as stream:
                json.dump(manifest, stream, separators=(',', ':'))
            self.assertGreater(path.stat().st_size, 32 * 1024 * 1024)
            ready = root / 'ready.json'
            ready.write_text(json.dumps({'exports': [dict(
                manifest_path=str(path), **{k: manifest[k] for k in
                ('export_uuid', 'nqn', 'nsid', 'image_len', 'block_size')})]}))
            with path.open('rb') as stream:
                expected = hashlib.file_digest(stream, 'sha256').hexdigest()
            entries = campaign.load_exports(str(ready), str(exports))
            self.assertEqual(entries[0]['manifest_sha256'], expected)
            self.assertNotIn('manifest_data', entries[0])
            with path.open('a') as stream:
                stream.write('\n')
            changed = campaign.load_exports(str(ready), str(exports))
            self.assertNotEqual(entries[0]['manifest_sha256'], changed[0]['manifest_sha256'])

    @unittest.skipUnless(os.geteuid() == 0 and Path('/run').is_dir(),
                         'requires root and a trusted /run directory')
    def test_real_state_directory_lifecycle(self):
        with tempfile.TemporaryDirectory(prefix='nvmeof-state-test-', dir='/run') as root:
            state_path = Path(root) / 'campaign'
            state = {'version': 1, 'kind': 'campaign-host', 'phase': 'connecting', 'exports': []}
            directory = campaign.new_campaign_state(state_path, state)
            child_path = state_path / 'export-000'
            child = campaign.manage.new_state(child_path, {'version': 1, 'kind': 'host', 'phase': 'disconnected'})
            child.close()
            child.close()
            directory.close()
            directory.close()
            directory, loaded = campaign.campaign_state(state_path, 'campaign-host')
            self.assertEqual(loaded, state)
            child, loaded_child = campaign.manage.load_state(child_path, 'host')
            self.assertEqual(loaded_child['phase'], 'disconnected')
            child.close()
            directory.close()

    @unittest.skipUnless(os.geteuid() == 0 and Path('/run').is_dir(),
                         'requires root and a trusted /run directory')
    def test_empty_owner_lifecycle(self):
        with tempfile.TemporaryDirectory(prefix='nvmeof-empty-test-', dir='/run') as root:
            root = Path(root)
            exports = root / 'exports'
            exports.mkdir()
            ready = root / 'ready.json'
            ready.write_text(json.dumps({'exports': []}))
            target = campaign.setup(SimpleNamespace(ready=str(ready), export_root=str(exports),
                state=str(root / 'target'), address='10.0.0.1', base_port_id=65535, base_service=65535))
            self.assertEqual(target['phase'], 'published')
            self.assertEqual(target['exports'], [])
            receipt = root / 'target.json'
            receipt.write_text(json.dumps(target))
            host = campaign.connect(SimpleNamespace(ready=str(ready), export_root=str(exports),
                state=str(root / 'host'), targets=str(receipt)))
            self.assertEqual(host['phase'], 'connected')
            self.assertEqual(host['bindings'], [])
            proof = campaign.disconnect(SimpleNamespace(state=str(root / 'host'), readers_drained=True))
            self.assertEqual(proof['phase'], 'disconnected')
            self.assertEqual(proof['exports'], [])
            proof_path = root / 'proof.json'
            proof_path.write_text(json.dumps(proof))
            cleaned = campaign.cleanup(SimpleNamespace(state=str(root / 'target'),
                readers_drained=True, host_disconnected=str(proof_path)))
            self.assertEqual(cleaned['phase'], 'cleaned')
            self.assertEqual(cleaned['exports'], [])


if __name__ == '__main__':
    unittest.main()
