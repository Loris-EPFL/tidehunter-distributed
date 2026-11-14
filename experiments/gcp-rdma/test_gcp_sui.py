import hashlib
from datetime import datetime, timezone
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import re
import uuid
from unittest.mock import patch
from types import SimpleNamespace

import gcp_sui


def put(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data) if isinstance(data, dict) else data)


def write_verification(root, contract, shape='shape'):
    directory = root / ('verification' if contract == 'harness' else 'verification-original-default')
    audit = {'all_equal': True}
    result = dict(contract=contract, complete=True, checkpoint_digest=gcp_sui.CHECKPOINT,
                  native={'completed_checkpoint_digest': gcp_sui.CHECKPOINT},
                  remote={'completed_checkpoint_digest': gcp_sui.CHECKPOINT}, audit=audit)
    put(directory/'experiment/results.json', result)
    put(directory/'experiment/audit.json', audit)
    put(directory/'experiment/native-schema/db/shape.yaml', shape)
    with tarfile.open(directory/'evidence.tar.gz', 'w:gz') as archive:
        archive.add(directory/'experiment', arcname='experiment')
    put(directory/'receipt.json', dict(complete=True, evidence_sha256=gcp_sui.sha(directory/'evidence.tar.gz')))
    return directory


class Store:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def download(self, key, path, *, generation, max_bytes):
        self.calls.append((key, generation))
        Path(path).write_bytes(self.data)


class SuiAdmissionTests(unittest.TestCase):
    def test_compact_suite_rejected_before_mailbox_access(self):
        with self.assertRaisesRegex(ValueError, 'Sui suite requires one or five owner hosts'):
            gcp_sui.run_suite({'fleet': [{}, {}, {}]}, None, None, None, output_root='/unused')

    def test_archive_preflight_and_extraction_share_symlink_safety(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / 'unsafe.tar.gz'
            with tarfile.open(archive, 'w:gz') as stream:
                link = tarfile.TarInfo('fixture/link')
                link.type = tarfile.SYMTYPE
                link.linkname = '../../escape'
                stream.addfile(link)
            with self.assertRaisesRegex(ValueError, 'symlink escapes'):
                gcp_sui.inspect_archive(archive)
            with self.assertRaisesRegex(ValueError, 'symlink escapes'):
                gcp_sui.extract(archive, root / 'unpack')

    def test_local_ssd_mount_is_required(self):
        run_id = 'bench-0123456789abcdef'
        with patch.object(Path, 'is_symlink', return_value=True), \
             patch.object(Path, 'resolve', return_value=Path('/dev/nvme0n1')), \
             patch.object(gcp_sui.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout='/dev/nvme0n1')) as findmnt:
            self.assertEqual(gcp_sui.storage_root(run_id), Path('/mnt/tidehunter-kv-comparison') / run_id)
            self.assertIn(str(Path('/mnt/tidehunter-kv-comparison') / run_id), findmnt.call_args.args[0])
        with patch.object(Path, 'is_symlink', return_value=True), \
             patch.object(gcp_sui.subprocess, 'run', return_value=SimpleNamespace(returncode=1, stdout='')):
            with self.assertRaisesRegex(ValueError, 'not mounted'):
                gcp_sui.storage_root(run_id)

    def test_owner_deadline_respects_fleet_and_supervisor_limits(self):
        now = 1_700_000_000
        stamp = lambda seconds: datetime.fromtimestamp(seconds, timezone.utc).isoformat()
        self.assertEqual(datetime.fromisoformat(gcp_sui.owner_deadline(stamp(now + 14400), now)).timestamp(), now + 7000)
        self.assertEqual(datetime.fromisoformat(gcp_sui.owner_deadline(stamp(now + 600), now)).timestamp(), now + 600)
        with self.assertRaises(TimeoutError):
            gcp_sui.owner_deadline(stamp(now + 60), now)

    def test_generation_and_hash_are_both_required(self):
        data = b'checkpoint archive bytes'
        store = Store(data)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'fixture'
            desc = dict(key='fixtures/real.tar.gz', generation='42', sha256=hashlib.sha256(data).hexdigest())
            gcp_sui.fetch(store, desc, path)
            self.assertEqual(store.calls, [('fixtures/real.tar.gz', '42')])
            with self.assertRaises(ValueError):
                gcp_sui.fetch(store, {**desc, 'sha256': '0' * 64}, path)
            with self.assertRaises(ValueError):
                gcp_sui.fetch(store, {**desc, 'generation': None}, path)

    def test_unsupported_transports_are_explicitly_rejected(self):
        self.assertEqual(gcp_sui.selected_transports({}), ['tcp'])
        self.assertEqual(gcp_sui.selected_transports({'sui_transports': ['tcp', 'tidehunter_verbs']}), ['tcp', 'tidehunter_verbs'])
        self.assertEqual(gcp_sui.selected_transports({'sui_transports': ['tidehunter_nvme_rdma']}), ['tidehunter_nvme_rdma'])
        for backend in ('nvme-tcp', 'nvme-rdma', 'efa', 'fabric-tcp', 'fabric-tcp-native'):
            with self.assertRaisesRegex(ValueError, 'unsupported Sui transport'):
                gcp_sui.selected_transports({'sui_transports': [backend]})

    def test_cell_settings_and_identity(self):
        settings = gcp_sui.sui_cell_settings({})
        self.assertEqual(settings, dict(sui_contract='harness', sui_owner_tier=None, sui_owner_wal='harness',
            sui_live_export_mode='per_owner', sui_live_queue_geometry=dict(io_queues=1, queue_size=32, poll_queues=0)))
        for key, value in [('sui_contract', 'bad'), ('sui_owner_tier', 'bad'), ('sui_owner_wal', 'bad'),
                           ('sui_live_export_mode', 'bad'), ('sui_live_queue_geometry', {'io_queues': True})]:
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'invalid Sui cell setting: ' + key):
                gcp_sui.sui_cell_settings({key: value})
        self.assertIsNotNone(re.fullmatch(gcp_sui.CELL_PATTERN, 'r02-tidehunter_nvme_rdma'))
        hosts = [{'rdma_ip': '10.2.0.1'}]*5
        identity = dict(run_id='bench-0123456789abcdef', cell='r02-tidehunter_nvme_rdma')
        live = gcp_sui.live_nvme_config(hosts, settings, **identity)
        self.assertEqual(live, gcp_sui.live_nvme_config(hosts, settings, **identity))
        self.assertNotEqual(live['host_id'], gcp_sui.live_nvme_config(
            hosts, settings, **dict(identity, cell='r04-tidehunter_nvme_rdma'))['host_id'])
        self.assertEqual(live['addresses'], ['10.2.0.1']*5)
        self.assertEqual(live['host_id'], list(uuid.UUID(live['host_nqn'].rsplit(':', 1)[1]).bytes))

    def test_explicit_cells(self):
        default = dict(transport='tcp', contract='harness', owner_tier=None, owner_wal='harness')
        self.assertEqual(gcp_sui.sui_cells({}), [default])
        cells = [dict(default, transport='tidehunter_nvme_rdma'),
                 dict(default, transport='tidehunter_nvme_rdma', contract='original_default',
                      owner_tier='native', owner_wal='engine_default')]
        self.assertEqual(gcp_sui.sui_cells({'sui_cells': cells}), cells)
        for key in ('sui_transports', 'sui_contract', 'sui_owner_tier', 'sui_owner_wal'):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'sui_cells replaces'):
                gcp_sui.sui_cells({'sui_cells': cells, key: None})
        for bad in ([], [default]*5, [{}], [dict(default, transport='bad')],
                    [dict(default, contract='bad')], [dict(default, owner_tier='bad')],
                    [dict(default, owner_wal='bad')], [dict(default, extra=1)]):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, 'invalid Sui cell setting: sui_cells'):
                gcp_sui.sui_cells({'sui_cells': bad})

    def test_revision_mismatch_fails_before_build_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            put(root / 'sources.tar.gz', 'source')
            put(root / 'source-manifest.json', {'sui': {'revision': 'sui-sha'}, 'tidehunter-sharded-wal': {'revision': 'different'}})
            plan = {'protocol': 15, 'source_archive_sha256': gcp_sui.sha(root / 'sources.tar.gz'),
                    'source_manifest_sha256': gcp_sui.sha(root / 'source-manifest.json'), 'fabric': False}
            put(root / 'plan.json', plan)
            put(root / 'build-manifest.json', {**plan, 'binaries': dict.fromkeys(gcp_sui.BINARIES, '0' * 64)})
            with self.assertRaisesRegex(ValueError, 'revision differs'):
                gcp_sui.admit_build(root, {'sui_revision': 'sui-sha'}, 'expected-engine')

    def test_local_audit_is_bound_to_fixture_and_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture, verification, build = root / 'fixture', root / 'verification', root / 'build'
            put(fixture / 'producer/state', 'real producer')
            prior = {'source_frozen': True, 'failure': None, 'checkpoint_limit': gcp_sui.TIP,
                     'checkpoint_digest': gcp_sui.CHECKPOINT, 'verified_source_range': {'start': 0, 'end': gcp_sui.TIP}}
            put(fixture / 'experiment/results.json', prior)
            expected = {str(p.relative_to(fixture)): gcp_sui.sha(p) for p in fixture.rglob('*') if p.is_file()}
            put(fixture / 'fixture-manifest.json', {'files': expected, 'checkpoint_limit': gcp_sui.TIP,
                'checkpoint_digest': gcp_sui.CHECKPOINT, 'source_range': prior['verified_source_range']})
            put(build / 'build-manifest.json', {'build': 'identity'})
            manifest = {'source_archive_sha256': 'a' * 64, 'binaries': {'node': 'b' * 64}}
            audit = {'all_equal': True, 'databases': {'a': {}}, 'rocksdb': {'a': {}}}
            result = {'complete': True, 'build_manifest_sha256': gcp_sui.sha(build / 'build-manifest.json'),
                'source_archive_sha256': manifest['source_archive_sha256'], 'binaries': manifest['binaries'],
                'checkpoint_digest': gcp_sui.CHECKPOINT, 'checkpoint_limit': gcp_sui.TIP,
                'source_range': prior['verified_source_range'], 'topology': 'loopback', 'audit': audit,
                'fixture_files': expected, 'native': {'completed_checkpoint_digest': gcp_sui.CHECKPOINT},
                'remote': {'completed_checkpoint_digest': gcp_sui.CHECKPOINT}}
            result.update(native_retried=True, native_attempts=[
                {'attempt': 1, 'outcome': 'rpc_index_catchup_timeout'}, {'attempt': 2, 'outcome': 'ok'}])
            put(verification / 'experiment/results.json', result)
            put(verification / 'experiment/audit.json', audit)
            put(verification / 'experiment/native-schema/db/shape.yaml', 'schema')
            for name in ('native.log', 'native-samples.jsonl', 'native-foreground.prom', 'native-final.prom',
                         'native-schema/db/shape.yaml', 'catchup.json'):
                put(verification / ('experiment/native-attempt1-' + name), 'retained failed attempt')
            with tarfile.open(verification / 'evidence.tar.gz', 'w:gz') as tar:
                tar.add(verification / 'experiment', arcname='experiment')
            put(verification / 'receipt.json', {'complete': True, 'evidence_sha256': gcp_sui.sha(verification / 'evidence.tar.gz'),
                'build_manifest_sha256': result['build_manifest_sha256'], 'source_archive_sha256': manifest['source_archive_sha256']})
            gcp_sui.admit_fixture(fixture, verification, manifest, build)
            put(fixture / 'producer/state', 'altered')
            with self.assertRaisesRegex(ValueError, 'fixture content differs'):
                gcp_sui.admit_fixture(fixture, verification, manifest, build)
            put(fixture / 'producer/state', 'real producer')
            audit['all_equal'] = False
            put(verification / 'experiment/audit.json', audit)
            with self.assertRaisesRegex(ValueError, 'audit is incomplete'):
                gcp_sui.admit_fixture(fixture, verification, manifest, build)

    def test_contract_schema_selection_has_no_harness_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            harness = write_verification(root, 'harness', 'mutexes: 1')
            self.assertEqual(gcp_sui.verification_for_contract(root, 'harness'), harness)
            with self.assertRaisesRegex(ValueError, 'schema missing.*original_default'):
                gcp_sui.verification_for_contract(root, 'original_default')
            original = write_verification(root, 'original_default', 'mutexes: 1024')
            self.assertEqual(gcp_sui.verification_for_contract(root, 'original_default'), original)
            put(original/'experiment/native-schema/db/shape.yaml', 'mutexes: 1')
            with self.assertRaisesRegex(ValueError, 'schema is not bound'):
                gcp_sui.verification_for_contract(root, 'original_default')

    def test_original_default_rejects_harness_replay_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            original = write_verification(root, 'original_default')
            result = gcp_sui.load(original/'experiment/results.json')
            result['contract'] = 'harness'
            put(original/'experiment/results.json', result)
            with self.assertRaisesRegex(ValueError, 'differs.*original_default'):
                gcp_sui.verification_for_contract(root, 'original_default')

    def test_remote_failure_does_not_qualify(self):
        with self.assertRaises(RuntimeError):
            gcp_sui.check_receipt({'exit_code': 1, 'stderr': 'audit failed'})




class ReviewedCompactSuiCells(unittest.TestCase):
    def test_four_reviewed_cells_use_compact_owner_addresses(self):
        import manual_sui
        spec = manual_sui.REVIEWED_SUI_SPECS['sui_nvmeof_compact']
        cells = gcp_sui.sui_cells(spec)
        self.assertEqual(len(cells), 4)
        self.assertEqual(cells, spec['sui_cells'])
        settings = gcp_sui.sui_cell_settings(spec)
        live = gcp_sui.live_nvme_config([{'rdma_ip': '10.2.0.2'}] * 5, settings,
            run_id='bench-0123456789abcdef', cell='r02-tidehunter_nvme_rdma')
        self.assertEqual(live['addresses'], ['10.2.0.2'] * 5)
        self.assertEqual(live['export_mode'], 'per_owner')

    def test_four_cells_complete_with_one_owner_mailbox(self, remote_failure=False):
        import copy
        from unittest.mock import Mock
        import manual_sui
        spec = copy.deepcopy(manual_sui.REVIEWED_SUI_SPECS['sui_nvmeof_compact'])
        desc = dict(key='object', generation='1', sha256='a'*64)
        spec['sui_verification'] = desc
        manifest = dict(fabric=True, nvmeof=True, source_archive_sha256='a'*64,
                        binaries={'storage-node': 'a'*64})
        node_data = json.dumps(dict(receipt={'complete': True}, logs=desc, resources={})).encode()
        collected = dict(key='resources', generation='1', sha256=hashlib.sha256(node_data).hexdigest())
        store = Mock()
        store.upload.return_value = {'generation': '1'}
        store.get.return_value = node_data
        mailbox = Mock(store=store)
        def submit(index, argv, **kwargs):
            self.assertEqual(index, 1)
            return dict(index=index, action=argv[3])
        def wait(ref, **kwargs):
            return dict(exit_code=0, stdout=json.dumps(collected) if ref['action'] == 'collect' else '{}')
        mailbox.submit.side_effect = submit
        mailbox.wait.side_effect = wait
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root/'data'
            data.mkdir()
            owner = dict(internal_ip='10.0.0.2', rdma_ip='10.1.0.2')
            config = dict(fleet=[dict(internal_ip='10.0.0.1', rdma_ip='10.1.0.1'), owner], spec=spec,
                package_root=str(root/'package'), run_id='bench-test', project_id='project', bucket='bucket',
                deadline_utc='2030-01-01T00:00:00Z')
            package = dict(sui_object_key='build', sui_generation='1', sui_sha256='a'*64)
            def extract(archive, target):
                target = Path(target)
                target.mkdir(parents=True, exist_ok=True)
                if target.name == 'fixture':
                    (target/'fixture').mkdir()
                    (target/'fixture/distributed.yaml').write_text('db-path: distributed/db\n')
                if target.name == 'verification':
                    for contract in ('harness', 'original_default'):
                        write_verification(target, contract)
            def load(path):
                path = Path(path)
                name = path.name
                if name == 'build-receipt.json':
                    return dict(bundle_sha256='a'*64, build_manifest_sha256='a'*64, source_archive_sha256='a'*64)
                if name == 'manifest.json':
                    return {'revision': 'b'*40}
                if name == 'source.json':
                    return {'sha256': {}}
                if name == 'source-manifest.json':
                    return {'tidehunter-sharded-wal': {'files': {}}}
                if name == 'external-manifest.json':
                    return dict(config_sha256='a'*64, build_manifest_sha256='a'*64, storage_binary_sha256='a'*64,
                        hosts=[dict(host=owner['internal_ip'], partitions=list(range(5)), archive='payload',
                                    archive_sha256='a'*64, commands=[], ready_files=[], stop_files=[])])
                if name in ('remote-starting.json', 'remote-finished.json', 'resources-required.json'):
                    return dict(config_sha256='a'*64, build_manifest_sha256='a'*64,
                                checkpoint_digest=gcp_sui.CHECKPOINT, stage=path.stem)
                if name == 'results.json':
                    return dict(complete=True, audit={'all_equal': True}, binaries=manifest['binaries'],
                                checkpoint_digest=gcp_sui.CHECKPOINT)
                if name == 'receipt.json':
                    return dict(complete=True, evidence_sha256='a'*64)
                raise AssertionError(str(path))
            def popen(argv, **kwargs):
                if remote_failure:
                    child = Mock()
                    child.poll.return_value = 1
                    return child
                stage = Path(argv[argv.index('--external-stage-dir')+1])
                (stage/'resources-required.json').write_text('{}')
                child = Mock()
                child.poll.return_value = None
                def finish(**kwargs):
                    child.poll.return_value = 0
                    return 0
                child.wait.side_effect = finish
                return child
            with patch.object(gcp_sui, 'storage_root', return_value=data), \
                 patch.object(gcp_sui, 'fetch') as fetch, patch.object(gcp_sui, 'extract', side_effect=extract), \
                 patch.object(gcp_sui, 'load', side_effect=load), patch.object(gcp_sui, 'sha', return_value='a'*64), \
                 patch.object(gcp_sui, 'admit_build', return_value=manifest), patch.object(gcp_sui, 'admit_fixture'), \
                 patch.object(gcp_sui, 'command') as command, patch.object(gcp_sui.shutil, 'copy2'), \
                 patch.object(gcp_sui.subprocess, 'Popen', side_effect=popen), \
                 patch.object(gcp_sui, 'verification_for_contract',
                     side_effect=lambda root, contract: root/('verification' if contract == 'harness' else 'verification-original-default')):
                if remote_failure:
                    with self.assertRaisesRegex(RuntimeError, 'run.py exited with status 1 before resources-required; last stage: none; see replay.log'):
                        gcp_sui.run_suite(config, mailbox, package, desc, output_root=root/'results')
                    self.assertTrue((root/'results/r01-tidehunter_verbs_native/owner-1.json').is_file())
                    self.assertTrue(any(call.args[1][3] == 'collect' for call in mailbox.submit.call_args_list))
                    self.assertTrue(any(Path(call.args[2]).name == 'owner-1-logs.tar.gz'
                                        for call in fetch.call_args_list))
                    return
                result = gcp_sui.run_suite(config, mailbox, package, desc, output_root=root/'results')
                prepares = [call.args[0] for call in command.call_args_list if str(call.args[0][1]).endswith('prepare_external.py')]
                self.assertEqual(len(prepares), 4)
                for argv, cell in zip(prepares, spec['sui_cells']):
                    schema = Path(argv[argv.index('--schema')+1])
                    self.assertEqual(schema.parts[-3],
                        'verification' if cell['contract'] == 'harness' else 'verification-original-default')
            self.assertTrue(result['complete'])
            self.assertEqual([{k: c[k] for k in ('transport','contract','owner_tier','owner_wal')}
                              for c in result['cells']], spec['sui_cells'])
            self.assertEqual(mailbox.submit.call_count, 16)

    def test_failed_replay_retains_owner_logs(self):
        self.test_four_cells_complete_with_one_owner_mailbox(remote_failure=True)


class KernelSnapshot(unittest.TestCase):
    def test_snapshot_keeps_controller_and_target_state_and_never_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = root/'sys/nvme7'
            controller.mkdir(parents=True)
            (controller/'state').write_text('connecting\n')
            (controller/'subsysnqn').write_text('nqn.test:wal\n')
            target = root/'nvmet/subsystems/nqn.test:wal/allowed_hosts'
            target.mkdir(parents=True)
            (target.parent/'attr_allow_any_host').write_text('0\n')
            with patch.object(gcp_sui.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout='nvme nvme7: Property Set error: 880\n', stderr='')):
                gcp_sui.kernel_snapshot(root/'out', 'at-failure', sysfs=root/'sys', configfs=root/'nvmet')
            rows = json.loads((root/'out/kernel-at-failure.json').read_text())
            self.assertEqual(rows['controllers']['nvme7']['state'], 'connecting')
            self.assertEqual(rows['controllers']['nvme7']['subsysnqn'], 'nqn.test:wal')
            self.assertIn('unreadable', rows['controllers']['nvme7']['hostnqn'])
            self.assertEqual(rows['target']['subsystems/nqn.test:wal/attr_allow_any_host'], '0')
            self.assertIn('Property Set error', (root/'out/dmesg-at-failure.txt').read_text())
            with patch.object(gcp_sui.subprocess, 'run', side_effect=OSError('no dmesg')):
                gcp_sui.kernel_snapshot(root/'out', 'second', sysfs=root/'sys', configfs=root/'nvmet')
            self.assertFalse((root/'out/kernel-second.json').exists())


if __name__ == '__main__':
    unittest.main()
