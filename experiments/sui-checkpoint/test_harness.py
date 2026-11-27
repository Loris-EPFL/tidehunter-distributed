import json
import copy
import hashlib
import io
import os
import signal
import shutil
import subprocess
import sys
import tarfile
import time
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
import tomllib
from unittest.mock import patch
import deployment
import analyze
import build
import run


class SoftwareFabricGeometry(unittest.TestCase):
    def test_excessive_or_invalid_software_receive_geometry_is_rejected(self):
        for value in ('', '4096', '0'):
            for transport in deployment.SOFTWARE_TRANSPORTS:
                with self.subTest(value=value, transport=transport):
                    with self.assertRaises(ValueError):
                        deployment.process_environment(
                            {'FI_OFI_RXM_MSG_RX_SIZE': value}, transport=transport)


class NativeProfileContract(unittest.TestCase):
    def test_capability_remains_available_with_zero_sampling_budget(self):
        clean = dict(native_profile_limit=0, native_profile_available=1,
                     native_profile_barrier_calls=0, native_profile_barrier_errors=0,
                     native_profile_file_calls=0, native_profile_file_successes=0,
                     native_profile_namespace_calls=0, native_profile_namespace_successes=0)
        run.validate_native_profiles({'live/checkpoints': clean}, 0)
        for changed in ({'native_profile_available': 0},
                        {'native_profile_limit': 1}, {'native_profile_barrier_calls': 1}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                run.validate_native_profiles({'live/checkpoints': clean | changed}, 0)

    def test_sampled_barriers_require_complete_successful_persistence(self):
        sampled = dict(native_profile_limit=8, native_profile_available=1,
                       native_profile_barrier_calls=3, native_profile_barrier_errors=0,
                       native_profile_file_calls=6, native_profile_file_successes=6,
                       native_profile_namespace_calls=3, native_profile_namespace_successes=3)
        run.validate_native_profiles({'live/checkpoints': sampled}, 8)
        for changed in ({'native_profile_barrier_errors': 1},
                        {'native_profile_file_successes': 5},
                        {'native_profile_namespace_calls': 2}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                run.validate_native_profiles({'live/checkpoints': sampled | changed}, 8)


class DurabilityTierContract(unittest.TestCase):
    def test_external_config_must_state_the_selected_tier(self):
        config = {'databases': {'db': {'live_nvme': {'leased_durability': 'native'}}}}
        self.assertIsNone(run.external_durability_error(config, 'native'))
        self.assertIn("states 'native'", run.external_durability_error(config, 'synchronous'))
        config['databases']['db']['live_nvme'] = {}
        self.assertIsNone(run.external_durability_error(config, 'synchronous'))
        with self.assertRaises(ValueError):
            run.external_durability_error(config, 'eventual')


class DeploymentContract(unittest.TestCase):
    def test_each_namespace_gets_the_requested_physical_nodes(self):
        for owners in (2, 5):
            with self.subTest(owners=owners), tempfile.TemporaryDirectory() as tmp, \
                    patch.object(deployment,'certificates',lambda path,owners:path.mkdir()):
                root=Path(tmp)
                schema={'live/perpetual':{'key_spaces':[]},'live/checkpoints':{'key_spaces':[]}}
                config,nodes=deployment.prepare(root/'deployment',schema,
                    root/'remote',shape_yamls={name:'key_spaces: []\n' for name in schema},
                    hosts=['127.0.0.1']*owners,owners=owners,port=24000,transport='tcp',
                    coordinator_cache=0,shard_cache=0)
                self.assertEqual(len(nodes),owners*len(schema))
                self.assertEqual(len(set(node['launch'] for node in nodes)),owners*len(schema))
                identities=[json.dumps(db['live_nvme']['owner_identities'],sort_keys=True)
                            for db in config['databases'].values()]
                self.assertNotEqual(*identities)
                for db in config['databases'].values():
                    self.assertEqual(db['storage']['partitions'],list(range(owners)))
                    self.assertNotIn('database',db['storage'])
                    self.assertEqual(len(db['endpoints']),owners)
                    self.assertEqual([row['partition'] for row in db['endpoints']],list(range(owners)))
                for database_number, namespace in enumerate(schema):
                    for node in (row for row in nodes if row['namespace'] == namespace):
                        pool = node['live_target_pool']
                        self.assertEqual(
                            pool['public_service_base'],
                            4420 + deployment.LIVE_TARGET_WINDOW * node['partition']
                            + deployment.LIVE_TARGET_STRIDE * database_number)

    def test_two_owner_host_inventory_is_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'deployment'
            with self.assertRaisesRegex(ValueError, 'exactly 2'):
                deployment.prepare(root, {'db': {}}, Path(tmp) / 'remote',
                    shape_yamls={'db': 'key_spaces: []\n'},
                    hosts=['10.0.0.1', '10.0.0.2', '10.0.0.3'], owners=2,
                    port=24000, transport='tcp', coordinator_cache=0, shard_cache=0)
            self.assertFalse(root.exists())

    def test_external_client_identity_stays_local(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(deployment,'certificates',lambda path,owners:path.mkdir()):
            root=Path(tmp)
            config,nodes=deployment.prepare(root/'deployment',{'live/perpetual':{'key_spaces':[]}},root/'remote',
                shape_yamls={'live/perpetual':'key_spaces: []\n'},hosts=['10.0.0.'+str(i) for i in range(1,6)],port=24000,transport='efa',
                coordinator_cache=8388608,shard_cache=8388608,guest_root=Path('/owned/remote'))
            self.assertTrue(config['databases']['live/perpetual']['tls']['key'].startswith(str(root)))
            self.assertTrue(nodes[0]['ready'].startswith('/owned/remote'))

    def test_invalid_schema_namespace_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'manifest.json').write_text(json.dumps({'namespace':'../escape'}))
            (root/'shape.json').write_text('{}')
            with self.assertRaises(ValueError):
                deployment.schemas(root)

    def test_remote_schema_rejects_native_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'manifest.json').write_text(json.dumps({'namespace':'live/perpetual','backend':'native'}))
            (root/'shape.json').write_text('{}')
            with self.assertRaises(ValueError):
                deployment.schemas(root,'sharded-wal')

    def test_legacy_per_shard_adapter_counters_are_rejected(self):
        for name in ('shard_sample_available', 'shard_cache_hits', 'resident_sample_available',
                     'authority_available', 'initiator_available'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'legacy per-shard'):
                analyze.adapter_stats({
                    f'tidehunter_adapter{{namespace="live/perpetual",stat="{name}"}}': 1})

    def test_tcp_queue_absence_is_not_zero_latency(self):
        values = {'read_profile_enabled':1, 'read_profile_transport_progress_available':1,
                  'read_profile_transport_cq_available':0, 'read_profile_transport_cq_call_ns':0,
                  'read_profile_transport_output_retire_ns':0}
        stats = analyze.adapter_stats({
            f'tidehunter_adapter{{namespace="live/perpetual",stat="{name}"}}':value
            for name,value in values.items()})['live/perpetual']
        self.assertIsNone(stats['read_profile_transport_cq_call_ns'])
        self.assertEqual(stats['read_profile_transport_output_retire_ns'], 0)

    def test_absent_schema_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                deployment.schemas(Path(tmp))

    def test_hardware_transport_cannot_run_loopback(self):
        for transport in ('efa', 'efa-direct', 'efa-native'):
            with self.subTest(transport=transport), tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(ValueError):
                    deployment.prepare(Path(tmp)/'x',{},Path(tmp),shape_yamls={},hosts=['127.0.0.1']*5,
                        port=24000,transport=transport,coordinator_cache=0,shard_cache=0)

    def test_software_fabric_rejects_nonloopback_hosts(self):
        for transport in ('fabric-tcp', 'fabric-tcp-native'):
            with self.subTest(transport=transport), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / 'deployment'
                with self.assertRaisesRegex(ValueError, 'loopback-only'):
                    deployment.prepare(root, {}, Path(tmp), shape_yamls={},
                        hosts=['127.0.0.1'] * 4 + ['10.0.0.1'], port=24000,
                        transport=transport, coordinator_cache=0, shard_cache=0)
                self.assertFalse(root.exists())

    def test_invalid_write_profile_budget_fails_before_deployment(self):
        for limit in (-1, 65537, True, 1.0, None):
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / 'deployment'
                with self.assertRaisesRegex(ValueError, 'write profile limit'):
                    deployment.prepare(root, {}, Path(tmp), shape_yamls={},
                        hosts=['127.0.0.1'] * 5, port=24000, transport='tcp',
                        coordinator_cache=0, shard_cache=0, write_profile_limit=limit)
                self.assertFalse(root.exists())

    def test_registered_reads_cannot_bypass_encrypted_transport_policy(self):
        resident = dict(source_bytes=8192, source_regions=4, destination_bytes=8192,
                        destination_slots=2, max_read_bytes=4096, max_handles=4,
                        require_device_read=False)
        for transport in ('tcp', 'fabric-tcp', 'efa', 'efa-direct'):
            with self.subTest(transport=transport), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / 'deployment'
                hardware = transport in deployment.HARDWARE_TRANSPORTS
                with self.assertRaises(ValueError):
                    deployment.prepare(root, {'db': {}}, Path(tmp) / 'remote',
                        shape_yamls={'db': 'key_spaces: []\n'},
                        hosts=['10.0.0.' + str(i) for i in range(1, 6)] if hardware else ['127.0.0.1'] * 5,
                        port=24000, transport=transport, coordinator_cache=0, shard_cache=0,
                        guest_root=Path('/owned') if hardware else None, resident_reads=resident)
                self.assertFalse(root.exists())

    def test_registered_destination_budgets_include_every_namespace(self):
        for destination_bytes, destination_slots in ((128 * deployment.MIB, 64),
                                                     (64 * deployment.MIB, 4096)):
            with self.subTest(bytes=destination_bytes, slots=destination_slots), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / 'deployment'
                schema = {f'db-{i}': {} for i in range(3)}
                resident = dict(source_bytes=8192, source_regions=4,
                    destination_bytes=destination_bytes, destination_slots=destination_slots,
                    max_read_bytes=4096, max_handles=4, require_device_read=False)
                with self.assertRaises(ValueError):
                    deployment.prepare(root, schema, Path(tmp) / 'remote',
                        shape_yamls={name: 'key_spaces: []\n' for name in schema},
                        hosts=['127.0.0.1'] * 5, port=24000, transport='fabric-tcp-native',
                        coordinator_cache=0, shard_cache=0, resident_reads=resident)
                self.assertFalse(root.exists())

    def test_registered_read_limit_must_fit_each_destination_not_just_total_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'deployment'
            resident = dict(source_bytes=8192, source_regions=4, destination_bytes=8192,
                            destination_slots=2, max_read_bytes=4097, max_handles=4,
                            require_device_read=False)
            with self.assertRaises(ValueError):
                deployment.prepare(root, {'db': {}}, Path(tmp) / 'remote',
                    shape_yamls={'db': 'key_spaces: []\n'}, hosts=['127.0.0.1'] * 5,
                    port=24000, transport='fabric-tcp-native', coordinator_cache=0,
                    shard_cache=0, resident_reads=resident)
            self.assertFalse(root.exists())

    def test_cache_only_reads_do_not_require_invented_rpc_samples(self):
        databases = {'db': dict(tls={'transport': 'tcp'}, endpoints=[{}])}
        stats = dict(read_rpcs=0, read_profile_selected=0, read_profile_completed=0,
                     read_profile_errors=0,
                     write_profile_expected_connections=1, write_profile_captured_connections=0,
                     write_profile_sampled_requests=0, write_profile_available=0)
        run.validate_profiles({'db': stats}, databases, 4096, 0)
        with self.assertRaisesRegex(ValueError, 'zero read-RPC population'):
            run.validate_profiles({'db': stats | {'read_profile_selected': 1}}, databases, 4096, 0)
        databases['db']['tls']['transport'] = 'fabric-tcp'
        rpc_stats = stats | dict(read_rpcs=1, read_profile_selected=1,
                                 read_profile_completed=1)
        run.validate_profiles({'db': rpc_stats}, databases, 4096, 0)
        with self.assertRaisesRegex(ValueError, 'bounded read profile'):
            run.validate_profiles({'db': rpc_stats | {'read_profile_completed': 0}},
                                  databases, 4096, 0)

    def test_write_profiles_reject_partial_errors_and_clean_population_contamination(self):
        databases = {'db': dict(tls={'transport': 'tcp'}, endpoints=[{}] * 5)}
        stats = dict(write_profile_expected_connections=5, write_profile_captured_connections=5,
                     write_profile_sampled_requests=640, write_profile_available=1,
                     write_profile_storage_append_calls=640, write_profile_storage_persist_calls=640,
                     write_profile_storage_file_persist_calls=640,
                     write_profile_storage_append_errors=0, write_profile_storage_persist_errors=0)
        run.validate_profiles({'db': stats}, databases, 0, 128)
        for change in (dict(write_profile_captured_connections=4, write_profile_available=0),
                       dict(write_profile_sampled_requests=641),
                       dict(write_profile_storage_append_errors=1),
                       dict(write_profile_storage_file_persist_calls=0)):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'write profile'):
                run.validate_profiles({'db': stats | change}, databases, 0, 128)
        with self.assertRaisesRegex(ValueError, 'clean population'):
            run.validate_profiles({'db': stats}, databases, 0, 0)

    def test_database_timings_survive_without_checkpoint_labels(self):
        total = 'rocksdb_write_batch_commit_latency_seconds_sum{db_name="epoch"}'
        count = 'rocksdb_write_batch_commit_latency_seconds_count{db_name="epoch"}'
        native = 'db_op_count{ks="transactions",op="get",db="perpetual"}'
        parsed = analyze.parse_metrics(f'{total} 1.5\n{count} 3\n{native} 7\n')
        self.assertEqual(parsed[total] / parsed[count], 0.5)
        self.assertEqual(parsed[native], 7)

    def test_failed_replay_never_produces_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'experiment').mkdir()
            (root/'experiment/results.json').write_text(json.dumps({'complete':False,'failure':'audit mismatch'}))
            (root/'receipt.json').write_text(json.dumps({'complete':False}))
            with self.assertRaises(ValueError):
                analyze.summarize(root)

class GuestAbiCeiling(unittest.TestCase):
    """A host build needs GLIBC 2.38/2.39; the guest loader has 2.34."""

    def test_the_guest_ceiling_is_recorded_next_to_the_check(self):
        self.assertEqual(build.GUEST_ABI_CEILING['GLIBC'], (2, 34))
        self.assertEqual(build.GUEST_ABI_CEILING['GLIBCXX'], (3, 4, 29))

    def test_a_host_build_above_the_guest_glibc_is_refused(self):
        versions = ("\nVersion needs section '.gnu.version_r' contains 3 entries:\n"
                    '  0000: Version: 1  Name: GLIBC_2.39\n'
                    '  0001: Version: 1  Name: GLIBC_2.34\n')
        required, failures = build.guest_abi('sui-node-remote', versions)
        self.assertEqual(required['GLIBC'], '2.39')
        self.assertTrue(any('GLIBC_2.39' in item and 'GLIBC_2.34' in item
                            for item in failures))

    def test_a_guest_build_is_admitted(self):
        versions = ("\nVersion needs section '.gnu.version_r' contains 2 entries:\n"
                    '  0000: Version: 1  Name: GLIBC_2.34\n'
                    '  0001: Version: 1  Name: GLIBCXX_3.4.29\n')
        required, failures = build.guest_abi('storage-node', versions)
        self.assertEqual(required, {'GLIBC': '2.34', 'GLIBCXX': '3.4.29',
                                    'CXXABI': None, 'OPENSSL': None})
        self.assertEqual(failures, [])

    def test_the_check_reads_real_readelf_output(self):
        import subprocess
        text = subprocess.check_output(['readelf', '--version-info', '/bin/ls'],
                                       text=True)
        required, failures = build.guest_abi('ls', text)
        self.assertIsNotNone(required['GLIBC'])
        # The host's own /bin/ls needs a newer glibc than the guest on most
        # workstations; the check must report it instead of passing silently.
        if build.abi_version(required['GLIBC']) > build.GUEST_ABI_CEILING['GLIBC']:
            self.assertTrue(failures)

    def test_a_missing_version_section_is_a_failure(self):
        required, failures = build.guest_abi('static', 'no version info here')
        self.assertEqual(set(required), set(build.GUEST_ABI_CEILING))
        self.assertTrue(all(value is None for value in required.values()))


class CheckpointSourceBoundaries(unittest.TestCase):
    support_relative = 'experiments/sui-checkpoint/checkpoint_support.py'

    def source_tree(self, root):
        identities = {}
        for owner in ('sui', 'tidehunter-sharded-wal'):
            base = root / 'sources' / owner
            base.mkdir(parents=True)
            if owner == 'tidehunter-sharded-wal':
                scripts = base / 'experiments/sui-checkpoint'
                scripts.mkdir(parents=True)
                (scripts / 'audit').mkdir()
                for path in Path(run.__file__).resolve().parent.glob('*.py'):
                    shutil.copy2(path, scripts / path.name)
                shutil.copy2(Path(deployment.__file__).resolve().parent.parent / 'resident_config.py',
                             base / 'experiments/resident_config.py')
                shutil.copy2(Path(deployment.__file__).resolve().parent.parent / 'portable_transport.py',
                             base / 'experiments/portable_transport.py')
            else:
                (base / 'Cargo.toml').write_text('[workspace]\n')
            identities[owner] = dict(revision='frozen', files={
                str(path.relative_to(base)): dict(sha256=build.digest(path), mode=0o644)
                for path in base.rglob('*') if path.is_file()})
        return identities

    def test_external_preparation_keeps_frozen_sources_immutable_without_ci_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = self.source_tree(root)
            script = root / 'sources/tidehunter-sharded-wal/experiments/sui-checkpoint/prepare_external.py'
            environment = {key: value for key, value in os.environ.items()
                           if key not in ('PYTHONDONTWRITEBYTECODE', 'PYTHONPYCACHEPREFIX')}
            subprocess.run([sys.executable, str(script), '--help'], env=environment,
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            for owner, identity in identities.items():
                base = root / 'sources' / owner
                actual = {str(path.relative_to(base)) for path in base.rglob('*') if path.is_file()}
                self.assertEqual(actual, set(identity['files']))
                for relative, entry in identity['files'].items():
                    self.assertEqual(build.digest(base / relative), entry['sha256'])
            build.verify_staged(root, identities)

    def cargo_sources(self, root, *, selected_version='0.1.0'):
        """Only local path/file Git dependencies; a fresh checkout needs Cargo offline disabled."""
        self.source_tree(root / 'inputs')
        sources = root / 'inputs/sources'
        engine = sources / 'tidehunter-sharded-wal'
        (engine / 'Cargo.toml').write_text(
            '[workspace]\nresolver = "2"\nmembers = ["tidehunter", "tidehunter-fabric"]\n'
            'exclude = ["experiments/sui-checkpoint/audit"]\n')
        for name in ('tidehunter', 'tidehunter-fabric'):
            crate = engine / name
            (crate / 'src').mkdir(parents=True)
            (crate / 'src/lib.rs').write_text('pub const SOURCE: &str = "selected";\n')
            manifest = f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"\n'
            if name == 'tidehunter':
                manifest += ('[dependencies]\ntidehunter-fabric = { path = "../tidehunter-fabric", optional = true }\n'
                    '[features]\nexperimental-distributed = []\ntest-utils = []\n'
                    'experimental-fabric = ["dep:tidehunter-fabric"]\n')
            (crate / 'Cargo.toml').write_text(manifest)
        audit = engine / 'experiments/sui-checkpoint/audit'
        (audit / 'src').mkdir()
        (audit / 'src/lib.rs').write_text('')
        (audit / 'Cargo.toml').write_text(
            '[workspace]\n[package]\nname = "sui-sharded-state-audit"\nversion = "0.1.0"\n'
            '[dependencies]\ntidehunter = { path = "../../../tidehunter", features = ["experimental-distributed", "test-utils"] }\n'
            '[features]\nfabric = ["tidehunter/experimental-fabric"]\n')
        published = root / 'published'
        shutil.copytree(engine, published)
        (published / 'tidehunter/src/lib.rs').write_text('pub const SOURCE: &str = "published";\n')
        def commit(path):
            for args in (['init', '--quiet'], ['add', '.'],
                         ['-c', 'user.name=Harness', '-c', 'user.email=harness@example.invalid',
                          'commit', '--quiet', '-m', 'fixture']):
                subprocess.run(['git', '-C', str(path), *args], check=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            return subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'], timeout=30).decode().strip()
        revision = commit(published)
        selected = engine / 'tidehunter/Cargo.toml'
        selected.write_text(selected.read_text().replace('version = "0.1.0"', f'version = "{selected_version}"'))
        sui = sources / 'sui'
        (sui / 'Cargo.toml').write_text(
            '[workspace]\nresolver = "2"\nmembers = ["typed-store", "sui-node"]\n')
        for name in ('typed-store', 'sui-node'):
            crate = sui / name
            (crate / 'src').mkdir(parents=True)
            (crate / 'src/lib.rs').write_text('')
            manifest = f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"\n'
            if name == 'typed-store':
                manifest += ('[target.\'cfg(not(windows))\'.dependencies]\n'
                    f'tidehunter = {{ git = "{published.as_uri()}", rev = "{revision}", version = "=0.1.0", '
                    'optional = true, features = ["experimental-distributed"] }\n'
                    '[features]\nsharded-wal-fabric = ["tidehunter", "tidehunter/experimental-fabric"]\n')
            else:
                manifest += ('[dependencies]\ntyped-store = { path = "../typed-store" }\n'
                    '[features]\nsharded-wal = ["typed-store/tidehunter"]\n'
                    'sharded-wal-fabric = ["sharded-wal", "typed-store/sharded-wal-fabric"]\n')
            (crate / 'Cargo.toml').write_text(manifest)
        commit(sui)
        commit(engine)
        return sources

    @patch.dict(os.environ, {'CARGO_NET_OFFLINE': 'false'})
    def test_prepare_selects_staged_engine_over_portable_git_pin_and_hash_binds_inputs(self):
        for fabric in (False, True):
            with self.subTest(fabric=fabric), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                sources = self.cargo_sources(root)
                sui, engine = sources / 'sui', sources / 'tidehunter-sharded-wal'
                # The published dependency is independently resolvable without a sibling
                # path patch. Cargo, not a mock, supplies this graph and the staged one.
                standalone = root / 'standalone.json'
                with standalone.open('w') as metadata, (root / 'standalone.log').open('w') as log:
                    build.bounded(['cargo', 'metadata', '--format-version', '1', '--features',
                        ','.join(build.metadata_features('sui', fabric))],
                        cwd=sui, stdout=metadata, stderr=log, timeout=60)
                git_packages = [p for p in json.loads(standalone.read_text())['packages']
                                if p['name'] == 'tidehunter']
                self.assertEqual(len(git_packages), 1)
                self.assertTrue(git_packages[0]['source'].startswith('git+file:'))
                before = {owner: build.source_files(sources / owner) for owner in ('sui', 'tidehunter-sharded-wal')}
                output = root / 'prepared'
                build.prepare(SimpleNamespace(sui=sui, engine=engine, output=output, jobs=2, fabric=fabric))
                self.assertEqual(before, {owner: build.source_files(sources / owner) for owner in before})
                manifest = run.read_json(output / 'source-manifest.json')
                self.assertEqual(set(manifest), {'sui', 'tidehunter-sharded-wal'})
                staged = output / 'sources'
                support = staged / 'tidehunter-sharded-wal' / self.support_relative
                self.assertEqual(manifest['tidehunter-sharded-wal']['files'][self.support_relative]['sha256'],
                                 build.digest(support))
                for relative in ('sui', 'tidehunter-sharded-wal', 'tidehunter-sharded-wal/experiments/sui-checkpoint/audit'):
                    metadata = run.read_json(output / ('resolve-' + relative.replace('/', '-') + '.json'))
                    packages = {p['name']: p for p in metadata['packages']}
                    for name in ('tidehunter', 'tidehunter-fabric') if fabric else ('tidehunter',):
                        self.assertIsNone(packages[name]['source'])
                        self.assertEqual(Path(packages[name]['manifest_path']),
                                         staged / 'tidehunter-sharded-wal' / name / 'Cargo.toml')
                    lock = staged / relative / 'Cargo.lock'
                    owner, _, subpath = relative.partition('/')
                    self.assertEqual(manifest[owner]['files'][str(Path(subpath) / 'Cargo.lock')]['sha256'],
                                     build.digest(lock))
                    records = [p for p in tomllib.loads(lock.read_text())['package']
                               if p['name'] in ('tidehunter', 'tidehunter-fabric')]
                    self.assertTrue(records)
                    self.assertTrue(all('source' not in p for p in records))
                self.assertNotEqual(build.digest(sui / 'Cargo.toml'), build.digest(staged / 'sui/Cargo.toml'))
                self.assertEqual(manifest['sui']['files']['Cargo.toml']['sha256'],
                                 build.digest(staged / 'sui/Cargo.toml'))
                build.verify_staged(output, manifest)
                with tarfile.open(output / 'sources.tar.gz') as archive:
                    owners = {Path(name).parts[1] for name in archive.getnames() if len(Path(name).parts) > 1}
                    self.assertEqual(owners, {'sui', 'tidehunter-sharded-wal'})
                    for owner, identity in manifest.items():
                        for relative, record in identity['files'].items():
                            archived = archive.extractfile(f'sources/{owner}/{relative}').read()
                            self.assertEqual(hashlib.sha256(archived).hexdigest(), record['sha256'])

    @patch.dict(os.environ, {'CARGO_NET_OFFLINE': 'false'})
    def test_prepare_rejects_git_version_mismatch_resolved_by_real_cargo(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sources = self.cargo_sources(root, selected_version='0.2.0')
            output = root / 'prepared'
            with self.assertRaisesRegex(ValueError, 'outside the selected staged source'):
                build.prepare(SimpleNamespace(sui=sources / 'sui', engine=sources / 'tidehunter-sharded-wal',
                    output=output, jobs=2, fabric=True))
            resolved = run.read_json(output / 'resolve-sui.json')
            package = next(p for p in resolved['packages'] if p['name'] == 'tidehunter')
            self.assertEqual(package['version'], '0.1.0')
            self.assertTrue(package['source'].startswith('git+file:'))
            self.assertFalse((output / 'plan.json').exists())
            self.assertFalse((output / 'sources.tar.gz').exists())

    def test_prepare_preserves_standalone_local_path_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sources = self.cargo_sources(root)
            dependency = sources / 'sui/typed-store/Cargo.toml'
            text = dependency.read_text()
            spec = tomllib.loads(text)['target']['cfg(not(windows))']['dependencies']['tidehunter']
            old = f'git = "{spec["git"]}", rev = "{spec["rev"]}"'
            dependency.write_text(text.replace(old, 'path = "../../tidehunter-sharded-wal/tidehunter"'))
            before = build.source_files(sources / 'sui')
            output = root / 'prepared'
            build.prepare(SimpleNamespace(sui=sources / 'sui', engine=sources / 'tidehunter-sharded-wal',
                output=output, jobs=2, fabric=True))
            self.assertEqual(before, build.source_files(sources / 'sui'))
            self.assertEqual(build.digest(sources / 'sui/Cargo.toml'),
                             build.digest(output / 'sources/sui/Cargo.toml'))
            build.verify_staged(output, run.read_json(output / 'source-manifest.json'))

    def test_prepare_rejects_ambiguous_git_pins_and_wrong_local_path_before_cargo(self):
        for change in ('ambiguous-pin', 'wrong-path'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                sources = self.cargo_sources(root)
                dependency = sources / 'sui/typed-store/Cargo.toml'
                if change == 'ambiguous-pin':
                    text = dependency.read_text()
                    spec = text.split('tidehunter = ', 1)[1].splitlines()[0]
                    rev = tomllib.loads(text)['target']['cfg(not(windows))']['dependencies']['tidehunter']['rev']
                    spec = spec.replace(rev, '0' * 40)
                    (sources / 'sui/sui-node/Cargo.toml').write_text(
                        (sources / 'sui/sui-node/Cargo.toml').read_text() + '\n[dev-dependencies]\ntidehunter = ' + spec + '\n')
                else:
                    text = dependency.read_text()
                    spec = tomllib.loads(text)['target']['cfg(not(windows))']['dependencies']['tidehunter']
                    old = f'git = "{spec["git"]}", rev = "{spec["rev"]}"'
                    dependency.write_text(text.replace(old, f'path = "{root / "published/tidehunter"}"'))
                output = root / 'prepared'
                with patch.object(build, 'bounded') as cargo:
                    with self.assertRaises(ValueError):
                        build.prepare(SimpleNamespace(sui=sources / 'sui', engine=sources / 'tidehunter-sharded-wal',
                            output=output, jobs=2, fabric=True))
                    cargo.assert_not_called()
                self.assertFalse((output / 'plan.json').exists())

    def test_prepare_rejects_missing_support_before_cargo(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identities = self.source_tree(root / 'inputs')
            support = root / 'inputs/sources/tidehunter-sharded-wal' / self.support_relative
            support.unlink()
            del identities['tidehunter-sharded-wal']['files'][self.support_relative]
            def snapshot(source, destination):
                shutil.copytree(source, destination)
                return identities[source.name]
            with patch.object(build, 'snapshot', side_effect=snapshot), patch.object(build, 'bounded') as cargo:
                with self.assertRaises(ValueError):
                    build.prepare(SimpleNamespace(sui=root / 'inputs/sources/sui',
                        engine=root / 'inputs/sources/tidehunter-sharded-wal', output=root / 'prepared',
                        jobs=2, fabric=False))
                cargo.assert_not_called()

    def replay_binding(self, root):
        identities = self.source_tree(root)
        build.save(root / 'source-manifest.json', identities)
        (root / 'sources.tar.gz').write_bytes(b'bound archive')
        manifest = dict(protocol=9, fabric=False,
            source_archive_sha256=build.digest(root / 'sources.tar.gz'),
            source_manifest_sha256=build.digest(root / 'source-manifest.json'),
            binaries={name: 'unused' for name in ('sui-node-native', 'sui-node-remote',
                'storage-node', 'state-audit', 'rocksdb-audit')})
        build.save(root / 'build-manifest.json', manifest)
        return identities, manifest

    def test_replay_rejects_missing_changed_or_unbound_support_before_spawn(self):
        for change in ('missing', 'changed', 'unbound'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                build_root = root / 'build'
                identities, manifest = self.replay_binding(build_root)
                support = build_root / 'sources/tidehunter-sharded-wal' / self.support_relative
                if change == 'changed':
                    support.write_text('changed checkpoint verification\n')
                else:
                    support.unlink()
                    if change == 'unbound':
                        del identities['tidehunter-sharded-wal']['files'][self.support_relative]
                        build.save(build_root / 'source-manifest.json', identities)
                        manifest['source_manifest_sha256'] = build.digest(build_root / 'source-manifest.json')
                        build.save(build_root / 'build-manifest.json', manifest)
                fixture = root / 'fixture'
                fixture.mkdir()
                output = root / 'replay'
                arguments = ['run.py', '--build', str(build_root), '--fixture', str(fixture), '--output', str(output)]
                with patch.object(sys, 'argv', arguments), patch.object(run.Owned, 'spawn') as spawn:
                    with self.assertRaises(ValueError):
                        run.main()
                    spawn.assert_not_called()
                self.assertFalse(output.exists())

    def test_build_rejects_changed_support_before_compilation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, manifest = self.replay_binding(root)
            build.save(root / 'plan.json', manifest)
            (root / 'sources/tidehunter-sharded-wal' / self.support_relative).write_text('changed\n')
            with patch.object(build, 'bounded') as cargo:
                with self.assertRaises(ValueError):
                    build.build(SimpleNamespace(output=root, target=root / 'target', deadline=60))
                cargo.assert_not_called()
            self.assertFalse((root / 'binaries').exists())


class RunnerKnobs(unittest.TestCase):
    def environment(self, **changes):
        values = dict(tier='native', fragment_mib=8, driver_concurrency=4, pacing_ms=0,
                      value_cache_size=None, default_value_cache_size=1000,
                      max_dirty_keys=1024, snapshot_written_bytes=8 << 30, max_maps=3,
                      max_index_maps=3,
                      index_blob_cache_min_bytes=None, index_blob_cache_bytes=0,
                      read_profile_limit=0, native_write_profile_limit=0,
                      mutex_count=1)
        values.update(changes)
        return run.runtime_environment(
            {'PATH': '/bin', 'TH_MAX_MAPS': '99',
             'TH_DEFAULT_MUTEX_COUNT': '1024',
             'SUI_CHECKPOINT_EXECUTION_MAX_CONCURRENCY': '16'}, **values)

    def test_explicit_values_override_inherited_engine_knobs(self):
        env = self.environment()
        self.assertEqual(env['TH_MAX_MAPS'], '3')
        self.assertEqual(env['TH_MAX_INDEX_MAPS'], '3')
        self.assertEqual(env['TH_DEFAULT_MAX_DIRTY_KEYS'], '1024')
        self.assertEqual(env['TH_DEFAULT_VALUE_CACHE_SIZE'], '1000')
        self.assertEqual(env['SUI_CHECKPOINT_EXECUTION_MAX_CONCURRENCY'], '4')
        self.assertNotIn('TH_VALUE_CACHE_SIZE', env)
        self.assertNotIn('SUI_CHECKPOINT_EXECUTION_PACING_MS', env)
        self.assertEqual(env['TH_SYNC_WRITES'], 'false')
        self.assertEqual(env['TH_DEFAULT_MUTEX_COUNT'], '1',
                         'the frozen layout replaces an inherited fork default')
        self.assertEqual(self.environment(tier='synchronous')['TH_SYNC_WRITES'], 'true')
        self.assertEqual(env['TH_SNAPSHOT_WRITTEN_BYTES'], str(8 << 30))
        self.assertNotIn('TH_INDEX_BLOB_CACHE_MIN_BYTES', env,
                         'the floor row stays at the engine 64 MiB default when unset')
        explicit = self.environment(index_blob_cache_min_bytes=0)
        self.assertEqual(explicit['TH_INDEX_BLOB_CACHE_MIN_BYTES'], '0')

    def test_flattened_cache_and_pacing_are_opt_in(self):
        env = self.environment(value_cache_size=0, pacing_ms=245)
        self.assertEqual(env['TH_VALUE_CACHE_SIZE'], '0')
        self.assertEqual(env['SUI_CHECKPOINT_EXECUTION_PACING_MS'], '245')

    def test_unknown_inherited_th_knob_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'TH_UNDOCUMENTED'):
            run.runtime_environment({'TH_UNDOCUMENTED': '1'}, tier='native',
                fragment_mib=8, driver_concurrency=4, pacing_ms=0, value_cache_size=None,
                default_value_cache_size=1000, max_dirty_keys=1024,
                snapshot_written_bytes=8 << 30, max_maps=3, max_index_maps=3,
                index_blob_cache_min_bytes=0, index_blob_cache_bytes=0,
                read_profile_limit=0, native_write_profile_limit=0, mutex_count=1)

    def test_value_and_index_map_limits_are_independent_and_never_panic(self):
        env = self.environment(max_maps=5, max_index_maps=3)
        self.assertEqual(env['TH_MAX_MAPS'], '5')
        self.assertEqual(env['TH_MAX_INDEX_MAPS'], '3')
        for field in ('max_maps', 'max_index_maps'):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'panics below 3'):
                self.environment(**{field: 2})

    def test_producer_has_no_range_and_measured_modes_are_explicit(self):
        producer = {'run-with-range': run.checkpoint.CheckpointBound(1775), 'db-path': 'x'}
        run.configure_range(producer, mode='producer', tip=1775, short_run=False)
        self.assertNotIn('run-with-range', producer)
        command = run.producer_command('sui-node-native', 'producer.yaml')
        self.assertEqual(command, ['sui-node-native', '--config-path', 'producer.yaml'])
        self.assertNotIn('--run-with-range-checkpoint', command)
        full = run.replay_mode(1775, None)
        self.assertEqual(full, dict(tip=1775, short_run=False, bound=1776))
        self.assertEqual(run.replay_mode(1775, 1776), full)
        for mode in ('native', 'remote'):
            config = {'run-with-range': 0}
            run.configure_range(config, mode=mode, tip=1775, short_run=False)
            self.assertEqual(int(config['run-with-range']), 1776)
        short = run.replay_mode(1775, 8)
        self.assertEqual(short, dict(tip=7, short_run=True, bound=7))
        for mode in ('native', 'remote'):
            config = {'run-with-range': 0}
            run.configure_range(config, mode=mode, tip=short['tip'],
                                short_run=short['short_run'])
            self.assertEqual(int(config['run-with-range']), 7)
        scrape = 'last_executed_checkpoint 7\n'
        self.assertEqual(run.require_short_watermark(scrape, 7), 7)
        for tip in (6, 8):
            with self.subTest(tip=tip), self.assertRaisesRegex(RuntimeError, 'expected'):
                run.require_short_watermark(scrape, tip)
        with self.assertRaisesRegex(RuntimeError, 'None'):
            run.require_short_watermark('no gauge\n', 7)

    def test_short_run_records_are_json_serializable(self):
        catchup, certified, freshness = run.short_run_records(
            tip=7, digest='DIGEST', certified_epoch=3,
            namespaces={'live/store/perpetual', 'live/store/epoch_0', 'live/checkpoints'})
        self.assertEqual(freshness['required_namespaces'],
                         ['live/checkpoints', 'live/store/epoch_0',
                          'live/store/perpetual'])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'results.json'
            build.save(path, dict(catchup=catchup, certified=certified,
                                  freshness=freshness))
            stored = json.loads(path.read_text())
        self.assertEqual(stored['freshness'], freshness)
        self.assertEqual(stored['certified'], certified)
        self.assertEqual(stored['catchup'], catchup)

    def test_checkpoint_limit_is_an_inclusive_count(self):
        for requested, expected in ((None, 1775), (1, 0), (8, 7), (100, 99),
                                    (1776, 1775)):
            self.assertEqual(run.requested_checkpoint(1775, requested), expected)
            if requested is not None:
                self.assertEqual(expected + 1, requested)
        self.assertEqual(run.replay_bound(run.requested_checkpoint(1775, 8)) + 1, 8)
        for requested in (0, 1777, -1, True, 1.0, '10'):
            with self.subTest(requested=requested), self.assertRaises(ValueError):
                run.requested_checkpoint(1775, requested)

    def test_pacing_needs_the_frozen_sui_hook(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'sources/sui/crates'
            executor = root / 'sui-core/src/checkpoints/checkpoint_executor'
            module = executor / 'mod.rs'
            module.parent.mkdir(parents=True)
            module.write_text('let concurrency = 4;\n')
            config = root / 'sui-config/src/node.rs'
            config.parent.mkdir(parents=True)
            config.write_text('SUI_CHECKPOINT_EXECUTION_PACING_MS\n')
            self.assertFalse(run.pacing_support(Path(tmp)),
                             'only the executor tree or a built binary counts')
            pacing = executor / 'pacing.rs'
            pacing.write_text('pub const HOOK: &str = "SUI_CHECKPOINT_EXECUTION_PACING_MS";\n')
            self.assertTrue(run.pacing_support(Path(tmp)))
            pacing.unlink()
            (Path(tmp) / 'binaries').mkdir()
            binary = Path(tmp) / 'binaries/sui-node-native'
            binary.write_bytes(b'\0' * 4096 + b'SUI_CHECKPOINT_EXECUTION_PACING_MS' + b'\0' * 16)
            self.assertTrue(run.pacing_support(Path(tmp)))
            binary.write_bytes(b'\0' * 8192)
            self.assertFalse(run.pacing_support(Path(tmp)))


def durable_cut_frame(live, *, epoch=2, next_=16, format=2, crc_delta=0):
    """One CRC-framed `durable-cut.v1` payload, as the engine writes it."""
    import zlib
    payload = json.dumps(dict(format=format, database=[0] * 16,
                              live=None if live is None else
                              dict(epoch=epoch, next=next_),
                              owners=[], truncations=[]),
                         sort_keys=True).encode()
    checksum = (zlib.crc32(payload) + crc_delta) & 0xffffffff
    return len(payload).to_bytes(4, 'big') + checksum.to_bytes(4, 'big') + payload


class IndexRowsAgreement(unittest.TestCase):
    def test_a_stated_index_section_would_desynchronise_the_nodes(self):
        self.assertIsNone(run.external_index_rows_error({'endpoints': []}))
        message = run.external_index_rows_error({'index': dict(frag_size=64 << 20)})
        self.assertIn('index section', message)
        self.assertIn('native node', message)


class LifecycleChecks(unittest.TestCase):
    def test_durable_cut_record_is_the_engine_crc_frame(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'durable-cut.v1'
            path.write_bytes(durable_cut_frame(True))
            record = run.durable_cut_record(path)
            self.assertEqual(run.durable_cut_frontier(record), dict(epoch=2, next=16))
            untracked = path.with_name('untracked.v1')
            untracked.write_bytes(durable_cut_frame(None))
            self.assertIsNone(run.durable_cut_frontier(
                run.durable_cut_record(untracked)))
            path.write_bytes(durable_cut_frame(True, crc_delta=1))
            with self.assertRaisesRegex(ValueError, 'CRC'):
                run.durable_cut_record(path)
            path.write_bytes(durable_cut_frame(True)[:-1])
            with self.assertRaisesRegex(ValueError, 'trailing'):
                run.durable_cut_record(path)
            path.write_bytes(durable_cut_frame(True, format=1))
            with self.assertRaisesRegex(ValueError, 'format'):
                run.durable_cut_record(path)
            with self.assertRaisesRegex(ValueError, 'malformed'):
                run.durable_cut_frontier(dict(format=2, live=dict(epoch=1)))

    def test_durable_cut_comparison_refuses_an_extension(self):
        record = lambda next_: dict(format=2, live=dict(epoch=2, next=next_))
        before, same, extended = (record(16), record(16), record(17))
        compared = run.durable_cut_comparison(dict(a=before), dict(a=same))
        self.assertTrue(compared['tracked'])
        self.assertIn('flush_durable', compared['scope'])
        self.assertEqual(compared['namespaces'], {'a': dict(
            tracked=True, before=dict(epoch=2, next=16),
            after=dict(epoch=2, next=16))})
        with self.assertRaisesRegex(ValueError, 'extended after flush_durable'):
            run.durable_cut_comparison(dict(a=before), dict(a=extended))
        untracked = dict(format=2, live=None)
        skipped = run.durable_cut_comparison(dict(a=untracked), dict(a=untracked))
        self.assertEqual((skipped['tracked'], skipped['namespaces']),
                         (False, {'a': dict(tracked=False)}))
        with self.assertRaisesRegex(ValueError, 'tracking changed'):
            run.durable_cut_comparison(dict(a=untracked), dict(a=before))

    def test_durable_cut_paths_find_the_namespace_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'remote'
            direct = root / 'live/checkpoints/durable-cut.v1'
            direct.parent.mkdir(parents=True)
            direct.write_bytes(durable_cut_frame(True))
            nested = root / 'live/store/perpetual/authority/durable-cut.v1'
            nested.parent.mkdir(parents=True)
            nested.write_bytes(durable_cut_frame(True))
            (root / 'live/store/epoch_0').mkdir(parents=True)
            paths = run.durable_cut_paths(root, ['live/checkpoints', 'live/store/perpetual',
                                                 'live/store/epoch_0'])
            self.assertEqual(paths, {'live/checkpoints': direct,
                                     'live/store/perpetual': nested})

    def live(self, exposed, observed=2, fixtures=0):
        return dict(available=True, counters={
            'target_exposed_total': exposed, 'target_observed_partitions': observed,
            'target_fixture_partitions': fixtures})

    def test_exports_comparison_refuses_a_new_export(self):
        compared = run.exports_comparison(self.live(6), self.live(6))
        self.assertEqual({key: compared[key] for key in
                          ('available', 'reason', 'before', 'after',
                           'observed_partitions', 'fixture_partitions')},
                         dict(available=True, reason=None, before=6, after=6,
                              observed_partitions=2, fixture_partitions=0))
        self.assertIn('post-reopen scrape', compared['scope'])
        with self.assertRaisesRegex(ValueError, '1 new export'):
            run.exports_comparison(self.live(6), self.live(7))
        with self.assertRaisesRegex(ValueError, 'regressed'):
            run.exports_comparison(self.live(7), self.live(6))
        fixture = run.exports_comparison(self.live(0, fixtures=2), self.live(0, fixtures=2))
        self.assertFalse(fixture['available'])
        self.assertIn('fixture target', fixture['reason'])
        missing = run.exports_comparison(dict(available=False, counters={}),
                                         self.live(0))
        self.assertFalse(missing['available'])

    def audit(self, tables):
        return dict(databases={'ns': dict(native=dict(tables=tables),
                                         remote=dict(tables=tables))},
                    rocksdb={'store': dict(equal=True, tables=tables)})

    def test_audit_comparison_refuses_a_changed_table(self):
        same = self.audit({'t': [1, 'a']})
        self.assertTrue(run.audit_comparison(same, self.audit({'t': [1, 'a']}))['equal'])
        with self.assertRaisesRegex(ValueError, 'differs after the reopen'):
            run.audit_comparison(same, self.audit({'t': [2, 'a']}))

    def test_index_blob_cache_needs_both_frozen_sui_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = root / 'sources/sui/crates/typed-store/src/tidehunter_util.rs'
            engine.parent.mkdir(parents=True)
            engine.write_text('fn thdb_config() { Config::default() }\n')
            self.assertFalse(run.index_blob_cache_support(root))
            message = run.index_blob_cache_error(root)
            self.assertIn('TH_INDEX_BLOB_CACHE_BYTES', message)
            self.assertIn('TH_INDEX_BLOB_CACHE_MIN_BYTES', message)
            self.assertIn('thdb_config', message)
            self.assertIn('native node', message)
            engine.write_text(
                'let v = parse_env_usize("TH_INDEX_BLOB_CACHE_BYTES", 1);\n')
            self.assertFalse(run.index_blob_cache_support(root),
                             'the budget row alone is not the pair')
            engine.write_text(
                'let v = parse_env_usize("TH_INDEX_BLOB_CACHE_BYTES", 1);\n'
                'let floor = parse_env_usize("TH_INDEX_BLOB_CACHE_MIN_BYTES", 64 << 20);\n')
            self.assertTrue(run.index_blob_cache_support(root))
            self.assertIsNone(run.index_blob_cache_error(root))
            engine.unlink()
            (root / 'binaries').mkdir()
            binary = root / 'binaries/sui-node-remote'
            binary.write_bytes(b'\0' * 4096 + b'TH_INDEX_BLOB_CACHE_BYTES'
                               + b'\0' * 16 + b'TH_INDEX_BLOB_CACHE_MIN_BYTES' + b'\0' * 8)
            self.assertTrue(run.index_blob_cache_support(root))
            binary.write_bytes(b'\0' * 4096 + b'TH_INDEX_BLOB_CACHE_BYTES' + b'\0' * 16)
            self.assertFalse(run.index_blob_cache_support(root))

    def test_the_floor_is_accepted_at_zero_and_never_exceeds_the_budget(self):
        self.assertIsNone(run.index_blob_cache_floor_error(None, 1024))
        self.assertIsNone(run.index_blob_cache_floor_error(0, 1024))
        self.assertIsNone(run.index_blob_cache_floor_error(64 << 20, 1024))
        self.assertIsNone(run.index_blob_cache_floor_error(64 << 20, 64))
        self.assertIsNone(run.index_blob_cache_floor_error(1 << 30, 0))
        self.assertIn('exceeds its budget',
                      run.index_blob_cache_floor_error((1 << 30) + 1, 1024))
        self.assertIsNone(run.index_blob_cache_floor_error((1 << 30) - 1, 1024))

    def test_index_blob_cache_is_an_explicit_off_by_default_knob(self):
        environment = dict(tier='native', fragment_mib=8, driver_concurrency=4,
                           pacing_ms=0, value_cache_size=None,
                           default_value_cache_size=1000, max_dirty_keys=1024,
                           snapshot_written_bytes=8 << 30, max_maps=3,
                           max_index_maps=3, index_blob_cache_min_bytes=None,
                           read_profile_limit=0, native_write_profile_limit=0,
                           mutex_count=1)
        off = run.runtime_environment({'PATH': '/bin'}, index_blob_cache_bytes=0,
                                      **environment)
        self.assertNotIn('TH_INDEX_BLOB_CACHE_BYTES', off)
        inherited = run.runtime_environment({'TH_INDEX_BLOB_CACHE_BYTES': '1024'},
                                            index_blob_cache_bytes=0, **environment)
        self.assertNotIn('TH_INDEX_BLOB_CACHE_BYTES', inherited,
                         'an inherited budget never sizes the cache behind the runner')
        on = run.runtime_environment({'PATH': '/bin'},
                                     index_blob_cache_bytes=(1 << 30), **environment)
        self.assertEqual(on['TH_INDEX_BLOB_CACHE_BYTES'], str(1 << 30))


class ReopenWithFakes(unittest.TestCase):
    """The reopen orchestration against a fake node, RPC and metrics server."""

    class Child:
        returncode = -signal.SIGTERM

    class Owned:
        def __init__(self, root):
            self.root, self.commands, self.stopped = Path(root), [], []

        def start(self, name, command, extra=None):
            self.commands.append((name, [str(part) for part in command], extra))
            return ReopenWithFakes.Child()

        def stop(self, child):
            self.stopped.append(child)

    class Response:
        def __init__(self, data):
            self.data = data

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, size=-1):
            return self.data

    def reopen(self, root, *, producer_delta='', tail=2):
        owned = self.Owned(root)

        def rpc(address, method, params):
            sequence = int(params[0])
            shift = producer_delta if address == 'producer' else ''
            return dict(sequenceNumber=sequence, digest=f'D{sequence}{shift}', epoch=0)

        with patch.object(run.checkpoint, 'wait_rpc', lambda child, address: 1775), \
                patch.object(run.checkpoint, 'wait_for_rpc_indexes',
                             lambda child, address, tip, metrics: (
                                 metrics, dict(status='all-ten-committed-at-frozen-tip'))), \
                patch.object(run.checkpoint, 'rpc', rpc), \
                patch.object(run.urllib.request, 'urlopen',
                             lambda *args, **kwargs: self.Response(
                                 b'last_executed_checkpoint 1775\n')):
            receipt, metrics = run.reopen(
                owned, binary='sui-node-remote', root=Path(root), address='node',
                producer_address='producer', tip=1775, digest='D1775', bound=1776,
                extra={'SUI_SHARDED_WAL_CONFIG': 'config.json'},
                tail_checkpoints=tail)
        return owned, receipt, metrics

    def test_reopen_certifies_the_tip_and_reads_back_the_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            owned, receipt, metrics = self.reopen(tmp)
            self.assertEqual(receipt['certified'], dict(
                sequenceNumber=1775, digest='D1775', epoch=0))
            self.assertEqual(receipt['tail']['window'], [1774, 1775])
            self.assertEqual([row['digest'] for row in receipt['tail']['sample']],
                             ['D1774', 'D1775'])
            self.assertEqual(receipt['exit_code'], -signal.SIGTERM)
            self.assertEqual(len(owned.stopped), 1)
            name, command, extra = owned.commands[0]
            self.assertEqual(name, 'reopen')
            self.assertEqual(command[-2:], ['--run-with-range-checkpoint', '1776'],
                             'the inclusive bound stays above the persisted watermark')
            self.assertEqual(extra, {'SUI_SHARDED_WAL_CONFIG': 'config.json'})
            self.assertEqual((Path(tmp) / 'reopen-final.prom').read_bytes(),
                             b'last_executed_checkpoint 1775\n')

    def test_a_changed_tail_digest_fails_the_reopen(self):
        with tempfile.TemporaryDirectory() as tmp, \
                self.assertRaisesRegex(RuntimeError, 'differs after the reopen'):
            self.reopen(tmp, producer_delta='!', tail=1)


class CampaignBuildTargets(unittest.TestCase):
    source_tree = CheckpointSourceBoundaries.source_tree
    replay_binding = CheckpointSourceBoundaries.replay_binding

    support_relative = 'experiments/sui-checkpoint/checkpoint_support.py'

    def test_owner_is_a_kv_research_binary_and_transport_floor_is_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identities, manifest = self.replay_binding(root)
            artifact = root / 'cargo-artifact'
            artifact.parent.mkdir(exist_ok=True)
            for owner in ('sui', 'tidehunter-sharded-wal'):
                path = root / 'sources' / owner / 'Cargo.lock'
                path.write_text('# frozen\n')
                identities[owner]['files']['Cargo.lock'] = {
                    'sha256': build.digest(path), 'mode': 0o644}
            build.save(root / 'source-manifest.json', identities)
            manifest['source_manifest_sha256'] = build.digest(root / 'source-manifest.json')
            build.save(root / 'plan.json', dict(manifest, fabric=False, jobs=2,
                native_features=['typed-store/tidehunter'],
                remote_features=['sui-node/sharded-wal']))

            def emitted(log, name, kind):
                artifact.write_bytes(name.encode())
                return artifact
            with patch.object(build, 'bounded'), \
                    patch.object(build, 'compiler_identity', return_value={}), \
                    patch.object(build, 'cargo_executable', side_effect=emitted), \
                    patch.object(build.subprocess, 'check_output', return_value=b''):
                build.build(SimpleNamespace(output=root, target=root / 'target', deadline=60))
            receipt = json.loads((root / 'build-manifest.json').read_text())
            labels = [command['label'] for command in receipt['commands']]
            self.assertEqual(labels, ['sui-node-native', 'sui-node-remote', 'rocksdb-audit',
                                      'storage-node', 'state-audit'])
            owner = next(c for c in receipt['commands'] if c['label'] == 'storage-node')
            for argument in ('-p', 'kv-research', '--bin', 'kv-research', '--features', 'test-utils'):
                self.assertIn(argument, owner['command'])
            self.assertFalse(any('sharded_wal_bench' in argument
                                 or 'sharded_wal_transport_bench' in argument
                                 for c in receipt['commands'] for argument in c['command']))
            self.assertEqual(set(receipt['binaries']), set(labels))


class ReplaySamplingLifecycle(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.owner = run.Owned(self.root, os.environ.copy())
        command = [sys.executable, '-c',
                   "import sys;sys.stdin.buffer.read(1);"
                   "print('actual child failure: operation deadline expired',flush=True);sys.exit(7)"]
        with (self.root/'remote.log').open('xb') as log:
            self.child = build.OwnedProcess(command, stdin=subprocess.PIPE,
                                            stdout=log, stderr=subprocess.STDOUT)
        self.owner.children.append(self.child)
        self.addCleanup(self.owner.close)
        self.addCleanup(self.child._process.stdin.close)

    def finish_child(self):
        self.child._process.stdin.write(b'x')
        self.child._process.stdin.flush()
        self.assertEqual(self.child.wait(timeout=5), 7)
        self.assertFalse(self.child.reaped)
        self.assertIsNone(self.child._process.returncode)

    def test_live_procfs_failures_are_not_exits_or_zero_io(self):
        read_text = Path.read_text
        for error in (PermissionError, FileNotFoundError, ProcessLookupError):
            failure = error('live procfs access failed')
            def read_proc(path, *args, **kwargs):
                if str(path) == f'/proc/{self.child.pid}/io':
                    raise failure
                return read_text(path, *args, **kwargs)
            with self.subTest(error=error.__name__), patch.object(Path, 'read_text', read_proc):
                with self.assertRaises(error) as raised:
                    run.sample(self.child)
                self.assertIs(raised.exception, failure)
                self.assertIsNone(self.child.poll())

    def test_permission_transition_after_exit_keeps_owned_exit_status(self):
        read_text = Path.read_text
        def read_proc(path, *args, **kwargs):
            if str(path) == f'/proc/{self.child.pid}/io':
                self.finish_child()
                raise PermissionError('counter access revoked during exit')
            return read_text(path, *args, **kwargs)
        with patch.object(Path, 'read_text', read_proc):
            row = run.sample(self.child)
        self.assertTrue(row['exited'])
        self.assertEqual(row['exit_code'], 7)
        self.assertEqual(row['unavailable'], {'io': 'PermissionError'})
        self.assertNotIn('io', row)
        self.assertNotIn('VmRSS', row)

    def test_permission_transition_checks_fresh_core_dump_state(self):
        status_reads = 0
        def read_proc(path, *args, **kwargs):
            nonlocal status_reads
            if str(path) == f'/proc/{self.child.pid}/status':
                status_reads += 1
                return f'CoreDumping:\t{int(status_reads > 1)}\nVmRSS:\t4 kB\n'
            if str(path) == f'/proc/{self.child.pid}/io':
                raise PermissionError('counter access revoked during core dump')
            self.fail(f'unexpected procfs path: {path}')
        with patch.object(Path, 'read_text', read_proc):
            row = run.sample(self.child)
        self.assertEqual(status_reads, 2)
        self.assertFalse(row['exited'])
        self.assertTrue(row['core_dumping'])
        self.assertEqual(row['unavailable'], {'io': 'PermissionError'})
        self.assertNotIn('io', row)
        self.assertNotIn('VmRSS', row)

    def test_replay_keeps_child_failure_samples_and_last_real_metrics(self):
        read_text = Path.read_text
        io_reads = 0
        def read_proc(path, *args, **kwargs):
            nonlocal io_reads
            if str(path) == f'/proc/{self.child.pid}/io':
                io_reads += 1
                if io_reads == 2:
                    self.finish_child()
                    raise PermissionError('counter access revoked during exit')
            return read_text(path, *args, **kwargs)
        metrics = 'last_executed_checkpoint 0\n'
        with patch.object(self.owner, 'start', return_value=self.child), \
                patch.object(run.checkpoint, 'configuration', return_value={'metrics-address': 'unused'}), \
                patch.object(run.urllib.request, 'urlopen', return_value=io.BytesIO(metrics.encode())), \
                patch.object(run, 'host_sample', return_value={}), \
                patch.object(Path, 'read_text', read_proc):
            with self.assertRaisesRegex(RuntimeError, f'owned process {self.child.pid} exited with code 7'):
                run.replay(self.owner, 'unused', self.root, 'remote', 1, 'digest', {}, [], 30)
        rows = [json.loads(line) for line in (self.root/'remote-samples.jsonl').read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertIn('io', rows[0]['node'])
        self.assertEqual(rows[1]['node']['exit_code'], 7)
        self.assertEqual(rows[1]['node']['unavailable'], {'io': 'PermissionError'})
        self.assertNotIn('io', rows[1]['node'])
        self.assertEqual((self.root/'remote-foreground.prom').read_text(), metrics)
        self.assertIn('actual child failure: operation deadline expired', (self.root/'remote.log').read_text())
        self.assertFalse(self.child.reaped)


class AggregateMetricCut(unittest.TestCase):
    """The 2026-11 engine has aggregate live counters and coordinator metrics.

    Per-owner `tidehunter_operational_read_io`, `tidehunter_shard_read` and the
    adapter `shard_*` family are removed from `analyze`; no test may require a
    generation counter or a five-partition population.
    """

    def scrape(self, *, reads=10, connects=1, keyspace='store', db='store', extra='',
               watermark=7):
        db_label = f',db="{db}"' if db else ''
        return '\n'.join([
            f'last_executed_checkpoint {watermark}',
            f'tidehunter_nvmeof_live_completed_read_ops_total {reads}',
            'tidehunter_nvmeof_live_completed_read_bytes_total 4096',
            'tidehunter_nvmeof_live_read_io_samples_total 4',
            'tidehunter_nvmeof_live_read_io_seconds_total 0.004',
            'tidehunter_nvmeof_live_errors_total 0',
            f'tidehunter_nvmeof_live_namespace_connects_total {connects}',
            'tidehunter_nvmeof_live_target_observed_partitions 2',
            f'db_op_bucket{{op="get",ks="{keyspace}"{db_label},le="1"}} 2',
            f'db_op_bucket{{op="get",ks="{keyspace}"{db_label},le="+Inf"}} 3',
            f'db_op_sum{{op="get",ks="{keyspace}"{db_label}}} 300',
            f'db_op_count{{op="get",ks="{keyspace}"{db_label}}} 3',
            f'large_table_contention_bucket{{ks="{keyspace}",le="+Inf"}} 2',
            f'large_table_contention_sum{{ks="{keyspace}"}} 20',
            f'large_table_contention_count{{ks="{keyspace}"}} 2',
            'index_blob_cache_hits 5',
            'index_blob_cache_fills 1',
        ]) + '\n' + extra

    def wait(self, texts, *, remote=True, namespaces={'live/store'}, budget=1,
             baseline=None, tip=7, observations=None):
        clock = [10.]
        def sleep(seconds):
            clock[0] += seconds
        responses = iter(texts)
        last = texts[-1]
        def scrape(*args, **kwargs):
            nonlocal last
            last = next(responses, last)
            return io.BytesIO(last.encode())
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(run.time, 'monotonic', side_effect=lambda: clock[0]), \
                patch.object(run.time, 'sleep', side_effect=sleep), \
                patch.object(run, 'sample', return_value={'pid': 123, 'exited': False}), \
                patch.object(run.urllib.request, 'urlopen', side_effect=scrape):
            destination = Path(directory) / 'final.prom'
            try:
                return run.wait_for_final_metrics(object(), [], 'unused', set(namespaces),
                    remote=remote, end=10 + budget, destination=destination,
                    baseline=baseline, tip=tip, observations=observations)
            finally:
                self.last_retained = destination.read_text() if destination.exists() else None

    def test_aggregate_live_and_coordinator_metrics_replace_owner_generations(self):
        baseline = analyze.metric_cut(analyze.parse_metrics(self.scrape()), {'live/store'}, remote=True)
        self.assertTrue(baseline['live']['available'])
        self.assertEqual(baseline['live']['completed_read_ops'], 10)
        self.assertEqual(baseline['live']['observed_partitions'], 2)
        self.assertEqual(baseline['coordinator']['databases'], ['store'])
        self.assertEqual(baseline['coordinator']['keyspaces'], ['store'])
        contention = baseline['coordinator']['large_table_contention']['']['store']
        self.assertEqual(contention['count'], 2)
        self.assertEqual(contention['mean_us'], 10)
        self.assertEqual(baseline['coordinator']['index_blob_cache']['hits'], 5)

    def test_owner_fetch_fraction_is_requests_not_cache_hits(self):
        current = analyze.metric_cut(analyze.parse_metrics(self.scrape()), {'live/store'}, remote=True)
        fraction = analyze.owner_fetch_fraction(current['live'], current['coordinator'])
        self.assertTrue(fraction['available'])
        self.assertEqual(fraction['owner_read_ops'], 10)
        self.assertEqual(fraction['api_gets'], 3)
        self.assertAlmostEqual(fraction['fraction'], 10 / 3)
        missing = analyze.owner_fetch_fraction(dict(completed_read_ops=10), dict(operations={}))
        self.assertFalse(missing['available'])
        self.assertIsNone(missing['fraction'])

    def test_remote_cut_requires_an_aggregate_publication_advance(self):
        first = analyze.metric_cut(analyze.parse_metrics(self.scrape()), {'live/store'}, remote=True)
        same = analyze.metric_cut(analyze.parse_metrics(self.scrape()), {'live/store'}, remote=True)
        self.assertFalse(analyze.metric_cut_progressed(first, same, remote=True))
        advanced = analyze.metric_cut(analyze.parse_metrics(self.scrape(reads=11)), {'live/store'}, remote=True)
        self.assertTrue(analyze.metric_cut_progressed(first, advanced, remote=True))
        with self.assertRaisesRegex(ValueError, 'regressed'):
            analyze.metric_cut_progressed(advanced, first, remote=True)

    def test_absent_live_family_is_transient_not_a_zero(self):
        with self.assertRaises(analyze.MetricCutUnavailable):
            analyze.metric_cut(analyze.parse_metrics('db_op_count{op="get",ks="store"} 1\n'),
                               {'live/store'}, remote=True)
        with self.assertRaises(analyze.MetricCutUnavailable):
            analyze.metric_cut(analyze.parse_metrics(self.scrape(keyspace='other', db='other')),
                               {'live/store'}, remote=True)

    def test_legacy_generation_counters_are_not_required_or_accepted(self):
        text, cut = self.wait([self.scrape(reads=11)], baseline=self.scrape())
        self.assertEqual(cut['status'], 'post-certified-scrape')
        self.assertEqual(cut['baseline_scope'], analyze.PRE_CERTIFICATION_BASELINE)
        self.assertEqual(cut['baseline']['live']['completed_read_ops'], 10)
        self.assertEqual(cut['final']['live']['completed_read_ops'], 11)
        self.assertEqual(self.last_retained, text)
        legacy = self.scrape(extra='tidehunter_adapter{namespace="live/store",stat="shard_sample_available"} 1\n')
        with self.assertRaisesRegex(ValueError, 'legacy per-shard'):
            analyze.adapter_stats(analyze.parse_metrics(legacy))
        self.assertNotIn('shard_sample_available', analyze.adapter_stats(analyze.parse_metrics(self.scrape())))

    def test_archived_cut_cannot_change_its_population(self):
        baseline = analyze.metric_cut(analyze.parse_metrics(self.scrape()), {'live/store'}, remote=True)
        final = analyze.metric_cut(analyze.parse_metrics(self.scrape(reads=11)), {'live/store'}, remote=True)
        cut = dict(status='post-certified-scrape', required_namespaces=['live/store'],
                   baseline=baseline, final=final,
                   baseline_scope=analyze.PRE_CERTIFICATION_BASELINE)
        values = analyze.parse_metrics(self.scrape(reads=11))
        analyze.validate_final_metric_cut(values, {'live/store'}, cut, remote=True)
        for change in (dict(required_namespaces=[]), dict(status='old'),
                       dict(baseline=dict(live=baseline['live'])),
                       dict(baseline_scope='first post-certification scrape')):
            with self.subTest(change=change), self.assertRaises(ValueError):
                analyze.validate_final_metric_cut(values, {'live/store'}, cut | change, remote=True)

    def test_remote_cut_requires_a_pre_certification_baseline(self):
        with self.assertRaisesRegex(ValueError, 'pre-certification baseline'):
            self.wait([self.scrape(reads=11)])

    def test_wait_retains_the_last_real_response_on_no_progress(self):
        with self.assertRaises(TimeoutError):
            self.wait([self.scrape(), self.scrape()], baseline=self.scrape(), budget=0.5)
        self.assertEqual(self.last_retained, self.scrape())

    def test_final_scrape_below_the_certified_tip_is_not_accepted(self):
        with self.assertRaises(TimeoutError):
            self.wait([self.scrape(watermark=6, reads=11)], baseline=self.scrape(), budget=0.5)
        with self.assertRaisesRegex(RuntimeError, 'passed the certified tip'):
            self.wait([self.scrape(watermark=8, reads=11)], baseline=self.scrape(), budget=0.5)

    def test_final_scrape_regression_fails_the_cut(self):
        with self.assertRaisesRegex(ValueError, 'regressed'):
            self.wait([self.scrape(reads=9)], baseline=self.scrape())

    def test_timeout_record_states_available_without_an_advance(self):
        observations = []
        with self.assertRaisesRegex(TimeoutError, 'observation\\(s\\), 0 unavailable'):
            self.wait([self.scrape()], baseline=self.scrape(), budget=0.5,
                      observations=observations)
        self.assertTrue(observations[0]['live_available'])

    def test_timeout_record_states_an_unavailable_cut(self):
        observations = []
        unavailable = self.scrape(keyspace='other', db='other')
        with self.assertRaisesRegex(TimeoutError, 'observation\\(s\\), [1-9]\\d* unavailable'):
            self.wait([unavailable], baseline=self.scrape(), budget=0.5,
                      observations=observations)
        self.assertTrue(observations[0]['unavailable'])

    def test_observation_record_is_bounded(self):
        text, cut = self.wait([self.scrape()] * 200 + [self.scrape(reads=11)],
                              baseline=self.scrape(), budget=30)
        self.assertEqual(len(cut['observations']), run.FINAL_METRIC_OBSERVATION_LIMIT)
        self.assertEqual(cut['final']['live']['completed_read_ops'], 11)

    def test_missing_coordinator_namespace_is_retried_not_accepted(self):
        with self.assertRaises(TimeoutError):
            self.wait([self.scrape(keyspace='other', db='other'),
                       self.scrape(keyspace='other', db='other')],
                      baseline=self.scrape(), namespaces={'live/store'}, budget=0.5)
        self.assertEqual(self.last_retained, self.scrape(keyspace='other', db='other'))

    def test_native_cut_needs_no_live_family(self):
        native = self.scrape().replace('tidehunter_nvmeof_live_completed_read_ops_total', '# removed')
        native = '\n'.join(line for line in native.splitlines() if not line.startswith('tidehunter_nvmeof_live_')) + '\n'
        text, cut = self.wait([native, native], remote=False)
        self.assertEqual(text, native)
        self.assertFalse(cut['final']['live']['available'])
        self.assertTrue(cut['final']['coordinator']['available'])

    def test_expected_population_excludes_closed_epochs_not_missing_live_namespaces(self):
        schema = {'live/checkpoints': {}, 'live/store/perpetual': {},
                  'live/store/epoch_0': {}, 'live/store/epoch_1': {}}
        self.assertEqual(run.live_metric_namespaces(schema, 1),
            {'live/checkpoints', 'live/store/perpetual', 'live/store/epoch_1'})
        with self.assertRaises(ValueError):
            run.live_metric_namespaces(schema, 2)


class FreshnessFailureRecord(unittest.TestCase):
    def test_a_failed_gate_saves_its_bounded_observations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            text = 'last_executed_checkpoint 1\n'
            owned = SimpleNamespace(root=root, start=lambda *args, **kwargs: object())
            def refuse(*args, **kwargs):
                kwargs['observations'].append(dict(
                    seconds=0.5, live_available=True, keyspaces=['store']))
                raise TimeoutError(
                    'final metrics freshness exceeded original replay deadline: '
                    '1 observation(s), 0 unavailable, last []')
            with patch.object(run, 'sample', return_value={'pid': 1, 'exited': False}), \
                    patch.object(run, 'host_sample', return_value={}), \
                    patch.object(run.checkpoint, 'configuration',
                                 return_value={'metrics-address': 'unused',
                                               'json-rpc-address': 'unused'}), \
                    patch.object(run.checkpoint, 'wait_for_rpc_indexes',
                                 return_value=(text, {})), \
                    patch.object(run.checkpoint, 'rpc',
                                 return_value={'digest': 'digest',
                                               'sequenceNumber': 1, 'epoch': 0}), \
                    patch.object(run.deployment, 'schemas',
                                 return_value={'live/store': {},
                                               'live/store/epoch_0': {}}), \
                    patch.object(run.urllib.request, 'urlopen',
                                 side_effect=lambda *a, **k: io.BytesIO(text.encode())), \
                    patch.object(run, 'wait_for_final_metrics', side_effect=refuse):
                with self.assertRaisesRegex(TimeoutError, '1 observation'):
                    run.replay(owned, 'unused', root, 'remote', 1, 'digest',
                               {'SUI_SHARDED_WAL_SCHEMA_OUTPUT': str(root / 'schema')},
                               [], 30)
            record = json.loads((root / 'remote-freshness.json').read_text())
            self.assertEqual(record['status'], 'failed')
            self.assertEqual(record['tip'], 1)
            self.assertIn('1 observation', record['failure'])
            self.assertTrue(record['observations'][0]['live_available'])

    def test_replay_passes_the_last_replay_scrape_below_the_certified_tip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scrapes = ['last_executed_checkpoint 0\n', 'last_executed_checkpoint 1\n',
                       'last_executed_checkpoint 2\n', 'last_executed_checkpoint 2\n']
            seen = {}
            def refuse(*args, **kwargs):
                seen.update(baseline=kwargs['baseline'], tip=kwargs['tip'])
                raise TimeoutError(
                    'final metrics freshness exceeded original replay deadline: '
                    '0 observation(s), 0 unavailable, last []')
            owned = SimpleNamespace(root=root, start=lambda *args, **kwargs: object())
            with patch.object(run, 'sample', return_value={'pid': 1, 'exited': False}), \
                    patch.object(run, 'host_sample', return_value={}), \
                    patch.object(run.checkpoint, 'configuration',
                                 return_value={'metrics-address': 'unused',
                                               'json-rpc-address': 'unused'}), \
                    patch.object(run.checkpoint, 'wait_for_rpc_indexes',
                                 return_value=(scrapes[-1], {})), \
                    patch.object(run.checkpoint, 'rpc',
                                 return_value={'digest': 'digest',
                                               'sequenceNumber': 2, 'epoch': 0}), \
                    patch.object(run.deployment, 'schemas',
                                 return_value={'live/store': {},
                                               'live/store/epoch_0': {}}), \
                    patch.object(run.urllib.request, 'urlopen',
                                 side_effect=[io.BytesIO(text.encode()) for text in scrapes]), \
                    patch.object(run, 'wait_for_final_metrics', side_effect=refuse):
                with self.assertRaisesRegex(TimeoutError, '0 observation'):
                    run.replay(owned, 'unused', root, 'remote', 2, 'digest',
                               {'SUI_SHARDED_WAL_SCHEMA_OUTPUT': str(root / 'schema')},
                               [], 30)
            self.assertEqual(seen['tip'], 2)
            self.assertEqual(seen['baseline'], scrapes[1])


class EvidenceBoundaries(unittest.TestCase):
    def assert_process_gone(self, pid, message):
        for _ in range(100):
            try:
                state = Path(f'/proc/{pid}/stat').read_text().split()[2]
            except (FileNotFoundError, ProcessLookupError):
                return
            if state == 'Z':
                return
            time.sleep(.01)
        self.fail(message)

    def test_cargo_uses_emitted_executable_not_unqualified_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            executable = root/'cross-target/release/sui-node'
            executable.parent.mkdir(parents=True)
            executable.write_bytes(b'current')
            log = root/'cargo.jsonl'
            log.write_text(json.dumps(dict(reason='compiler-artifact', target=dict(name='sui-node', kind=['bin']),
                                           executable=str(executable)))+'\n')
            self.assertEqual(build.cargo_executable(log, 'sui-node', 'bin'), executable)
            log.write_text(json.dumps(dict(reason='build-finished', success=True))+'\n')
            with self.assertRaises(ValueError):
                build.cargo_executable(log, 'sui-node', 'bin')

    def test_archive_reader_ignores_tampered_unpacked_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'experiment').mkdir()
            (root/'experiment/results.json').write_text('tampered')
            with tarfile.open(root/'evidence.tar.gz', 'w:gz') as archive:
                content = b'{"complete":true}'
                member = tarfile.TarInfo('experiment/results.json')
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
            build.save(root/'receipt.json', {'complete':True, 'evidence_sha256':build.digest(root/'evidence.tar.gz')})
            with analyze.evidence_archive(root) as archive:
                self.assertEqual(analyze.archived_json(archive, 'experiment/results.json'), {'complete':True})

    def test_external_loopback_fabric_is_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, 'loopback'):
                deployment.prepare(Path(tmp)/'deployment', {}, Path(tmp), shape_yamls={},
                    hosts=['10.0.0.'+str(i) for i in range(5)], port=24000, transport='fabric-tcp',
                    coordinator_cache=0, shard_cache=0, guest_root=Path('/owned'))
            self.assertFalse((Path(tmp)/'deployment').exists())

    def test_audit_timeout_kills_owned_descendants(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pidfile = root/'descendant.pid'
            script = ("import subprocess,sys,time,pathlib; "
                      "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); "
                      "pathlib.Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(60)")
            with self.assertRaises(subprocess.TimeoutExpired):
                run.command_json([sys.executable, '-c', script, pidfile], root/'audit.json', os.environ.copy(), deadline=1)
            pid = int(pidfile.read_text())
            self.assert_process_gone(pid, 'audit descendant survived timeout cleanup')

    def test_audit_interrupt_reaps_process_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pidfile = root/'descendant.pid'
            script = ("import os,signal,subprocess,sys,time,pathlib; "
                      "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); "
                      "pathlib.Path(sys.argv[1]).write_text(str(p.pid)); "
                      "os.kill(os.getppid(),signal.SIGTERM); time.sleep(60)")
            def interrupted(signum, frame):
                raise InterruptedError('test interrupt')
            previous = signal.signal(signal.SIGTERM, interrupted)
            try:
                with self.assertRaises(InterruptedError):
                    run.command_json([sys.executable, '-c', script, pidfile], root/'audit.json', os.environ.copy())
            finally:
                signal.signal(signal.SIGTERM, previous)
            pid = int(pidfile.read_text())
            self.assert_process_gone(pid, 'audit descendant survived signal cleanup')

    def test_observed_exited_leader_stays_reserved_until_descendants_killed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pidfile = root/'descendant.pid'
            descendant = ("import os,signal,sys,time,pathlib;"
                          "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
                          "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()));time.sleep(60)")
            leader = ("import subprocess,sys,time,pathlib;"
                      "subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]]);"
                      "\nwhile not pathlib.Path(sys.argv[2]).exists(): time.sleep(.01)"
                      "\nsys.exit(7)")
            owner = run.Owned(root, os.environ.copy())
            child = owner.start('leader', [sys.executable, '-c', leader, descendant, pidfile])
            try:
                self.assertEqual(child.wait(timeout=5), 7)
                self.assertEqual(child.poll(), 7)  # checkpoint readiness/replay observation
                self.assertIsNone(child._process.returncode)
                delivered = []
                killpg = os.killpg
                def kill_reserved(pid, number):
                    status = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                    self.assertEqual(status.si_pid, child.pid)
                    self.assertFalse(child.reaped)
                    delivered.append(number)
                    killpg(pid, number)
                with patch.object(build.os, 'killpg', side_effect=kill_reserved):
                    owner.stop(child)
                self.assertEqual(delivered, [signal.SIGTERM, signal.SIGKILL])
                self.assertTrue(child.reaped)
                self.assertEqual(child.returncode, 7)
                with patch.object(build.os, 'killpg') as signal_recycled_id:
                    build.stop_group(child)
                    with self.assertRaises(ValueError):
                        owner.stop(child)
                    signal_recycled_id.assert_not_called()
                pid = int(pidfile.read_text())
                self.assert_process_gone(pid, 'descendant survived exited-leader cleanup')
            finally:
                owner.close()

    def test_cached_exit_does_not_authorize_signalling_reused_pid(self):
        child = object.__new__(build.OwnedProcess)
        child.pid, child.returncode, child.reaped = 12345, 0, False
        child._process = SimpleNamespace(returncode=None)
        with patch.object(build.os, 'waitid', side_effect=ChildProcessError), \
                patch.object(build.os, 'killpg') as signal_recycled_id:
            with self.assertRaisesRegex(RuntimeError, 'reaped outside'):
                build.stop_group(child)
            signal_recycled_id.assert_not_called()
        child._process.returncode = 0  # underlying Popen reaped it; PID may now name another child
        with patch.object(build.os, 'waitid') as observe_recycled_id, \
                patch.object(build.os, 'killpg') as signal_recycled_id:
            with self.assertRaisesRegex(RuntimeError, 'reaped outside'):
                build.stop_group(child)
            observe_recycled_id.assert_not_called()
            signal_recycled_id.assert_not_called()
        with patch.object(build.os, 'killpg') as signal_unowned:
            with self.assertRaises(TypeError):
                build.stop_group(object())
            signal_unowned.assert_not_called()

    def test_successful_bounded_command_cleans_descendants_before_reaping(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pidfile = root/'descendant.pid'
            script = ("import subprocess,sys,pathlib;"
                      "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);"
                      "pathlib.Path(sys.argv[1]).write_text(str(p.pid))")
            build.bounded([sys.executable, '-c', script, pidfile], timeout=5)
            pid = int(pidfile.read_text())
            self.assert_process_gone(pid, 'successful command left a live descendant')

    def test_stage_history_and_bound_resource_handshake(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            binding = dict(config_sha256='c', build_manifest_sha256='b', checkpoint_digest='d')
            for stage in ('remote-starting', 'remote-finished', 'resources-required'):
                row = run.publish_stage(directory, stage, binding)
                self.assertEqual(run.read_json(directory/f'{stage}.json'), row)
            self.assertEqual(run.read_json(directory/'stage.json'), row)
            with self.assertRaises(TimeoutError):
                run.wait_resources(directory, binding, timeout=0)
            for mutation in ({'config_sha256':'wrong'}, {'build_manifest_sha256':'wrong'},
                             {'checkpoint_digest':'wrong'}, {'unix_ns':0}):
                build.save(directory/'resources-ready', dict(binding, unix_ns=time.time_ns()) | mutation)
                with self.assertRaises(ValueError):
                    run.wait_resources(directory, binding)
            build.save(directory/'resources-ready', dict(binding, unix_ns=time.time_ns()))
            run.wait_resources(directory, binding)
            with self.assertRaises(ValueError):
                run.publish_stage(directory, 'remote-starting', binding)

    def test_summary_consumes_archived_metrics_audit_and_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = root/'experiment'
            evidence.mkdir()
            provenance = root/'provenance'
            provenance.mkdir()
            build.save(provenance/'source-manifest.json', {'frozen':'source'})
            build.save(provenance/'build-manifest.json', {
                'source_archive_sha256':'source-archive',
                'source_manifest_sha256':build.digest(provenance/'source-manifest.json')})
            build_hash = build.digest(provenance/'build-manifest.json')
            audit = dict(all_equal=True, databases={'live/db':dict(equal=True,
                         native=dict(tables={'objects':dict(records=5)}))}, rocksdb={'epochs':dict(equal=True)})
            def scrape(reads):
                return '\n'.join([
                    f'tidehunter_nvmeof_live_completed_read_ops_total {reads}',
                    'tidehunter_nvmeof_live_completed_read_bytes_total 4096',
                    'tidehunter_nvmeof_live_errors_total 0',
                    'tidehunter_nvmeof_live_namespace_connects_total 1',
                    'db_op_bucket{op="get",ks="objects",db="db",le="+Inf"} 3',
                    'db_op_sum{op="get",ks="objects",db="db"} 300',
                    'db_op_count{op="get",ks="objects",db="db"} 3',
                    'large_table_contention_bucket{ks="objects",le="+Inf"} 0',
                    'large_table_contention_sum{ks="objects"} 0',
                    'large_table_contention_count{ks="objects"} 0',
                ]) + '\n'
            def cut_for(text, remote):
                current = analyze.metric_cut(analyze.parse_metrics(text), {'live/db'}, remote=remote)
                baseline = analyze.metric_cut(analyze.parse_metrics(scrape(9) if remote else text),
                                              {'live/db'}, remote=remote) if remote else current
                cut = dict(status='post-certified-scrape', required_namespaces=['live/db'],
                           baseline=baseline, final=current, observations=[], seconds=0.,
                           unavailable_observations=0)
                if remote:
                    cut['baseline_scope'] = analyze.PRE_CERTIFICATION_BASELINE
                return cut
            native_text = 'last_executed_checkpoint 1\n'
            def replay_for(cut):
                return dict(completed_checkpoint_digest='tip', elapsed_seconds_including_startup=2,
                            completed_checkpoint_epoch=0, final_metrics_freshness=cut,
                            phase_timing={}, completed_window={}, peak_sampled_rss_bytes=4096, process_io_final={},
                            host_before=dict(swap_pages=dict(pswpin=0, pswpout=0)),
                            host_after=dict(swap_pages=dict(pswpin=0, pswpout=0)))
            result = dict(complete=True, metrics_schema=3, checkpoint_limit=1, checkpoint_digest='tip', audit=audit,
                          source_range={}, source_archive_sha256='source-archive', build_manifest_sha256=build_hash,
                          topology='loopback', durability='durable', cache={},
                          native=replay_for(cut_for(native_text, False)),
                          remote=replay_for(cut_for(scrape(10), True)))
            build.save(evidence/'results.json', result)
            build.save(evidence/'audit.json', audit)
            (evidence/'native-foreground.prom').write_text('last_executed_checkpoint 1\n')
            (evidence/'native-final.prom').write_text('last_executed_checkpoint 1\n')
            (evidence/'remote-foreground.prom').write_text(scrape(10))
            (evidence/'remote-final.prom').write_text(scrape(10))
            with tarfile.open(root/'evidence.tar.gz','w:gz') as archive:
                for directory in (evidence, provenance):
                    for path in directory.iterdir():
                        archive.add(path, arcname=str(path.relative_to(root)))
            build.save(root/'receipt.json', dict(complete=True, evidence_sha256=build.digest(root/'evidence.tar.gz'),
                build_manifest_sha256=build_hash, source_archive_sha256='source-archive'))
            before = analyze.summarize(root)
            (evidence/'results.json').write_text('tampered timings')
            (evidence/'audit.json').write_text('tampered audit')
            (evidence/'remote-foreground.prom').write_text('tampered metrics')
            self.assertEqual(analyze.summarize(root), before)
            self.assertEqual(before['modes']['native']['foreground_seconds'], 2)
            self.assertEqual(before['table_counts']['live/db']['objects'], 5)
            with (root/'evidence.tar.gz').open('ab') as stream:
                stream.write(b'tampered archive')
            with self.assertRaisesRegex(ValueError, 'archive binding'):
                analyze.summarize(root)


class ExternalResourceBoundaries(unittest.TestCase):
    def setUp(self):
        self.binding = dict(config_sha256='config', build_manifest_sha256='build', checkpoint_digest='tip')
        self.stages = [dict(unix_ns=10), dict(unix_ns=20), dict(unix_ns=30)]
        self.manifest = dict(storage_binary_sha256='binary', owners=5, hosts=[])
        self.resources = dict(self.binding, hosts={})
        for index in range(5):
            address = f'10.0.0.{index+1}'
            command = ['/owned/storage-node', '--experimental-storage-node', '/owned/db/launch.yaml']
            self.manifest['hosts'].append(dict(host=address, guest_root='/owned', commands=[command]))
            pid = 100+index
            process = dict(pid=pid, index=0, starttime_ticks=1, exited=False, unavailable={},
                           VmRSS=4096, VmHWM=8192, VmSwap=0,
                           io={key:0 for key in ('rchar','wchar','syscr','syscw','read_bytes','write_bytes','cancelled_write_bytes')})
            self.resources['hosts'][address] = dict(self.binding, storage_binary_sha256='binary',
                root='/owned', complete=True, cleanup_complete=True, all_namespaces_healthy=True,
                samples_dropped=0, failure=None,
                namespaces=[dict(command=command, pid=pid, process_group=pid, starttime_ticks=1, index=0, reaped=True)],
                samples=[dict(unix_ns=stamp, healthy=True, processes=[copy.deepcopy(process)]) for stamp in (9,15,21)],
                swap_before=dict(pswpin=0, pswpout=0), swap_after=dict(pswpin=0, pswpout=0),
                diskstats_before='259 0 nvme0n1 0 0 0 0 0 0 0 0 0 0 0\n',
                diskstats_after='259 0 nvme0n1 0 0 0 0 0 0 0 0 0 0 0\n')

    def validate(self):
        run.validate_resources(self.resources, self.manifest, self.binding, self.stages)

    def test_real_zero_counters_and_exited_final_sample_are_distinct(self):
        for host in self.resources['hosts'].values():
            host['samples'].append(dict(unix_ns=31, healthy=False, processes=[
                dict(pid=host['namespaces'][0]['pid'], exited=True, unavailable={'process':'exited_and_reaped'})]))
        self.validate()

    def test_numeric_io_and_rss_cannot_be_placeholders(self):
        original = copy.deepcopy(self.resources)
        for field in ('VmRSS','VmHWM','VmSwap','read_bytes','write_bytes'):
            for bad in (None, '0', True, -1, 0.0):
                with self.subTest(field=field, bad=bad):
                    self.resources = copy.deepcopy(original)
                    process = self.resources['hosts']['10.0.0.1']['samples'][1]['processes'][0]
                    (process if field.startswith('Vm') else process['io'])[field] = bad
                    with self.assertRaises(ValueError):
                        self.validate()

    def test_missing_owned_pid_and_reused_pid_fail(self):
        host = self.resources['hosts']['10.0.0.1']
        process = host['samples'][1]['processes'].pop()
        with self.assertRaises(ValueError):
            self.validate()
        process['starttime_ticks'] += 1
        host['samples'][1]['processes'].append(process)
        with self.assertRaises(ValueError):
            self.validate()

    def test_host_counter_placeholders_and_unbounded_samples_fail(self):
        original = copy.deepcopy(self.resources)
        for key, value in (('swap_before', {}), ('swap_after', None), ('diskstats_before', ''),
                           ('diskstats_after', 'unavailable'), ('samples_dropped', 1), ('complete', False)):
            with self.subTest(field=key):
                self.resources = copy.deepcopy(original)
                self.resources['hosts']['10.0.0.1'][key] = value
                with self.assertRaises(ValueError):
                    self.validate()

    def test_samples_must_span_replay(self):
        host = self.resources['hosts']['10.0.0.1']
        host['samples'] = host['samples'][1:]
        with self.assertRaises(ValueError):
            self.validate()


class EngineDefaults(unittest.TestCase):
    """No cell pins a fork default unless its reviewed row declares it (F9)."""

    def environment(self, **changes):
        values = dict(tier='native', fragment_mib=1024, driver_concurrency=4,
                      pacing_ms=0, value_cache_size=None,
                      default_value_cache_size=1000, max_dirty_keys=1024,
                      snapshot_written_bytes=8 << 30, max_maps=None,
                      max_index_maps=None, index_blob_cache_min_bytes=None,
                      index_blob_cache_bytes=0, read_profile_limit=0,
                      native_write_profile_limit=0, mutex_count=1)
        values.update(changes)
        return run.runtime_environment({'PATH': '/bin', 'TH_MAX_MAPS': '99'}, **values)

    def test_the_map_rows_stay_unset_and_the_mutex_row_is_pinned(self):
        env = self.environment()
        self.assertNotIn('TH_MAX_MAPS', env)
        self.assertNotIn('TH_MAX_INDEX_MAPS', env)
        self.assertEqual(env['TH_DEFAULT_MUTEX_COUNT'], '1',
                         'the frozen fixture layout is explicit, not inherited')
        self.assertEqual(env['TH_FRAG_SIZE'], str(1024 * deployment.MIB))

    def test_a_declared_map_limit_is_written_and_validated(self):
        env = self.environment(max_maps=3, max_index_maps=4)
        self.assertEqual(env['TH_MAX_MAPS'], '3')
        self.assertEqual(env['TH_MAX_INDEX_MAPS'], '4')
        for field in ('max_maps', 'max_index_maps'):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'panics below 3'):
                self.environment(**{field: 2})


class FrozenMutexCount(unittest.TestCase):
    """The replay declares the frozen fixture's keyspace layout."""

    def fixture(self, root, *, mutexes=1, name='checkpoint_content', extra=None):
        shape = (Path(root) / 'producer' / 'store' / 'live' / 'checkpoints'
                 / 'shape_v2.yaml')
        shape.parent.mkdir(parents=True)
        lines = ['key_spaces:', '- inner:', '    id: 0', f'    name: {name}',
                 '    key_indexing: !Fixed 32', f'    mutexes: {mutexes}',
                 '    key_type: !Uniform', '      cells_per_mutex: 1']
        if extra is not None:
            lines += ['- inner:', '    id: 1', f'    name: {extra[0]}',
                      f'    mutexes: {extra[1]}']
        shape.write_text('\n'.join(lines) + '\n')
        return Path(root)

    def test_the_count_comes_from_the_checkpoint_content_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp, mutexes=3, extra=('other', 7))
            self.assertEqual(run.frozen_mutex_count(root), 3)

    def test_a_shape_without_the_checkpoint_row_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp, name='other')
            with self.assertRaisesRegex(ValueError, 'checkpoint_content'):
                run.frozen_mutex_count(root)

    def test_two_shapes_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp)
            source = next((root / 'producer').rglob('shape_v2.yaml'))
            second = (root / 'producer' / 'other' / 'live' / 'checkpoints'
                      / 'shape_v2.yaml')
            second.parent.mkdir(parents=True)
            shutil.copy2(source, second)
            with self.assertRaisesRegex(ValueError, 'exactly one'):
                run.frozen_mutex_count(root)

    def test_the_declared_range_is_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp, mutexes=0)
            with self.assertRaisesRegex(ValueError, '1..4096'):
                run.frozen_mutex_count(root)


class ReviewedCellAdmission(unittest.TestCase):
    """A reviewed cell id fixes the knobs the cell may carry (F8, F25)."""

    def namespace(self, cell_id, **overrides):
        values = dict(cell_id=cell_id, durability_tier='native', fragment_mib=1024,
                      coordinator_cache_mib=0, max_maps=None, max_index_maps=None,
                      hold_seconds=0, checkpoints=None, lifecycle_checks=False,
                      tail_checkpoints=64, value_cache_size=None,
                      default_value_cache_size=1000, driver_concurrency=4,
                      pacing_ms=0, max_dirty_keys=1024, index_blob_cache_mib=0,
                      index_blob_cache_min_bytes=None)
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_each_reviewed_row_admits_its_own_knobs(self):
        rows = {
            'S1-0-native': dict(),
            'S1-0-synchronous': dict(),
            'S1-1-native': dict(lifecycle_checks=True),
            'S1-7-cache0-native': dict(value_cache_size=0),
            'S1-7-blob1024-native': dict(index_blob_cache_mib=1024,
                                         index_blob_cache_min_bytes=0),
            'M1-paced-c4-native': dict(pacing_ms=245),
            'M2-cache128-native': dict(default_value_cache_size=128),
            'M3-native': dict(fragment_mib=64, max_maps=3, max_index_maps=3,
                              max_dirty_keys=16),
            'M4-synchronous': dict(fragment_mib=64, index_blob_cache_mib=1024,
                                   index_blob_cache_min_bytes=0),
            'M5-hold': dict(hold_seconds=1800),
        }
        for cell_id, knobs in rows.items():
            with self.subTest(cell=cell_id):
                tier = 'synchronous' if cell_id.endswith('-synchronous') else 'native'
                row = run.cell_admission(self.namespace(cell_id, durability_tier=tier,
                                                        **knobs))
                self.assertEqual(row['id'], cell_id)
        self.assertIsNone(run.cell_admission(self.namespace(None)))

    def test_a_row_refuses_a_knob_it_does_not_declare(self):
        cases = (
            ('S1-1-native', dict(fragment_mib=8), 'fragment-mib'),
            ('S1-1-native', dict(coordinator_cache_mib=8), 'coordinator-cache-mib'),
            ('S1-1-native', dict(lifecycle_checks=False), 'lifecycle-checks'),
            ('M3-native', dict(fragment_mib=1024, max_maps=3, max_index_maps=3), 'fragment-mib'),
            ('M3-native', dict(fragment_mib=64, max_maps=3, max_index_maps=3,
                               max_dirty_keys=1024), 'max-dirty-keys'),
            ('M5-hold', dict(hold_seconds=0), 'hold-seconds'),
            ('M2-cache0-native', dict(value_cache_size=None), 'value-cache-size'),
        )
        for cell_id, knobs, needle in cases:
            with self.subTest(cell=cell_id, knob=needle):
                with self.assertRaisesRegex(ValueError, needle):
                    run.cell_admission(self.namespace(cell_id, **knobs))

    def test_a_wrong_tier_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'is a native cell, not synchronous'):
            run.cell_admission(self.namespace('S1-1-native',
                                              durability_tier='synchronous'))


class HoldWindow(unittest.TestCase):
    """M5: the certified node stays resident while its disk is sampled (F6)."""

    def test_the_hold_samples_disk_and_takes_a_final_scrape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reads = iter(range(1000))
            def disk_sample():
                return dict(unix_ns=time.time_ns(),
                            databases={'live/db': dict(allocated_bytes=next(reads))})
            with patch.object(run, 'sample', return_value=dict(pid=1)), \
                    patch.object(run.urllib.request, 'urlopen',
                                 return_value=io.BytesIO(b'last_executed_checkpoint 3\n')):
                hold = run.hold_node(object(), object(), [],
                                     address='127.0.0.1:1', seconds=0.3,
                                     disk_sample=disk_sample, interval_seconds=0.05,
                                     destination=root / 'remote-hold.prom',
                                     poll_seconds=0.01)
            self.assertGreaterEqual(hold['seconds'], 0.3)
            self.assertGreaterEqual(len(hold['disk_samples']), 2)
            self.assertTrue(hold['disk_samples'][-1]['databases'])
            self.assertTrue(hold['final_metrics_available'])
            self.assertEqual(hold['requested_seconds'], 0.3)
            self.assertEqual((root / 'remote-hold.prom').read_text(),
                             'last_executed_checkpoint 3\n')
            self.assertFalse(hold['triggers']['snapshot']['triggered'])
            self.assertFalse(hold['triggers']['relocation']['triggered'])
            self.assertIn('no external trigger', hold['triggers']['snapshot']['reason'])

    def test_a_dead_node_fails_the_hold(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(run, 'sample',
                              return_value=dict(pid=7, exited=True, exit_code=9)):
                with self.assertRaisesRegex(RuntimeError, 'exited with code 9'):
                    run.hold_node(object(), object(), [], address='127.0.0.1:1',
                                  seconds=0.2,
                                  disk_sample=lambda: dict(databases={}),
                                  interval_seconds=0.05,
                                  destination=Path(tmp) / 'remote-hold.prom',
                                  poll_seconds=0.01)


if __name__=='__main__':
    unittest.main()