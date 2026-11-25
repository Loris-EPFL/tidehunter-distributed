"""Offline contract fixtures transcribed from the Sui, L2, A1/V6 and R0/R1 producers."""

from copy import deepcopy
from hashlib import blake2b
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from q1_admit import replay_memory_binding, verify_image_recipe
from q1_capacity import read_r0, verify_r1_retirement
from q1_golden import DATABASES, TEMPLATES, arm_index, common_cut, genesis_spec, producer_command, topology, verify_restart_prefix
from q1_index import a1_local_command, validate_a1_local_build, validate_a1_local_reopen, validate_a1_selection
from q1_manifest import CAPACITY, MANDATORY, SLOTS
from q1_owner import bind_launch, owner_command, validate_config_command, verify_ready, verify_shutdown
from q1_package import file_digest

SHA = '1' * 64


def temporary_directory(test):
    directory = TemporaryDirectory(dir=Path.cwd())
    test.addCleanup(directory.cleanup)
    return Path(directory.name)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return file_digest(path)


class GoldenProducer(unittest.TestCase):
    def setUp(self):
        self.case = MANDATORY[0]
        self.placement = {'run_id': 'q1-fleet', 'slots': {
            slot: {'ordinary_ip': f'10.0.1.{i+1}', 'rdma_ip': f'10.0.2.{i+1}'} for i, slot in enumerate(SLOTS)}}
        self.layout = topology(self.placement, self.case, 10)
        self.routes = {}
        for slot in ('V2', 'V3'):
            endpoints = [{'partition': i, 'address': self.layout['owners'][f'owner_{slot}_{owner}']['control'],
                          'server_name': f'{slot.lower()}-{owner.lower()}'} for i, owner in enumerate(SLOTS[4:])]
            config = {'storage': {'partitions': [0, 1]}, 'endpoints': endpoints,
                      'tls': {'transport': {'kind': 'tcp'}, 'ca': '/tls/ca', 'cert': '/tls/cert', 'key': '/tls/key'},
                      'live_nvme': {'leased_durability': 'native', 'export_mode': 'per_owner'}}
            self.routes[slot] = {'databases': dict.fromkeys(DATABASES, config), 'templates': dict.fromkeys(TEMPLATES, config)}

    def test_l2_genesis_inputs_and_exact_route_roles(self):
        spec = genesis_spec(self.placement, self.case, self.layout, self.routes)
        self.assertEqual(spec['restart_validator'], 3)
        self.assertEqual(spec['retention'], {'kind': 'retain_all'})
        self.assertEqual(spec['validators'][1]['backend'], {'kind': 'native'})
        self.assertEqual(spec['validators'][3]['backend']['templates'], self.routes['V3']['templates'])
        self.assertEqual(set(spec['fullnode']), {'host', 'root', 'port_base'})
        routes = deepcopy(self.routes)
        routes['V3']['databases'][DATABASES[0]]['storage']['database'] = [1] * 16
        with self.assertRaisesRegex(ValueError, 'two Native owner hosts'):
            genesis_spec(self.placement, self.case, self.layout, routes)
        routes = deepcopy(self.routes)
        routes['V3']['templates'].pop(TEMPLATES[0])
        with self.assertRaisesRegex(ValueError, 'template patterns'):
            genesis_spec(self.placement, self.case, self.layout, routes)

    def test_arm_index_binds_actual_public_roster(self):
        spec = genesis_spec(self.placement, self.case, self.layout, self.routes)
        root = temporary_directory(self)
        committee = {'epoch_duration_ms': 30_000, 'restart_validator': 3, 'retention': spec['retention'],
                     'validators': [], 'fullnode': {'name': 'not-a-validator'}}
        for i, node in enumerate(spec['validators']):
            kind = node['backend']['kind']
            committee['validators'].append({
                'index': i, 'name': f'protocol-key-{i}', 'backend': kind,
                **{key: node[key] for key in ('host', 'port_base', 'root')},
                'network_address': '/ip4/10.0.1.1/tcp/17000', 'p2p_address': '/ip4/10.0.1.1/udp/17001',
                'consensus_address': '/ip4/10.0.1.1/tcp/17006',
                'sharded_routes': {'databases': list(DATABASES), 'templates': list(TEMPLATES)} if kind == 'remote' else None,
            })
        path = root / 'committee.json'
        receipt = {'phase': 'genesis', 'status': 'passed', 'run_id': spec['run_id'], 'case_id': self.case,
                   'artifacts': [{'path': path.name, 'sha256': write_json(path, committee), 'bytes': path.stat().st_size}]}
        captures = {i: f'capture-{i}' for i in range(4)}
        reopens = {i: f'reopen-{i}' for i in range(4)}
        arm = arm_index(root, receipt, spec, 'drive', captures, reopens)
        self.assertEqual(arm['validators'][3], {'index': 3, 'backend': 'remote', 'name': 'protocol-key-3',
                                               'pre_stop': 'capture-3', 'reopened': 'reopen-3'})
        committee['validators'][3]['sharded_routes']['templates'].reverse()
        receipt['artifacts'][0]['sha256'] = write_json(path, committee)
        with self.assertRaisesRegex(ValueError, 'committee differs'):
            arm_index(root, receipt, spec, 'drive', captures, reopens)

    def test_commands_and_certified_barriers(self):
        self.assertEqual(producer_command('native', 'genesis', 'spec', output='out'),
                         ['native', 'genesis', '--input', 'spec', '--output', 'out'])
        self.assertEqual(producer_command('sharded', 'reopen', 'capsule', cut=19, output='cold'),
                         ['sharded', 'reopen', '--capsule', 'capsule', '--cut', '19', '--output', 'cold'])
        args = producer_command('native', 'ctl', {'op': 'resume', 'barrier': 'restart_start'}, control='/private/sock')
        self.assertEqual(args[:5], ['native', 'ctl', '--socket', '/private/sock', '--request'])
        self.assertEqual(json.loads(args[-1]), {'op': 'resume', 'barrier': 'restart_start'})
        cut = {'ok': True, 'error': None, 'body': {'cut': 19, 'digest': 'cut-digest', 'pending_executions': 0,
                                                 'epoch_end_checkpoint': 19, 'qualified': True}}
        cuts = {slot: deepcopy(cut) for slot in SLOTS[:4]}
        self.assertEqual(common_cut(cuts), (19, 'cut-digest'))
        cuts['V3']['body']['digest'] = 'different'
        with self.assertRaisesRegex(ValueError, 'disagree'):
            common_cut(cuts)
        before = {'ok': True, 'error': None, 'body': {'sequence_number': 12, 'digest': 'prefix', 'epoch': 2,
                                                    'highest_executed': 12}}
        after = deepcopy(before)
        after['body']['highest_executed'] = 19
        verify_restart_prefix(before, after)
        after['body']['digest'] = 'changed'
        with self.assertRaisesRegex(ValueError, 'prefix changed'):
            verify_restart_prefix(before, after)


class OwnerHostProducer(unittest.TestCase):
    def setUp(self):
        self.durability = {'tier': 'native', 'max_unsynced_age_ms': None, 'max_pending_completions': 1024}
        self.prepared = {'host': {'max_epochs': 4, 'wal_prewrite': False}, 'limits': {},
                         'tls': {'transport': {'kind': 'tcp'}, 'ca': '/tls/ca', 'cert': '/tls/cert', 'key': '/tls/key'},
                         'durability': self.durability, 'history_persistence': 'wal_replay'}
        self.owner = {'partition': 1, 'control': '10.1.1.2:17100', 'root': '/data/storage', 'max_databases': 10,
                      'live_target_pool': {'address': '10.1.2.2', 'transport': 'rdma', 'public_port_base': 18000,
                                           'public_service_base': 18000, 'commissioning_port_base': 18010,
                                           'commissioning_service_base': 18010}}
        self.launch = bind_launch(self.prepared, self.owner, 'native', '/run/q1', 'nonce')
        self.receipt = {'partition': 1, 'address': '10.1.1.2:17100', 'launch_nonce': 'nonce', 'pid': 7,
                        'boot_id': 'boot-1', 'started_unix_ns': 100, 'durability': self.durability,
                        'history_persistence': 'wal_replay'}

    def test_l2_launch_and_ready_not_archive(self):
        self.assertEqual(self.launch['host']['live_target_pool'], self.owner['live_target_pool'])
        self.assertEqual(owner_command('kv', 'launch.json'), ['kv', '--experimental-storage-node', 'launch.json'])
        self.assertEqual(validate_config_command('kv', 'coord.json', ['o0.json', 'o1.json']),
                         ['kv', '--experimental-validate-config', 'coord.json', 'o0.json', 'o1.json'])
        self.assertEqual(verify_ready(self.receipt, self.launch, 'boot-1'), self.receipt)
        current = self.receipt | {'pid': 8, 'started_unix_ns': 101}
        verify_ready(current, self.launch, 'boot-1', previous=self.receipt)
        for bad in (self.receipt | {'database': [1] * 16}, self.receipt | {'launch_nonce': 'stale'},
                    self.receipt | {'boot_id': 'stale'}, self.receipt | {'partition': 0}):
            with self.assertRaises(ValueError):
                verify_ready(bad, self.launch, 'boot-1')
        with self.assertRaisesRegex(ValueError, 'restart'):
            verify_ready(self.receipt, self.launch, 'boot-1', previous=self.receipt)
        with self.assertRaisesRegex(ValueError, 'not NodeConfig'):
            bind_launch({'node': {}}, self.owner, 'native', '/run/q1', 'nonce')

    def test_shutdown_binds_all_routed_targets_and_launch_bytes(self):
        launch_bytes = json.dumps(self.launch).encode()
        identity = {'process_id': 7, 'process_start_ticks': 22, 'launch_blake2b512': blake2b(launch_bytes).hexdigest()}
        clean = {'fixture_only': False, 'target_implementation': 'kernel', 'errors': 0, 'owned_namespace_count': 0}
        proof = {'schema_version': 2, 'scope': 'kernel_live_target_shutdown', 'complete': True,
                 'owner': {'partition': 1, 'root': '/data/storage'}, **identity,
                 'targets': [{'database': [byte] * 16, 'target': dict(clean)} for byte in (1, 2)],
                 'preparation': {'wal_prewrite_bytes': 0, 'wal_prewrite_ns': 0, 'wal_prewrite_files': 0},
                 'inline_exports': 0, 'owner_threads': {'read_workers': 4}, 'serve_error': None, 'cleanup_error': None}
        unit = {'MainPID': '0', 'SubState': 'exited', 'Result': 'success', 'ExecMainStatus': '0'}
        expected = {'01' * 16, '02' * 16}
        def admit(value, data=launch_bytes):
            return verify_shutdown(value, self.launch, data, identity, self.receipt, unit, expected)
        self.assertEqual(admit(proof), proof)
        with self.assertRaisesRegex(ValueError, 'target set'):
            admit(proof | {'targets': proof['targets'][:1]})
        with self.assertRaisesRegex(ValueError, 'admitted process'):
            admit(proof, launch_bytes + b' ')
        broken = deepcopy(proof)
        broken['targets'][0]['target']['owned_namespace_count'] = 1
        with self.assertRaisesRegex(ValueError, 'unclean'):
            admit(broken)


class IndexProducer(unittest.TestCase):
    def test_a0_v6_gates_same_local_a1_run_and_exact_selection(self):
        manifest = {'schema_version': 1, 'tree': 'engine+patches', 'binary_sha256': SHA}
        v6 = manifest | {'gate': 'a0_v6_device_reconciliation', 'passed': True, 'max_abs_residual_ratio': 0.04,
                         'residual_limit': 0.10, 'tiers_covered': ['native', 'synchronous'],
                         'binary_blake2b512': '2' * 128, 'failures': [],
                         'runs': [{'run': f'v6-{tier}', 'tier': tier, 'ratio': ratio, 'valid': True,
                                   'diagnostics_sha256': SHA, 'reasons': []}
                                  for tier, ratio in (('native', 0.01), ('synchronous', 0.04))]}
        report = {'schema_version': 1, 'decision': {
            'verdict': 'Exact sufficient (projected)', 'selected_setting': 'e16', 'setting_status': {'e4': 'fail', 'e16': 'pass'},
            'v6_gate': {'passed': True, 'receipt': 'v6.json', 'tree': manifest['tree'], 'max_abs_residual_ratio': 0.04}},
            'settings': {'e16': {'arm': 'e16', 'status': 'pass', 'tiers': {'native': {}, 'synchronous': {}}}}}
        self.assertEqual(validate_a1_selection(v6, manifest, report, 'E16'), 'e16')
        for bad in (v6 | {'binary_sha256': '2' * 64}, v6 | {'max_abs_residual_ratio': 0.11},
                    v6 | {'tiers_covered': ['native']}, v6 | {'passed': False}):
            with self.assertRaisesRegex(ValueError, 'A0 V6'):
                validate_a1_selection(bad, manifest, report, 'E16')
        with self.assertRaisesRegex(ValueError, 'A1 did not select'):
            validate_a1_selection(v6, manifest, report, 'E4')

    def test_local_build_and_cold_are_actual_a1_schema_not_fleet_schema(self):
        opts = {'path': '/case/a1', 'owners': 1, 'transport': {'kind': 'tcp-trusted'}, 'durability_tier': 'synchronous',
                'index_fixture': {'stage': 'build', 'arm': 'exact', 'max_admitted_batches': 4096,
                                  'max_index_bytes': 268435456, 'fragment_bytes': 4194304, 'wal_file_bytes': 67108864},
                'replay_limits': {'max_batches': 4096, 'max_encoded_bytes': 268435456}}
        config = {'schema_version': 1, 'case_id': 'e4-synchronous-build', 'backend': {'kind': 'tidehunter', 'options': opts},
                  'seed': 77, 'key_namespace_hex': '2' * 32, 'dataset': {'keys': 1_000_000, 'value_size': 1024, 'entropy': 1.0}}
        dataset = config['dataset'] | {'seed': config['seed'], 'key_namespace_hex': config['key_namespace_hex']}
        rows = {'rows': dataset['keys'], 'mismatched': 0, 'mismatch_sample': [], 'elapsed_ns': 200}
        build = {'schema_version': 1, 'build_case_id': config['case_id'], 'path': opts['path'], 'dataset': dataset,
                 'shape_yaml': 'shape', 'fixture': opts['index_fixture'], 'replay_limits': opts['replay_limits'],
                 'spec': {'tier': 'synchronous'}, 'verification': {'dataset_rows': rows, 'successful_puts': dataset['keys'],
                   'iterated_rows': dataset['keys'] + 2, 'other_rows': 2, 'checkpoint_after_explicit_root': {'snapshot_generation': 1},
                   'owner_progress': [{'partition': 0}]}}
        validate_a1_local_build(config, build)
        self.assertEqual(a1_local_command('kv', 'build.json', 'fresh', config, 'build'),
                         ['kv', '--config', 'build.json', '--output', 'fresh'])
        with self.assertRaisesRegex(ValueError, 'local fixture'):
            a1_local_command('kv', 'build.json', 'fresh', config | {'dataset': dataset | {'keys': 100_000_000}}, 'build')
        overwrite = deepcopy(config)
        overwrite['case_id'] = 'e4-synchronous-overwrite'
        overwrite['backend']['options']['index_fixture']['stage'] = 'overwrite'
        self.assertEqual(a1_local_command('kv', 'overwrite.json', 'cold', overwrite, 'cold-reopen'),
                         ['kv', '--index-a1-cold-reopen', 'overwrite.json', '--output', 'cold'])
        stopped = {'fencing_generation': 2, 'snapshot_generation': 1, 'suffix_batches': 5, 'suffix_bytes': 7500}
        stop = {'schema_version': 1, 'stage': 'overwrite', 'case_id': overwrite['case_id'], 'measurement_ok': True,
                'dataset': dataset, 'stopped': stopped, 'timed': {'complete': True, 'ack_matches_publication': True},
                'touched': {'count': 10, 'first_read_id': 3, 'file': 'touched.bin'}}
        cold = {'schema_version': 1, 'case_id': overwrite['case_id'], 'qualified': True, 'cold': True, 'correct': True,
                'tail_matches_stop_record': True, 'fencing_generation_advanced': True,
                'close_error': None, 'owner_stop_error': None, 'touched_count': 10, 'stop_record': stopped,
                'verification': rows, 'first_read': {'id': 3, 'correct': True, 'open_start_to_first_correct_read_ns': 900},
                'open': {'recovery': {'tail_batches': 5, 'tail_bytes': 7500, 'cut': {'discarded_batches': 0}}},
                'residency': {'coordinator': {'resident_fraction': 0.01}, 'owner_wal': {'resident_bytes': 0},
                              'first_value_block_nonresident': True}}
        self.assertEqual(validate_a1_local_reopen(overwrite, stop, cold), 900)
        bad = deepcopy(cold)
        bad['open']['recovery']['cut']['discarded_batches'] = 1
        with self.assertRaisesRegex(ValueError, 'replay tail'):
            validate_a1_local_reopen(overwrite, stop, bad)


class CapacityProducer(unittest.TestCase):
    def test_r0_array_and_summary_count_remain_distinct(self):
        path = temporary_directory(self) / 'r0.json'
        events = ['begin', 'prune', 'roll', 'roll', 'roll', 'end']
        file = {'partition': 0, 'fencing_generation': 2, 'file_start': 0, 'file_end': 4096,
                'device': 91, 'inode': 5, 'allocated_bytes': 4096, 'logical_bytes': 4096, 'state': 'used',
                'blockers': ['retained_member'], 'retained_put_members': 1}
        samples = [{'sample_id': i, 'event': event, 'observed_start_ns': 2*i, 'observed_end_ns': 2*i+1,
                    'complete': True, 'reason': None, 'files': [file],
                    'rolled_files': [{'partition': 0, 'fencing_generation': 2, 'file_index': i-1}] if event == 'roll' else []}
                   for i, event in enumerate(events)]
        slope = {'partition': 0, 'reason': None, 'net_allocated_growth_bytes_per_day': -512}
        report = {'format_version': 1, 'run_id': 'r0-run', 'workload': 'sui_mainnet_retention', 'input_digest': SHA,
                  'database': 'af' * 16, 'wal_file_bytes': 4096, 'complete': True, 'reasons': [], 'owners': [0],
                  'samples': samples, 'sample_count': len(samples), 'prune_events': 1, 'file_roll_events': 3,
                  'value_wal_unlinks': 0, 'missed_events': 0, 'elapsed_ns': 100,
                  'worst_owner': slope, 'per_owner': [slope | {'baseline_allocated_bytes': 4096, 'end_allocated_bytes': 4096,
                                                               'new_allocated_bytes': 0, 'newly_shadow_reclaimed_bytes': 512}]}
        def read():
            return read_r0(path, run_id='r0-run', workload='sui_mainnet_retention', input_digest=SHA, min_rolls=3)
        write_json(path, report)
        self.assertEqual(read(), report)
        path.write_text(json.dumps(report)[:-1] + ', "samples": 6}')
        with self.assertRaisesRegex(ValueError, 'duplicate field'):
            read()
        for bad in (report | {'value_wal_unlinks': None}, report | {'missed_events': 1}, report | {'input_digest': '2' * 64}):
            write_json(path, bad)
            with self.assertRaisesRegex(ValueError, 'stale or incomplete'):
                read()
        write_json(path, report | {'sample_count': 5})
        with self.assertRaisesRegex(ValueError, 'sample window'):
            read()

    def test_r1_real_seal_and_rpc_result_do_not_replace_drain_trace(self):
        seal = {'start': 0, 'end': 4096, 'device': 91, 'inode': 5, 'root_generation': 8, 'last_batch_sequence': 12}
        proof = {'file_start': 0, 'inode': {'device': 91, 'inode': 5}, 'seal_generation': 8}
        for outcome in ('Completed', 'Busy'):
            self.assertEqual(verify_r1_retirement(seal, proof, outcome, file_start=0, file_bytes=4096), outcome)
        with self.assertRaisesRegex(ValueError, 'sealed WAL inode'):
            verify_r1_retirement(seal, proof | {'seal_generation': 7}, 'Completed', file_start=0, file_bytes=4096)


class HarnessFixesProducer(unittest.TestCase):
    def test_image_must_bind_current_recipe_and_exact_numeric_id(self):
        recipes = {'prepare_sha256': SHA, 'qualify_sha256': '2' * 64, 'scratch_sha256': '3' * 64}
        receipt = {'schema_version': 1, 'image': {'id': '23', 'path': 'projects/p/global/images/q1'}, 'recipes': recipes,
                   'runtime_sha256': SHA, 'evidence': [
                       {'bucket': 'q1', 'object': f'proof-{i}', 'generation': str(i), 'sha256': SHA} for i in (1, 2)]}
        verify_image_recipe(receipt, 23, recipes)
        with self.assertRaisesRegex(ValueError, 'refreshed Q1 recipe'):
            verify_image_recipe(receipt, 23, recipes | {'prepare_sha256': '4' * 64})
        with self.assertRaisesRegex(ValueError, 'this image'):
            verify_image_recipe(receipt, 24, recipes)

    def test_memory_uses_harnessfixes_measured_headroom_and_censoring(self):
        import gcp_runtime
        from hashlib import sha256

        root = temporary_directory(self)
        fixture = 'experiments/sui-mainnet/fixture/manifest.json'
        inputs = {fixture: write_json(root / fixture, {'format_version': 1})}
        jobs = [{'case_kind': 'mainnet_replay', 'case_id': 'cap', 'repeat': 0}]
        inputs['q1-replay-jobs.json'] = write_json(root / 'q1-replay-jobs.json', jobs)
        workload = sha256(json.dumps([inputs[fixture], {'case_kind': 'mainnet_replay'}],
                                    sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        mem = 'evidence/cgroup-memory.json'
        peak, cap = 24 * 1024**3, 32 * 1024**3
        evidence = {'admission': {'workload_sha256': workload, 'cap_bytes': cap, 'qualified_peak_bytes': None},
                    'measurement_completed': True, 'client': {'available': True, 'samples': [
                        {'memory.max': cap, 'memory.current': peak, 'memory.peak': peak,
                         'memory.events': {'max': 0, 'high': 0, 'oom': 0}}]}}
        policy = {'cap_mib': 32 * 1024, 'workloads': {workload: {'qualified_peak_bytes': peak, 'evidence': []}}}
        spec = {'inputs': inputs, 'cases': [{'id': CAPACITY}]}
        def seal():
            inputs[mem] = write_json(root / mem, evidence)
            policy['workloads'][workload]['evidence'] = [{'path': mem, 'sha256': inputs[mem]}]
            inputs['q1-replay-memory.json'] = write_json(root / 'q1-replay-memory.json', policy)
        def admit():
            return replay_memory_binding(root, spec, gcp_runtime.admit_replay_memory)
        seal()
        self.assertEqual(admit(), (32 * 1024, {workload: peak}))
        policy['cap_mib'] = 29 * 1024
        seal()
        with self.assertRaisesRegex(ValueError, '25% headroom'):
            admit()
        policy['cap_mib'] = 32 * 1024
        evidence['client']['samples'][0]['memory.events']['max'] = 1
        seal()
        with self.assertRaisesRegex(ValueError, 'censored'):
            admit()
        inputs.pop(mem)
        with self.assertRaisesRegex(ValueError, 'unsealed memory peak'):
            admit()
