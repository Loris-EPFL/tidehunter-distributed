"""Offline false-success gates; no AWS calls or provider/device assumptions."""
import copy
import csv
import json
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import checkpoint_matrix as matrix
import sui_checkpoint as supervisor
import test_sui_checkpoint_supervisor as fixtures
from test_sui_checkpoint_supervisor import example_config, traffic, completed_node

import checkpoint_kv

def completed_matrix(cfg):
    cells = []
    for planned in matrix.matrix_plan(cfg):
        cell = dict(planned, complete=True, config_sha256=f"{planned['ordinal'] + 1:064x}",
                    replay_evidence_sha256='d' * 64, resources_sha256='e' * 64,
                    namespace_ids={'perpetual': [planned['ordinal'] + 1] * 16}, node_archives={}, efa={})
        for key in ('build_manifest_sha256', 'binaries', 'fixture_bundle', 'verification_bundle',
                    'source_archive_sha256', 'source_manifest_sha256'):
            cell[key] = copy.deepcopy(cfg[key])
        for node in cfg['nodes']:
            cell['node_archives'][node['instance_id']] = dict(bucket=cfg['bucket'], sha256='f' * 64,
                key=f"{cfg['s3_prefix']}/results/cells/{cell['cell_id']}/storage/{node['partition']}/final/evidence.tar.gz")
        if cell['transport'] in ('efa', 'efa-direct', 'efa-native'):
            for instance_id in [cfg['coordinator_instance_id'], *(node['instance_id'] for node in cfg['nodes'])]:
                identity = dict(run_id=cfg['run_id'], cell_id=cell['cell_id'], instance_id=instance_id,
                    config_sha256=cell['config_sha256'], build_manifest_sha256=cfg['build_manifest_sha256'])
                cell['efa'][instance_id] = traffic(identity)
        cells.append(cell)
    return cells


class MatrixEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.cfg = example_config()
        self.cells = completed_matrix(self.cfg)

    def test_single_registered_profile_changes_only_trusted_native_cell_metadata(self):
        cfg = self.cfg | dict(transports=['tcp', 'efa', 'efa-direct', 'efa-native'])
        baseline = matrix.matrix_plan(cfg)
        registered = cfg | dict(kv_suite=dict(resident_reads=fixtures.resident_profile()))
        observed = matrix.matrix_plan(registered)
        self.assertEqual(len(observed), len(baseline))
        for before, after in zip(baseline, observed, strict=True):
            if before['transport'] == 'efa-native':
                self.assertEqual(after, before | dict(resident_reads=fixtures.resident_profile()))
            else:
                self.assertEqual(after, before)
        matrix.validate_matrix(registered, completed_matrix(registered))
        for index, cell in enumerate(observed):
            changed = completed_matrix(registered)
            if cell['transport'] == 'efa-native':
                changed[index].pop('resident_reads')
            else:
                changed[index]['resident_reads'] = fixtures.resident_profile()
            with self.subTest(transport=cell['transport']), self.assertRaises(ValueError):
                matrix.validate_matrix(registered, changed)

    def test_disabled_registered_profile_preserves_historical_matrix_shape(self):
        cfg = self.cfg | dict(transports=['tcp', 'efa-native'])
        baseline = matrix.matrix_plan(cfg)
        for profile in (None, matrix.resident_read_config()):
            observed = matrix.matrix_plan(cfg | dict(kv_suite=dict(resident_reads=profile)))
            self.assertEqual(observed, baseline)
            self.assertTrue(all('resident_reads' not in cell for cell in observed))

    def test_matrix_rejects_invalid_canonical_registered_profile_before_work(self):
        for profile in ({'destination_slots': 4}, [],
                        fixtures.resident_profile() | dict(require_device_read=1),
                        fixtures.resident_profile() | dict(max_read_bytes=1024**2)):
            cfg = self.cfg | dict(kv_suite=dict(resident_reads=profile))
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                matrix.matrix_plan(cfg)

    def test_local_fabric_windows_keep_peer_absence_and_nonwire_scope(self):
        records = [
            dict(schema=1, reason='profile-disable', window=1, data_posts=8,
                 credit_posts=2, piggyback_advances=3, peer=None),
            dict(schema=1, reason='session-drop', window=2, data_posts=4,
                 credit_posts=0, piggyback_advances=1, peer='127.0.0.1:1->127.0.0.1:2')]
        blob = b'ordinary stderr\\n\n' + b''.join(
            matrix.FABRIC_LOCAL_PREFIX + b' ' + json.dumps(record).encode() + b'\n' for record in records)
        result = matrix.parse_fabric_local_diagnostics(io.BytesIO(blob))
        self.assertEqual(result['records'], records)
        self.assertEqual(result['totals'], dict(data_posts=12, credit_posts=2, piggyback_advances=4))
        self.assertIsNone(result['records'][0]['peer'])
        matrix.validate_fabric_local_population(result, 'controller.stderr', required=True)
        with self.assertRaisesRegex(ValueError, 'clean'):
            matrix.validate_fabric_local_population(result, 'controller.stderr', forbidden=True)
        empty = matrix.parse_fabric_local_diagnostics(io.BytesIO(b'ordinary stderr\n'))
        self.assertFalse(empty['available'])
        with self.assertRaisesRegex(ValueError, 'required'):
            matrix.validate_fabric_local_population(empty, 'controller.stderr', required=True)
        self.assertIn('SIGTERM', empty['completeness'])

    def test_local_fabric_window_parser_rejects_corruption_and_bounds(self):
        record = dict(schema=1, reason='profile-reset', window=7, data_posts=1,
                      credit_posts=0, piggyback_advances=0, peer=None)
        def line(value):
            return matrix.FABRIC_LOCAL_PREFIX + b' ' + json.dumps(value).encode() + b'\n'
        for field, value in (('schema', True), ('window', 0), ('reason', 'unknown'),
                             ('window', 2**64), ('data_posts', -1),
                             ('credit_posts', True), ('piggyback_advances', 2**64),
                             ('peer', 'x' * 257)):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                matrix.parse_fabric_local_diagnostics(io.BytesIO(line(record | {field: value})))
        for blob in (line(record)[:-1], line(record) + line(record),
                     matrix.FABRIC_LOCAL_PREFIX + b' {\"schema\":1,\"schema\":1}\n'):
            with self.assertRaises(ValueError):
                matrix.parse_fabric_local_diagnostics(io.BytesIO(blob))
        with self.assertRaises(ValueError):
            matrix.parse_fabric_local_diagnostics(io.BytesIO(line(record)), max_bytes=32)
        with self.assertRaises(ValueError):
            matrix.parse_fabric_local_diagnostics(io.BytesIO(line(record)), max_records=0)
        with self.assertRaises(ValueError):
            matrix.parse_fabric_local_diagnostics(io.BytesIO(line(record | dict(data_posts=0))))

    def test_local_fabric_piggybacks_require_successful_data_posts(self):
        for data, credit, piggyback in ((0, 0, 1), (0, 1, 1), (1, 0, 2),
                                       (0, 1, 0), (1, 0, 1), (2, 1, 2)):
            with self.subTest(data=data, credit=credit, piggyback=piggyback):
                record = dict(schema=1, reason='profile-disable', window=1, data_posts=data,
                              credit_posts=credit, piggyback_advances=piggyback, peer=None)
                blob = matrix.FABRIC_LOCAL_PREFIX + b' ' + json.dumps(record).encode() + b'\n'
                if piggyback > data:
                    with self.assertRaises(ValueError):
                        matrix.parse_fabric_local_diagnostics(io.BytesIO(blob))
                else:
                    result = matrix.parse_fabric_local_diagnostics(io.BytesIO(blob))
                    self.assertEqual(result['records'], [record])


    def test_complete_counterbalanced_pairs_and_single_transport(self):
        self.assertEqual([cell['cell_id'] for cell in self.cells],
                         ['r01-tcp', 'r01-efa', 'r02-efa', 'r02-tcp',
                          'r03-tcp', 'r03-efa', 'r04-efa', 'r04-tcp'])
        self.assertEqual(len({cell['cell_id'] for cell in self.cells}), 8)
        self.assertEqual([cell['read_profile_limit'] for cell in self.cells],
                         [0, 0, 4096, 4096, 4096, 4096, 0, 0])
        matrix.validate_matrix(self.cfg, self.cells)
        for transport in ('tcp', 'efa', 'efa-direct', 'efa-native'):
            cfg = self.cfg | dict(transports=[transport], repetitions=1, read_profile_limits=[0],
                                  write_profile_limits=[0], native_write_profile_limits=[0])
            matrix.validate_matrix(cfg, completed_matrix(cfg))

    def test_cell_identity_accepts_four_repetitions_and_rejects_out_of_bounds(self):
        for repetition in range(1, 5):
            for transport in ('tcp', 'efa', 'efa-direct', 'efa-native'):
                matrix.check_cell(f'r{repetition:02d}-{transport}', transport)
        for cell_id in ('r00-tcp', 'r05-tcp', 'r99-tcp', 'r4-tcp', 'r004-tcp',
                        'r04-efa', 'r04-tcp-extra', 'r04-tcp\n', None, 4):
            with self.subTest(cell_id=cell_id), self.assertRaises(ValueError):
                matrix.check_cell(cell_id, 'tcp')
        with self.assertRaises(ValueError):
            matrix.check_cell('r04-fabric-tcp', 'fabric-tcp')

    def test_direct_modes_require_all_six_host_source_and_cell_proofs(self):
        for transport in ('efa-direct', 'efa-native'):
            cfg = self.cfg | dict(transports=['tcp', 'efa', transport])
            cells = completed_matrix(cfg)
            matrix.validate_matrix(cfg, cells)
            self.assertEqual([cell['transport'] for cell in cells[:6]],
                             ['tcp', 'efa', transport, transport, 'efa', 'tcp'])
            for host in cells[2]['efa']:
                for mutation in ('missing', 'source', 'cell', 'software'):
                    changed = copy.deepcopy(cells)
                    proof = changed[2]['efa'][host]
                    if mutation == 'missing':
                        del changed[2]['efa'][host]
                    elif mutation == 'software':
                        proof['after']['devices']['rdmap0s1']['driver'] = 'rxe'
                    else:
                        field = 'build_manifest_sha256' if mutation == 'source' else 'cell_id'
                        proof['before']['identity'][field] = 'foreign'
                    with self.subTest(transport=transport, host=host, mutation=mutation), self.assertRaises(ValueError):
                        matrix.validate_matrix(cfg, changed)
            for changed in (cells[:-1], cells + [cells[-1]]):
                with self.assertRaises(ValueError):
                    matrix.validate_matrix(cfg, changed)

    def test_exact_fabric_is_required_for_new_hardware_negotiation(self):
        for transport, fabric in (('efa', 'efa'), ('efa-direct', 'efa-direct'), ('efa-native', 'efa-direct')):
            text = f'provider: efa\n    fabric: {fabric}\n    type: FI_EP_RDM\n'
            matrix.validate_provider(text, transport)
            verbose = f'fi_info:\n    fi_ep_attr:\n        type: FI_EP_RDM\n    fi_fabric_attr:\n        name: {fabric}\n        prov_name: efa\n'
            matrix.validate_provider(verbose, transport)
            for invalid in (text.replace('fabric: ' + fabric, 'fabric: tcp'),
                            text.replace('    fabric: ' + fabric + '\n', ''),
                            text.replace('provider: efa', 'provider: efa-direct'),
                            text + 'provider: tcp\n    fabric: tcp\n    type: FI_EP_RDM\n',
                            text + 'provider: efa\n    type: FI_EP_RDM\n',
                            text.replace('FI_EP_RDM', 'FI_EP_MSG')):
                with self.subTest(transport=transport, invalid=invalid), self.assertRaises(ValueError):
                    matrix.validate_provider(invalid, transport)
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa\n type: FI_EP_RDM\n', 'efa-direct')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-direct\n type: FI_EP_RDM\n', 'efa')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-direct\n type: FI_EP_RDM\n')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa\n type: FI_EP_RDM\n', 'efa-native')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-native\n type: FI_EP_RDM\n', 'efa-native')

    def test_missing_duplicate_reordered_incomplete_cells_fail(self):
        cases = [self.cells[:-1], self.cells + [self.cells[0]], self.cells[::-1],
                 self.cells[:2] + [self.cells[1]] + self.cells[3:]]
        incomplete = copy.deepcopy(self.cells)
        incomplete[-1]['complete'] = False
        cases.append(incomplete)
        for cells in cases:
            with self.subTest(cells=cells), self.assertRaises(ValueError):
                matrix.validate_matrix(self.cfg, cells)

    def test_every_efa_host_requires_its_own_cell_bound_activity(self):
        for host in self.cells[1]['efa']:
            for mutation in ('missing', 'stale-cell', 'zero-tx', 'zero-rx', 'reset', 'software'):
                cells = copy.deepcopy(self.cells)
                proof = cells[1]['efa'][host]
                if mutation == 'missing':
                    del cells[1]['efa'][host]
                elif mutation == 'stale-cell':
                    proof['before']['identity']['cell_id'] = 'r02-efa'
                else:
                    device = proof['after']['devices']['rdmap0s1']
                    if mutation == 'software':
                        device['driver'] = 'rxe'
                    else:
                        counter = 'recv_bytes' if mutation == 'zero-rx' else 'send_bytes'
                        device['counters']['ports/1/hw_counters/' + counter] = 99 if mutation == 'reset' else 100
                with self.subTest(host=host, mutation=mutation), self.assertRaises(ValueError):
                    matrix.validate_matrix(self.cfg, cells)

    def test_old_namespace_config_archive_and_missing_pair_evidence_fail(self):
        for mutation in ('namespace', 'config', 'archive', 'replay', 'resources', 'fixture', 'profile'):
            cells = copy.deepcopy(self.cells)
            cell = cells[-1]
            if mutation == 'namespace':
                cell['namespace_ids'] = cells[0]['namespace_ids']
            elif mutation == 'config':
                cell['config_sha256'] = cells[0]['config_sha256']
            elif mutation == 'archive':
                cell['node_archives'] = cells[0]['node_archives']
            elif mutation == 'fixture':
                cell['fixture_bundle']['sha256'] = '0' * 64
            elif mutation == 'profile':
                cell['read_profile_limit'] += 1
            else:
                del cell['replay_evidence_sha256' if mutation == 'replay' else 'resources_sha256']
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                matrix.validate_matrix(self.cfg, cells)

    def test_matrix_bounds_reject_obsolete_or_ambiguous_configuration(self):
        for change in (dict(transport='tcp'), dict(transports=[]), dict(transports=['tcp', 'tcp']),
                       dict(transports=['fabric-tcp']), dict(repetitions=0), dict(repetitions=True),
                       dict(repetitions=5, read_profile_limits=[0] * 5), dict(read_profile_limit=0),
                       dict(read_profile_limits=[]), dict(read_profile_limits=[0]),
                       dict(read_profile_limits=[0, 4096, 4096]), dict(read_profile_limits=[0, True, 4096, 0]),
                       dict(read_profile_limits=[False, 4096, 4096, 0]), dict(read_profile_limits=[0, -1, 4096, 0]),
                       dict(read_profile_limits=[0, 65537, 4096, 0]), dict(read_profile_limits=[0, 1.0, 4096, 0]),
                       dict(read_profile_limits='0,4096,4096,0'), dict(read_profile_limits=None),
                       dict(write_profile_limit=128), dict(write_profile_limits=[]),
                       dict(write_profile_limits=[0, 128]), dict(write_profile_limits=[0, True, 128, 0]),
                       dict(write_profile_limits=[0, -1, 128, 0]), dict(write_profile_limits=[0, 65537, 128, 0]),
                       dict(write_profile_limits=[0, 1.0, 128, 0]), dict(write_profile_limits=None),
                       dict(native_write_profile_limits=[640]), dict(native_write_profile_limits=[0, True, 640, 0]),
                       dict(native_write_profile_limits=[0, 65537, 640, 0])):
            with self.subTest(change=change), self.assertRaises(ValueError):
                supervisor.validate_cfg(self.cfg | change)
        missing = self.cfg.copy()
        del missing['read_profile_limits']
        with self.assertRaises(ValueError):
            supervisor.validate_cfg(missing)

    def test_kv_scope_rejects_unknown_or_ambiguous_requests(self):
        for options in (
                {'repetitions': True, 'diagnostics': False, 'scope': 'targeted'},
                {'repetitions': 1, 'diagnostics': 0, 'scope': 'targeted'},
                {'repetitions': 1, 'diagnostics': False, 'scope': 'unknown'},
                {'repetitions': 1, 'diagnostics': False, 'scope': 'targeted', 'skip_efa': True},
                {'scope': 'targeted'}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                checkpoint_kv.validate_options(options)

    def test_explicit_direct_requires_hardware_admission_without_legacy_schedule_changes(self):
        from guest import Campaign, efa_cases
        campaign = Campaign.__new__(Campaign)
        campaign.cfg = dict(campaign='efa-cache', kv_subsuite=True, efa=False)
        with self.assertRaisesRegex(ValueError, 'EFA-enabled build and fleet'):
            campaign.run_case('current', (256, 1, 1), 0, storage=True, transport='efa-direct')
        self.assertEqual({spec[3].get('transport', 'tcp') for spec in efa_cases()}, {'tcp', 'efa'})
        options = dict(repetitions=1, diagnostics=False, scope='targeted')
        planned = checkpoint_kv.plan(options, ['tcp', 'efa', 'efa-direct'])
        self.assertEqual(len(planned), 36)
        for transport in ('tcp', 'efa', 'efa-direct'):
            rows = [spec for spec in planned if spec['transport'] == transport]
            self.assertEqual(len(rows), 12)
            self.assertEqual(sum(spec['transport_floor'] for spec in rows), 1)

    def diagnostic_fixture(self, root, transport='efa-native', mode='tcp', cold=True,
                           shape=(256, 1, 1), reads=256):
        directory = Path(root)
        spec = dict(shape=list(shape), repetition=3, transport=transport,
                    cold=cold, diagnostics=True, cached=False, authority_resident_batches=None,
                    workload_reads=reads)
        native = mode == 'native-sync'
        identity = dict(backend='native_value_wal_barrier' if native else matrix.TRANSPORT_BACKENDS[transport],
                        value_bytes=shape[0], batch_size=shape[1], window=shape[2], repetition=0)
        def table(filename, fields, applicable):
            states = ['warm'] + (['cache-evicted'] if cold else []) + [
                'reopened-warm' if native else 'reopened-cache-cleared']
            with (directory / filename).open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=[*identity, 'state', 'sample', *fields])
                writer.writeheader()
                if applicable:
                    for state in states:
                        for sample in range(spec['workload_reads']):
                            values = dict.fromkeys(fields, 0)
                            values.update(index_ns=1, whole_call_ns=100)
                            if native:
                                values.update(wal_and_decode_ns=2, value_cache_hit='false')
                            else:
                                values.update(cache_misses=1, exchange_ns=10, server_record_read_ns=2,
                                              wire_read_requests=1, server_timing_available='true',
                                              server_response_recheck_samples=1)
                            writer.writerow(identity | dict(state=state, sample=sample) | values)
        table('native_read_profiles.csv',
              ['index_ns', 'wal_and_decode_ns', 'value_cache_hit', 'whole_call_ns'], native)
        table('remote_read_profiles.csv', [
            'index_ns', 'dispatch_ns', 'wait_ns', 'actor_queue_ns', 'actor_service_ns', 'actor_return_ns',
            'request_encode_ns', 'exchange_ns', 'response_decode_ns', 'record_verify_ns',
            'server_handler_ns', 'server_inventory_wait_ns', 'server_authority_lookup_ns',
            'server_record_read_ns', 'cache_hits', 'cache_misses', 'whole_call_ns', 'inline_read_ns',
            'server_response_recheck_wait_ns', 'server_response_recheck_ns',
            'server_response_recheck_samples', 'server_response_recheck_deferrals',
            'wire_read_requests', 'server_timing_available'], not native)
        phases = ['load-write-diagnostics',
                  'warm-profiled-read-diagnostics', 'warm-window-read-diagnostics',
                  'warm-batched-read-diagnostics']
        if cold:
            phases += ['cache_evicted_get-diagnostics', 'cache_evicted_get_many-diagnostics',
                       'cache_evicted_get_window-diagnostics', 'cache-evicted-read-diagnostics']
        phases += ['reopened-read-diagnostics']
        transport_profile = dict.fromkeys(
            ('cq_polls', 'cq_completions', 'cq_call_ns', 'cq_timed_calls',
             'cq_dispatch_ns', 'cq_dispatch_calls'), 1)
        for name in ('tls_read', 'tls_write', 'tls_process'):
            transport_profile[name] = dict.fromkeys(('ns', 'calls', 'bytes', 'errors'), 0)
        profile = dict(requests=1, request_decode_ns=1, handler_ns=1, response_encode_ns=1,
                       response_send_ns=1, append_wal_ns=1, append_persist_ns=1,
                       read_response_recheck_samples=1, read_response_recheck_ns=1,
                       receive_frame_including_idle_ns=0,
                       storage_write=dict(append_calls=1, persist_calls=1),
                       client=dict(requests=1, exchange_ns=1, server_handler_ns=1,
                                   server_record_read_ns=1, server_inventory_wait_ns=0),
                       polling=dict(progress_sweeps=1, request_first_poll_samples=1),
                       transport=transport_profile, client_transport=transport_profile)
        records = [] if native else [
            identity | dict(phase=phase, nodes=[[partition, copy.deepcopy(profile)] for partition in range(5)])
            for phase in phases]
        for record in records:
            if record['phase'] not in ('warm-profiled-read-diagnostics',
                                       'cache-evicted-read-diagnostics', 'reopened-read-diagnostics'):
                for _, node in record['nodes']:
                    node['client']['server_handler_ns'] = 0
                    node['client']['server_record_read_ns'] = 0
        (directory / 'node_profiles.yaml').write_text(
            ''.join('---\n' + json.dumps(record) + '\n' for record in records))
        return spec, records

    def test_diagnostic_profiles_admit_actual_backend_schemas_and_structural_zeros(self):
        for transport in ('tcp', 'efa', 'efa-direct', 'efa-native'):
            for mode in ('tcp', 'native-sync'):
                for cold in (False, True):
                    with self.subTest(transport=transport, mode=mode, cold=cold), tempfile.TemporaryDirectory() as root:
                        spec, _ = self.diagnostic_fixture(root, transport, mode, cold)
                        checkpoint_kv.validate_diagnostic_evidence(root, spec, mode, list(range(5)))

    def test_current_native_diagnostic_availability_metadata_keeps_strict_clock_and_numeric_admission(self):
        for mutation in (None, 'metadata_type', 'unknown_boolean', 'zero_clock', 'boolean_counter'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                spec, records = self.diagnostic_fixture(root, transport='fabric-tcp-native',
                    cold=False, shape=(8192, 32, 1), reads=64)
                for row in records:
                    for _, node in row['nodes']:
                        profiled = row['phase'] in ('warm-profiled-read-diagnostics', 'reopened-read-diagnostics')
                        node['client'].update(server_handler_available=profiled,
                                              server_handler_not_applicable=row['phase'] != 'load-write-diagnostics')
                node = records[1]['nodes'][0][1]
                if mutation == 'metadata_type':
                    node['client']['server_handler_available'] = 'true'
                elif mutation == 'unknown_boolean':
                    node['client']['handler_available_unknown'] = True
                elif mutation == 'zero_clock':
                    node['client']['server_record_read_ns'] = 0
                elif mutation == 'boolean_counter':
                    node['client']['requests'] = True
                path = Path(root) / 'node_profiles.yaml'
                path.write_text(''.join('---\n' + json.dumps(record) + '\n' for record in records))
                retained = path.read_text()
                if mutation is None:
                    checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))
                    self.assertEqual(path.read_text(), retained)
                    self.assertIs(records[1]['nodes'][0][1]['client']['server_handler_available'], True)
                    self.assertIs(records[1]['nodes'][0][1]['client']['server_handler_not_applicable'], True)
                else:
                    with self.assertRaises(ValueError):
                        checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))

    def test_diagnostic_cold_samples_cannot_stop_at_the_local_index(self):
        with tempfile.TemporaryDirectory() as root:
            spec, _ = self.diagnostic_fixture(root)
            path = Path(root) / 'remote_read_profiles.csv'
            with path.open() as stream:
                reader = csv.DictReader(stream)
                fields, rows = reader.fieldnames, list(reader)
            cold = next(row for row in rows if row['state'] == 'cache-evicted')
            cold.update(cache_misses='0', exchange_ns='0', server_record_read_ns='0')
            with path.open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaises(ValueError):
                checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))

    def test_diagnostic_idle_owners_are_not_missing_active_owner_profiles(self):
        def idle(profile):
            return {key: idle(value) if isinstance(value, dict) else 0
                    for key, value in profile.items()}
        for transport in ('tcp', 'efa', 'efa-direct', 'efa-native'):
            with self.subTest(transport=transport), tempfile.TemporaryDirectory() as root:
                spec, records = self.diagnostic_fixture(root, transport=transport)
                path = Path(root) / 'node_profiles.yaml'
                owner = records[1]['nodes'][0]
                owner[1] = idle(owner[1])
                def save():
                    path.write_text(''.join('---\n' + json.dumps(row) + '\n' for row in records))
                save()
                checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))
                owner[1]['client']['requests'] = 1
                save()
                with self.assertRaises(ValueError):
                    checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))
                owner[1]['requests'] = 1
                save()
                with self.assertRaises(ValueError):
                    checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))

    def test_diagnostic_active_owner_clocks_cannot_hide_in_fleet_totals(self):
        for field in ('handler_ns', 'append_persist_ns', 'cq_timed_calls'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                spec, records = self.diagnostic_fixture(root)
                profile = records[0]['nodes'][0][1]
                if field == 'cq_timed_calls':
                    profile['transport'][field] = 0
                else:
                    profile[field] = 0
                path = Path(root) / 'node_profiles.yaml'
                path.write_text(''.join('---\n' + json.dumps(row) + '\n' for row in records))
                with self.assertRaises(ValueError):
                    checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))

    def test_diagnostic_load_and_drained_owner_populations_must_be_complete(self):
        for change in ('idle_load', 'truncated_server', 'truncated_client'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                spec, records = self.diagnostic_fixture(root)
                profile = records[0 if change == 'idle_load' else 1]['nodes'][0][1]
                if change == 'idle_load':
                    profile['requests'] = profile['client']['requests'] = 0
                elif change == 'truncated_server':
                    profile['client']['requests'] = 2
                else:
                    profile['requests'] = 2
                path = Path(root) / 'node_profiles.yaml'
                path.write_text(''.join('---\n' + json.dumps(row) + '\n' for row in records))
                with self.assertRaises(ValueError):
                    checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))

    def test_diagnostic_profiles_reject_missing_malformed_and_tampered_populations(self):
        for change in ('missing', 'malformed', 'phase', 'duplicate_phase', 'owner', 'repetition',
                       'backend', 'counter', 'clock', 'read_counter', 'read_clock',
                       'cq', 'tls', 'negative', 'bool'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                spec, records = self.diagnostic_fixture(root)
                path = Path(root) / 'node_profiles.yaml'
                if change == 'missing':
                    path.unlink()
                elif change == 'malformed':
                    path.write_text('---\n{broken')
                else:
                    if change == 'phase':
                        records.pop()
                    elif change == 'duplicate_phase':
                        records[-1] = copy.deepcopy(records[0])
                    elif change == 'owner':
                        records[0]['nodes'][-1][0] = 0
                    elif change in ('backend', 'repetition'):
                        records[0][change] = 'foreign' if change == 'backend' else 3
                    elif change in ('read_counter', 'read_clock'):
                        for _, profile in records[1]['nodes']:
                            if change == 'read_counter':
                                profile['read_response_recheck_samples'] = 0
                            else:
                                profile['client']['server_record_read_ns'] = 0
                    else:
                        for _, profile in records[0]['nodes']:
                            if change == 'counter':
                                profile['storage_write']['persist_calls'] = 0
                            elif change == 'clock':
                                del profile['handler_ns']
                            elif change == 'cq':
                                profile['transport']['cq_timed_calls'] = 0
                            elif change == 'tls':
                                profile['client_transport']['tls_read']['calls'] = 1
                            else:
                                profile['requests'] = -1 if change == 'negative' else True
                    path.write_text(''.join('---\n' + json.dumps(record) + '\n' for record in records))
                with self.assertRaises((ValueError, FileNotFoundError)):
                    checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))

    def test_diagnostic_read_profiles_reject_false_complete_rows(self):
        for mode in ('tcp', 'native-sync'):
            for change in ('missing', 'schema', 'sample', 'state', 'backend', 'repetition',
                           'whole', 'nested', 'negative', 'missing_clock', 'duplicate'):
                with self.subTest(mode=mode, change=change), tempfile.TemporaryDirectory() as root:
                    spec, _ = self.diagnostic_fixture(root, mode=mode)
                    path = Path(root) / ('native_read_profiles.csv' if mode == 'native-sync' else 'remote_read_profiles.csv')
                    with path.open() as stream:
                        reader = csv.DictReader(stream)
                        fields, rows = reader.fieldnames, list(reader)
                    if change == 'missing':
                        path.unlink()
                    else:
                        if change == 'schema':
                            fields.remove('index_ns')
                            for row in rows:
                                del row['index_ns']
                        elif change == 'duplicate':
                            rows.append(rows[-1])
                        else:
                            key, value = {
                                'sample': ('sample', '1'), 'state': ('state', 'reopened'),
                                'backend': ('backend', 'experimental_tcp_authority'),
                                'repetition': ('repetition', '3'), 'whole': ('whole_call_ns', '0'),
                                'nested': ('index_ns', '101'), 'negative': ('index_ns', '-1'),
                                'missing_clock': ('index_ns', '')}[change]
                            rows[0][key] = value
                        with path.open('w') as stream:
                            writer = csv.DictWriter(stream, fieldnames=fields)
                            writer.writeheader()
                            writer.writerows(rows)
                    with self.assertRaises((ValueError, FileNotFoundError)):
                        checkpoint_kv.validate_diagnostic_evidence(root, spec, mode, list(range(5)))

    def test_diagnostic_missing_profiles_cannot_pass_complete_marker(self):
        with tempfile.TemporaryDirectory() as root:
            spec = checkpoint_kv.plan(dict(repetitions=1, diagnostics=True), ['tcp'])[0]
            directory = Path(root) / 'case-native-sync'
            directory.mkdir()
            (directory / 'COMPLETE').touch()
            cfg = dict(kv_suite=dict(repetitions=1, diagnostics=True), transports=['tcp'],
                       binaries={'storage-node': 'a', 'transport-floor': 'b'},
                       nodes=[dict(partition=p) for p in range(5)])
            state = dict(plan=[spec], completed_cases=[], binary_sha256='a', floor_binary_sha256='b')
            with patch.object(checkpoint_kv, 'plan', return_value=[spec]), patch.object(
                    checkpoint_kv, 'completed_case_ids', return_value=['case']):
                with self.assertRaises(FileNotFoundError):
                    checkpoint_kv.validate_auxiliary(root, state, cfg)

    def test_full_diagnostics_are_first_without_changing_clean_order_or_controls(self):
        transports = ['tcp', 'efa', 'efa-direct', 'efa-native']
        options = dict(scope='full', repetitions=3, diagnostics=False)
        clean = checkpoint_kv.plan(options, transports)
        combined = checkpoint_kv.plan(options | dict(diagnostics=True), transports)
        self.assertEqual(len(combined), 172)
        self.assertTrue(all(case['diagnostics'] for case in combined[:16]))
        self.assertEqual(combined[16:], clean)
        self.assertEqual(sum(case.get('transport_floor', False) for case in clean), 4)
        self.assertEqual([(case['transport'], case['shape']) for case in combined[:16]],
                         [(transport, shape) for transport in transports for shape in
                          ([256, 1, 1], [256, 32, 32], [8192, 1, 1], [8192, 32, 32])])

    def test_targeted_diagnostics_leave_clean_comparison_populations_unchanged(self):
        for repetitions in (1, 3):
            for transports in (['tcp'], ['tcp', 'efa'], ['tcp', 'efa', 'efa-direct']):
                with self.subTest(repetitions=repetitions, transports=transports):
                    options = dict(scope='targeted', repetitions=repetitions, diagnostics=False)
                    clean = checkpoint_kv.plan(options, transports)
                    combined = checkpoint_kv.plan(options | dict(diagnostics=True), transports)
                    self.assertEqual([case for case in combined if not case['diagnostics']], clean)
                    profiled = [case for case in combined if case['diagnostics']]
                    self.assertEqual({case['transport'] for case in profiled}, set(transports))
                    self.assertTrue(all(case['repetition'] > max(row['repetition'] for row in clean)
                                        for case in profiled))

    def test_full_plan_retains_shapes_and_separates_repeated_clean_controls(self):
        transports = ['tcp', 'efa', 'efa-direct']
        options = dict(scope='full', repetitions=3, diagnostics=True)
        planned = checkpoint_kv.plan(options, transports)
        self.assertEqual(len(planned), 129)
        self.assertEqual(sum(not spec['diagnostics'] for spec in planned), 117)
        for transport in transports:
            clean = [spec for spec in planned if spec['transport'] == transport and not spec['diagnostics']]
            for repetition in range(3):
                rows = [spec for spec in clean if spec['repetition'] == repetition]
                self.assertEqual(len(rows), 13)
                self.assertEqual({tuple(spec['shape']) for spec in rows},
                                 {(256, 1, 1), (256, 32, 32), (8192, 1, 1), (8192, 32, 32)})
                tails = [spec for spec in rows if spec.get('workload_batches') == 3200]
                self.assertEqual(len(tails), 1)
                self.assertFalse(tails[0]['cold'] or tails[0]['cached'])
                self.assertEqual(tails[0]['workload_batches'] // tails[0]['shape'][2], 100)
                history = (3200 + 2 * 64) * 32 * 8192
                self.assertLessEqual(history, 1024**3)
                hits = [spec for spec in rows if spec.get('cache_hit_contention')]
                self.assertEqual({spec['shape'][0] for spec in hits}, {256, 8192})
                self.assertTrue(all(spec['cached'] and spec['shape'][1] == 1 for spec in hits))
            floors = [spec for spec in clean if spec.get('transport_floor')]
            self.assertEqual(len(floors), 1)
            self.assertEqual(floors[0]['repetition'], 0)
        self.assertTrue(all(spec['repetition'] == 3 for spec in planned if spec['diagnostics']))

    def test_completed_pairs_reject_reused_repetitions_and_workload_families(self):
        planned = [dict(shape=[256, 1, 1], repetition=rep, transport='tcp',
            cold=False, diagnostics=False, cached=cached, authority_resident_batches=None)
            for rep, cached in ((0, True), (1, True), (1, False))]
        completed = []
        for case_id, variant, modes in (
                ('cache-current-v256-b1-w1-r0', 'cache', ('native-sync', 'tcp')),
                ('cache-current-v256-b1-w1-r1', 'cache', ('tcp', 'native-sync')),
                ('storage-current-v256-b1-w1-r1', 'storage', ('native-sync', 'tcp'))):
            completed.extend(dict(case_id=case_id, variant=variant, backend=mode,
                build='current', transport='tcp' if mode == 'tcp' else 'local') for mode in modes)
        checkpoint_kv.completed_case_ids(planned, completed)
        for source, destination in ((0, 2), (2, 4)):
            changed = copy.deepcopy(completed)
            changed[destination:destination + 2] = copy.deepcopy(completed[source:source + 2])
            with self.subTest(source=source, destination=destination), self.assertRaises(ValueError):
                checkpoint_kv.completed_case_ids(planned, changed)
        for field, value in (('build', 'baseline'), ('variant', 'cold'), ('transport', 'efa')):
            changed = copy.deepcopy(completed)
            changed[2][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                checkpoint_kv.completed_case_ids(planned, changed)

    def test_completed_pairs_bind_direct_transport_and_storage_layout(self):
        planned = [dict(shape=[8192, 32, 32], repetition=2, transport='efa-direct',
            cold=False, diagnostics=False, cached=False, authority_resident_batches=None,
            shard_cache_bytes=64 * 1024**2, preparation_chunk_batches=32, wal_layout='sui')]
        variant = 'wal-sui-preparation32-shardcache67108864-storage-efa-direct'
        case_id = variant + '-current-v8192-b32-w32-r2'
        completed = [dict(case_id=case_id, variant=variant, backend=mode, build='current',
            transport='efa-direct' if mode == 'tcp' else 'local') for mode in ('tcp', 'native-sync')]
        checkpoint_kv.completed_case_ids(planned, completed)
        changed = copy.deepcopy(completed)
        changed[0]['transport'] = 'efa'
        with self.assertRaises(ValueError):
            checkpoint_kv.completed_case_ids(planned, changed)
        changed = copy.deepcopy(completed)
        for row in changed:
            row['case_id'] = row['case_id'].removeprefix('wal-sui-')
            row['variant'] = row['variant'].removeprefix('wal-sui-')
        with self.assertRaises(ValueError):
            checkpoint_kv.completed_case_ids(planned, changed)

    def test_cache_contention_requires_real_overlap_and_exact_hit_population(self):
        spec = dict(shape=[8192, 1, 1], transport='efa-direct')
        rows = []
        for callers in (1, 8):
            intervals = [[round * 1000 + index * 10, round * 1000 + index * 10 + 5]
                         for round in range(32) for index in range(32)]
            writes = [[round * 1000, round * 1000 + 500] for round in range(32)]
            rows.append(dict(api='distributed::sui::Db::get', value_bytes=8192,
                backend='experimental_efa_direct_authority',
                batch_size=1, window=1, repetition=0,
                transport=checkpoint_kv.metadata_transport_label('efa-direct'),
                observation=dict(callers=callers, reads_per_caller=1024,
                    concurrent_durable_writes=32, phase_wall_ns=32000,
                    get_api_intervals_ns_by_caller=[copy.deepcopy(intervals) for _ in range(callers)],
                    write_api_intervals_ns=writes, overlapping_reads_by_caller=[1024] * callers,
                    remote_cache_counter_available=True, remote_hits=callers * 1024,
                    remote_misses=0, remote_read_rpcs=0)))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'cache_contention.jsonl'
            def save(values):
                path.write_text(''.join(json.dumps(row) + '\n' for row in values))
            save(rows)
            checkpoint_kv.validate_contention(path, spec, 'tcp')
            for label in ('efa-direct', 'ofi-efa-direct-trusted'):
                changed = copy.deepcopy(rows)
                changed[0]['transport'] = label
                save(changed)
                with self.subTest(label=label), self.assertRaises(ValueError):
                    checkpoint_kv.validate_contention(path, spec, 'tcp')
            for field, value in (('remote_misses', 1), ('remote_read_rpcs', 1),
                                 ('remote_hits', 1023), ('remote_cache_counter_available', False)):
                changed = copy.deepcopy(rows)
                changed[0]['observation'][field] = value
                save(changed)
                with self.subTest(field=field), self.assertRaises(ValueError):
                    checkpoint_kv.validate_contention(path, spec, 'tcp')
            changed = copy.deepcopy(rows)
            changed[0]['observation']['write_api_intervals_ns'] = [
                [round * 1000 + 600, round * 1000 + 900] for round in range(32)]
            changed[0]['observation']['overlapping_reads_by_caller'] = [0]
            save(changed)
            with self.assertRaisesRegex(ValueError, 'overlap'):
                checkpoint_kv.validate_contention(path, spec, 'tcp')
            save(rows[:-1])
            with self.assertRaises(ValueError):
                checkpoint_kv.validate_contention(path, spec, 'tcp')

    def test_async_submission_is_not_physical_device_io_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            fields = ['phase', 'role', 'partition', 'boundary', 'cache_hits', 'cache_misses',
                      'wal_reads', 'resident_read_successes', 'async_read_submissions']
            with (directory / 'read_path_evidence.csv').open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                for partition in range(5):
                    for boundary in ('before', 'after'):
                        writer.writerow(dict(phase='point_get', role='storage', partition=partition,
                            boundary=boundary, cache_hits=0, cache_misses=0,
                            wal_reads=8 if boundary == 'after' else 0,
                            resident_read_successes=0,
                            async_read_submissions=8 if boundary == 'after' else 0))
            def physical(values):
                with (directory / 'read_io.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=['phase', 'partition', 'physical_read_bytes'])
                    writer.writeheader()
                    for partition, value in enumerate(values):
                        writer.writerow(dict(phase='point_get', partition=partition, physical_read_bytes=value))
            physical([])
            evidence = checkpoint_kv.read_service_evidence(directory, 'tcp')['point_get']
            self.assertEqual(evidence['owner_async_submissions'], 40)
            self.assertIsNone(evidence['owner_resident_read_deferred'])
            self.assertTrue(evidence['owner_counters_available'])
            self.assertFalse(evidence['async_submissions_prove_device_io'])
            self.assertIsNone(evidence['charged_physical_read_bytes'])
            self.assertEqual(evidence['physical_io_scope'], 'unavailable')
            physical([0] * 5)
            evidence = checkpoint_kv.read_service_evidence(directory, 'tcp')['point_get']
            self.assertEqual(evidence['charged_physical_read_bytes'], 0)
            physical([4096, 0, 0, 0, 0])
            evidence = checkpoint_kv.read_service_evidence(directory, 'tcp')['point_get']
            self.assertEqual(evidence['charged_physical_read_bytes'], 4096)
            self.assertEqual(evidence['physical_io_scope'], 'positive_process_interval_IO_not_per_value_device_latency')

    def test_owner_counter_availability_requires_exact_configured_population(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            fields = ['phase', 'role', 'partition', 'boundary', 'cache_hits', 'cache_misses',
                      'wal_reads', 'resident_read_successes', 'async_read_submissions']
            def capture(owners, physical):
                with (directory / 'read_path_evidence.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    for partition in owners:
                        for boundary in ('before', 'after'):
                            writer.writerow(dict(phase='point_get', role='storage', partition=partition,
                                boundary=boundary, cache_hits=0, cache_misses=0, wal_reads=0,
                                resident_read_successes=0, async_read_submissions=0))
                with (directory / 'read_io.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=['phase', 'partition', 'physical_read_bytes'])
                    writer.writeheader()
                    for partition in physical:
                        writer.writerow(dict(phase='point_get', partition=partition, physical_read_bytes=4096))
            capture([], [0])
            native = checkpoint_kv.read_service_evidence(directory, 'native-sync')['point_get']
            self.assertFalse(native['owner_counters_available'])
            self.assertIsNone(native['owner_resident_read_successes'])
            self.assertIsNone(native['owner_async_submissions'])
            self.assertEqual(native['charged_physical_read_bytes'], 4096)
            capture([], range(5))
            remote = checkpoint_kv.read_service_evidence(directory, 'tcp')['point_get']
            self.assertFalse(remote['owner_counters_available'])
            self.assertIsNone(remote['owner_resident_read_successes'])
            self.assertIsNone(remote['owner_async_submissions'])
            capture(range(4), range(5))
            with self.assertRaisesRegex(ValueError, 'configured owner'):
                checkpoint_kv.read_service_evidence(directory, 'tcp')
            capture([2, 3], [2, 3])
            configured = checkpoint_kv.read_service_evidence(
                directory, 'tcp', owner_partitions=[2, 3])['point_get']
            self.assertTrue(configured['owner_counters_available'])
            self.assertEqual(configured['owner_resident_read_successes'], 0)
            self.assertEqual(configured['owner_async_submissions'], 0)
            capture([2, 4], [2, 3])
            with self.assertRaisesRegex(ValueError, 'configured owner'):
                checkpoint_kv.read_service_evidence(directory, 'tcp', owner_partitions=[2, 3])

    def test_resident_budget_deferrals_are_not_cache_hits_or_device_io(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            fields = ['phase', 'role', 'partition', 'boundary', 'cache_hits', 'cache_misses',
                      'wal_reads', 'resident_read_successes', 'async_read_submissions',
                      'resident_read_deferred']
            with (directory / 'read_path_evidence.csv').open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                for partition in range(5):
                    for boundary in ('before', 'after'):
                        after = boundary == 'after'
                        writer.writerow(dict(phase='point_get', role='storage', partition=partition,
                            boundary=boundary, cache_hits=0, cache_misses=3 if after else 0,
                            wal_reads=3 if after else 0, resident_read_successes=1 if after else 0,
                            async_read_submissions=2 if after else 0,
                            resident_read_deferred=2 if after else 0))
            (directory / 'read_io.csv').write_text('phase,partition,physical_read_bytes\n')
            evidence = checkpoint_kv.read_service_evidence(directory, 'tcp')['point_get']
            self.assertEqual(evidence['owner_resident_read_successes'], 5)
            self.assertEqual(evidence['owner_async_submissions'], 10)
            self.assertEqual(evidence['owner_resident_read_deferred'], 10)
            self.assertEqual(sum(owner['cache_hits'] for owner in evidence['owners'].values()), 0)
            self.assertIsNone(evidence['charged_physical_read_bytes'])
            self.assertFalse(evidence['async_submissions_prove_device_io'])

    def test_checkpoint_records_join_completion_and_pressure_without_invented_spans(self):
        spec = dict(diagnostics=True, transport='fabric-tcp')
        identity = dict(backend='experimental_fabric_tcp_authority',
                        transport=checkpoint_kv.metadata_transport_label('fabric-tcp'),
                        phase='load_write_window', sample=0, value_bytes=8192,
                        batch_size=32, window=32, repetition=0)
        cut = dict(batch=dict(epoch=3, sequence=17), operation_index=31)
        complete = dict(cursor=0, epoch=3, generation=2, checkpoint_generation=None,
                        trigger=None, outcome=None, pressure_wait_ns=None,
                        requested_cut=None, captured_cut=cut, staging_manifest_ns=11)
        pressure = dict(cursor=1, epoch=3, generation=None, checkpoint_generation=2,
                        trigger='IndexReservation', outcome='AfterExistingCapture',
                        pressure_wait_ns=100, requested_cut=cut, captured_cut=cut,
                        staging_manifest_ns=None, existing_capture_wait_ns=75)
        row = identity | dict(diagnostic_available=True, cursor_before=0, record_count=2,
            snapshot=dict(next_cursor=2, dropped_records=0, records=[complete, pressure]))
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with (directory / 'write_windows.csv').open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=list(identity))
                writer.writeheader()
                writer.writerow(identity)
            path = directory / 'checkpoint_diagnostics.jsonl'
            path.write_text(json.dumps(row) + '\n')
            checkpoint_kv.validate_checkpoint_records(directory, spec, 'tcp')
            changed = row | dict(transport='fabric-tcp')
            path.write_text(json.dumps(changed) + '\n')
            with self.assertRaisesRegex(ValueError, 'exact timed-window identity'):
                checkpoint_kv.validate_checkpoint_records(directory, spec, 'tcp')
            path.write_text(json.dumps(row) + '\n')
            for records in ([], [complete], [complete, complete]):
                changed = copy.deepcopy(row)
                changed['record_count'] = len(records)
                changed['snapshot']['records'] = records
                path.write_text(json.dumps(changed) + '\n')
                with self.assertRaisesRegex(ValueError, 'retained suffix'):
                    checkpoint_kv.validate_checkpoint_records(directory, spec, 'tcp')
            # dropped_records is global. A late window need not start at zero,
            # and requesting an older cut returns exactly the bounded suffix.
            for before, retained in ((195, range(195, 200)), (70, range(72, 200)), (200, ())):
                retained = [pressure | dict(cursor=cursor) for cursor in retained]
                changed = row | dict(cursor_before=before, record_count=len(retained),
                    snapshot=dict(next_cursor=200, dropped_records=72, records=retained))
                path.write_text(json.dumps(changed) + '\n')
                checkpoint_kv.validate_checkpoint_records(directory, spec, 'tcp')
            for next_cursor, dropped in ((2, 1), (2**64 - 1, 0)):
                changed = row | dict(record_count=0, snapshot=dict(
                    next_cursor=next_cursor, dropped_records=dropped, records=[]))
                path.write_text(json.dumps(changed) + '\n')
                with self.assertRaisesRegex(ValueError, 'cardinality'):
                    checkpoint_kv.validate_checkpoint_records(directory, spec, 'tcp')
            changed = copy.deepcopy(row)
            changed['snapshot']['records'][0]['pressure_wait_ns'] = 0
            path.write_text(json.dumps(changed) + '\n')
            with self.assertRaisesRegex(ValueError, 'completion'):
                checkpoint_kv.validate_checkpoint_records(directory, spec, 'tcp')
            changed = copy.deepcopy(row)
            changed['snapshot']['records'][1]['captured_cut'] = dict(partition=0, offset=123)
            path.write_text(json.dumps(changed) + '\n')
            with self.assertRaisesRegex(ValueError, 'logical cut'):
                checkpoint_kv.validate_checkpoint_records(directory, spec, 'tcp')
            path.write_text(json.dumps(row) + '\n')
            with self.assertRaisesRegex(ValueError, 'clean/native'):
                checkpoint_kv.validate_checkpoint_records(directory, spec | dict(diagnostics=False), 'tcp')

    def test_cache_schedules_are_bound_to_each_cell_and_cannot_be_forged(self):
        cfg = self.cfg | dict(shard_cache_mib_by_repetition=[0, 16, 16, 0])
        cells = completed_matrix(cfg)
        matrix.validate_matrix(cfg, cells)
        for index, cell in enumerate(cells):
            changed = copy.deepcopy(cells)
            changed[index]['shard_cache_mib'] = 0 if cell['shard_cache_mib'] else 16
            with self.subTest(cell=cell['cell_id']), self.assertRaises(ValueError):
                matrix.validate_matrix(cfg, changed)
        for field in ('coordinator_cache_mib', 'shard_cache_mib'):
            changed = copy.deepcopy(cells)
            changed[0][field] = False
            with self.subTest(field=field), self.assertRaises(ValueError):
                matrix.validate_matrix(cfg, changed)
        for budgets in ([0], [0, 16, 16, True], [0, -1, 16, 0], [0, 129, 16, 0], None):
            with self.subTest(budgets=budgets), self.assertRaises(ValueError):
                supervisor.validate_cfg(self.cfg | dict(shard_cache_mib_by_repetition=budgets))

    def test_profile_limits_are_bound_to_repetition_not_transport(self):
        cfg = self.cfg | dict(repetitions=2, read_profile_limits=[65536, 0],
                              write_profile_limits=[128, 0], native_write_profile_limits=[640, 0])
        cells = completed_matrix(cfg)
        self.assertEqual([cell['read_profile_limit'] for cell in cells], [65536, 65536, 0, 0])
        matrix.validate_matrix(cfg, cells)
        for index in range(4):
            for field in ('read_profile_limit', 'write_profile_limit', 'native_write_profile_limit'):
                changed = copy.deepcopy(cells)
                changed[index][field] = 0 if index < 2 else 1
                with self.subTest(cell=index, field=field), self.assertRaises(ValueError):
                    matrix.validate_matrix(cfg, changed)
        boolean = copy.deepcopy(self.cells)
        boolean[0]['read_profile_limit'] = False
        with self.assertRaises(ValueError):
            matrix.validate_matrix(self.cfg, boolean)

    def test_historical_unprofiled_write_evidence_remains_readable(self):
        for config_field, cell_field, limits in (
                ('write_profile_limits', 'write_profile_limit', [0, 128, 128, 0]),
                ('native_write_profile_limits', 'native_write_profile_limit', [0, 640, 640, 0])):
            cfg = self.cfg.copy()
            del cfg[config_field]
            cells = completed_matrix(cfg)
            for cell in cells:
                del cell[cell_field]
            matrix.validate_matrix(cfg, cells)
            cfg[config_field] = limits
            with self.subTest(field=cell_field), self.assertRaises(ValueError):
                matrix.validate_matrix(cfg, cells)

    def test_provider_must_be_hardware_efa_rdm(self):
        matrix.validate_provider('provider: efa\n    type: FI_EP_RDM\n')
        verbose = 'fi_info:\n    fi_ep_attr:\n        type: FI_EP_RDM\n    fi_fabric_attr:\n        prov_name: efa\n'
        matrix.validate_provider(verbose)
        for text in ('provider: efa;ofi_rxd\n type: FI_EP_RDM\n',
                     'provider: tcp\n type: FI_EP_RDM\n', 'provider: efa\n type: FI_EP_MSG\n',
                     verbose.replace('prov_name: efa', 'prov_name: efa;ofi_rxd'),
                     verbose.replace('prov_name: efa', 'prov_name: tcp'),
                     verbose.replace('FI_EP_RDM', 'FI_EP_MSG'),
                     verbose + 'provider: tcp\n type: FI_EP_RDM\n'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                matrix.validate_provider(text)

    def test_sysfs_snapshot_fails_closed_and_binds_both_directions(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            with self.assertRaises(ValueError):
                matrix.efa_snapshot({}, base)
            driver = base / 'drivers/efa'
            driver.mkdir(parents=True)
            device = base / 'rdmap0s1'
            (device / 'device').mkdir(parents=True)
            (device / 'device/driver').symlink_to(driver)
            counters = device / 'ports/1/hw_counters'
            counters.mkdir(parents=True)
            (counters / 'send_bytes').write_text('100')
            with self.assertRaises(OSError):
                matrix.efa_snapshot({}, base)
            (counters / 'recv_bytes').write_text('200')
            before = matrix.efa_snapshot({'cell_id': 'r01-efa'}, base)
            (counters / 'send_bytes').write_text('120')
            (counters / 'recv_bytes').write_text('240')
            after = matrix.efa_snapshot({'cell_id': 'r01-efa'}, base)
            proof = matrix.efa_proof(before, after, {'cell_id': 'r01-efa'})
            self.assertEqual((proof['send_bytes'], proof['recv_bytes']), (20, 40))
            (counters / 'recv_bytes').write_text('unreadable')
            with self.assertRaises(ValueError):
                matrix.efa_snapshot({}, base)

    def test_efa_archive_cannot_substitute_raw_counters_or_software_provider(self):
        validator = fixtures.EvidenceTests()
        for mutation in ('raw', 'provider', 'proof'):
            expected, resources, files = completed_node('efa')
            if mutation == 'raw':
                files['efa-before.json'] = b'{}'
            elif mutation == 'provider':
                files['preflight/efa-provider.txt'] = b'provider: efa;ofi_rxd\n type: FI_EP_RDM\n'
            else:
                proof = json.loads(files['efa-proof.json'])
                proof['send_bytes'] += 1
                files['efa-proof.json'] = json.dumps(proof).encode()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validator.validate(files, expected, resources)


class MatrixExecutionTests(unittest.TestCase):
    def campaign(self, root):
        campaign = object.__new__(supervisor.SuiCampaign)
        campaign.cfg = example_config()
        campaign.root, campaign.campaign_results = root, root / 'results'
        campaign.campaign_results.mkdir()
        campaign.results = campaign.campaign_results
        campaign.deadline = campaign.cfg['deadline_utc']
        campaign.plan = matrix.matrix_plan(campaign.cfg)
        campaign.nodes = campaign.cfg['nodes']
        campaign.state = dict(cells=[], complete=False)
        campaign.install_node, campaign.collect_node = Mock(), Mock()
        campaign.node_command = Mock(return_value='release-owned-unit')
        campaign.ssm_one = Mock()
        campaign.parallel_nodes = lambda action, function: [function(node) for node in campaign.nodes]
        campaign.prepare = Mock()
        campaign.replay = Mock()
        campaign.retain_replay = Mock()
        return campaign

    def test_all_pairs_run_in_order_with_release_barriers(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            templates = completed_matrix(campaign.cfg)
            events = []
            def prepare():
                template = templates[campaign.cell['ordinal']]
                campaign.external = dict(config_sha256=template['config_sha256'])
                campaign.cell['namespace_ids'] = template['namespace_ids']
                events.append(('prepare', campaign.cell['cell_id']))
            def replay():
                self.assertEqual(campaign.cell['read_profile_limit'],
                                 campaign.cfg['read_profile_limits'][campaign.cell['repetition'] - 1])
                events.append(('paired-replay', campaign.cell['cell_id']))
                (campaign.results / 'replay-evidence.tar.gz').write_bytes(b'paired full-state audit fixture')
                (campaign.results / 'resources.json').write_bytes(b'physical resources fixture')
            def collect(node, final=False, logs=False):
                if logs:
                    template = templates[campaign.cell['ordinal']]
                    campaign.cell['node_archives'][node['instance_id']] = template['node_archives'][node['instance_id']]
                    if campaign.cell['transport'] == 'efa':
                        target = campaign.results / 'storage' / str(node['partition']) / 'final'
                        target.mkdir(parents=True)
                        supervisor.write_json(target / 'efa-proof.json', template['efa'][node['instance_id']])
            def snapshot(identity):
                template = templates[campaign.cell['ordinal']]
                proof = template['efa'][identity['instance_id']]
                return proof['after' if (campaign.results / 'efa-before.json').exists() else 'before']
            campaign.prepare.side_effect = prepare
            campaign.replay.side_effect = replay
            campaign.collect_node.side_effect = collect
            campaign.ssm_one.side_effect = lambda *args, **kwargs: events.append(('release', campaign.cell['cell_id']))
            with patch.object(supervisor, 'fabric_preflight'), patch.object(supervisor, 'efa_snapshot', side_effect=snapshot):
                campaign.run_matrix()
            self.assertTrue(campaign.state['complete'])
            self.assertEqual([cell['cell_id'] for cell in campaign.state['cells']], [cell['cell_id'] for cell in templates])
            self.assertEqual(campaign.replay.call_count, 8)
            self.assertEqual([cell['read_profile_limit'] for cell in campaign.state['cells']],
                             [0, 0, 4096, 4096, 4096, 4096, 0, 0])
            for index, template in enumerate(templates):
                self.assertEqual(events[index * 7:(index + 1) * 7],
                    [('prepare', template['cell_id']), ('paired-replay', template['cell_id'])]
                    + [('release', template['cell_id'])] * 5)

    def test_partial_export_preserves_prior_cells_and_current_logs(self):
        import tarfile
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            prior = campaign.results / 'cells/r01-tcp'
            prior.mkdir(parents=True)
            (prior / 'replay-evidence.tar.gz').write_bytes(b'prior completed evidence')
            campaign.results = campaign.campaign_results / 'cells/r01-efa'
            campaign.results.mkdir()
            campaign.cell = dict(cell_id='r01-efa', transport='efa', complete=False)
            campaign.state['failures'] = ['injected partial replay failure']
            campaign.replay_root = campaign.root / 'checkpoint-cells/r01-efa/replay'
            experiment = campaign.replay_root / 'experiment'
            experiment.mkdir(parents=True)
            (experiment / 'remote.stderr.log').write_text('partial replay stderr')
            for name, content in (
                    ('native-schema/perpetual/shape.json', 'partial native schema'),
                    ('remote-schema/perpetual/shape.yaml', 'partial remote schema'),
                    ('tls/private.pk8', 'secret'),
                    ('storage/node-0/data', 'bulk database')):
                path = experiment / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            (experiment / 'linked-secret').symlink_to(experiment / 'tls/private.pk8')
            campaign.retain_replay = lambda: supervisor.SuiCampaign.retain_replay(campaign)
            campaign.s3 = Mock()
            retained = {
                'kv/cold-case-tcp/cold_probes.jsonl': b'{"probe_index":0,"requested_probes":1024}\n',
                'kv/cold-case-tcp/cold_group_probes.jsonl': b'{"probe_index":0,"member_count":32}\n',
                'kv/cold-case-tcp/write_io.csv': b'phase,pid,accounted_process_write_bytes_delta\nload_write_window,100,4096\n',
                'kv/cold-case-tcp-process.command.json': b'{"binary_sha256":"source-bound","exit_code":1}\n',
                'kv/plan.json': b'{"source_archive_sha256":"admitted-source","cold_group_probes":1024}\n',
                'kv/raw-rma/raw-rma-0123456789abcdef-v256/client.json': b'{"capabilities":{"read_mode":"native"}}\n',
                'kv/raw-rma/raw-rma-0123456789abcdef-v256/server.tar.gz': b'retrieved hash-bound raw target archive',
            }
            for name, content in retained.items():
                path = campaign.campaign_results / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            with patch.object(supervisor, 'evidence', return_value={}), patch.object(supervisor, 'upload_final', return_value={}):
                campaign.export()
            with tarfile.open(campaign.root / 'checkpoint-results.tar.gz') as archive:
                self.assertEqual(archive.extractfile('results/cells/r01-tcp/replay-evidence.tar.gz').read(), b'prior completed evidence')
                self.assertEqual(archive.extractfile('results/cells/r01-efa/replay-logs/remote.stderr.log').read(), b'partial replay stderr')
                self.assertEqual(archive.extractfile(
                    'results/cells/r01-efa/replay-logs/native-schema/perpetual/shape.json').read(),
                    b'partial native schema')
                self.assertEqual(archive.extractfile(
                    'results/cells/r01-efa/replay-logs/remote-schema/perpetual/shape.yaml').read(),
                    b'partial remote schema')
                for name in ('tls/private.pk8', 'storage/node-0/data', 'linked-secret'):
                    self.assertNotIn('results/cells/r01-efa/replay-logs/' + name, archive.getnames())
                self.assertNotIn('results/COMPLETE', archive.getnames())
                for name, content in retained.items():
                    self.assertEqual(archive.extractfile('results/' + name).read(), content)
                receipt = json.load(archive.extractfile('results/campaign-receipt.json'))
                self.assertFalse(receipt['complete'])

    def test_cell_failure_never_starts_later_cells(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.prepare.side_effect = RuntimeError('injected deployment failure')
            with self.assertRaisesRegex(RuntimeError, 'deployment failure'):
                campaign.run_matrix()
            self.assertEqual([cell['cell_id'] for cell in campaign.state['cells']], ['r01-tcp'])
            campaign.install_node.assert_not_called()
            self.assertFalse(campaign.state['complete'])

    def test_work_deadline_cannot_silently_shrink_matrix(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            with patch.object(supervisor, 'remaining', return_value=59), self.assertRaises(TimeoutError):
                campaign.run_matrix()
            campaign.prepare.assert_not_called()
            self.assertFalse(campaign.state['complete'])

    def test_release_failure_prevents_next_cell_even_after_replay_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.external = dict(config_sha256='a' * 64)
            campaign.ssm_one.side_effect = RuntimeError('owned process remains')
            with self.assertRaisesRegex(RuntimeError, 'owned process remains'):
                campaign.run_matrix()
            campaign.replay.assert_called_once()
            self.assertEqual(len(campaign.state['cells']), 1)
            self.assertFalse(campaign.state['cells'][0]['complete'])
            campaign.retain_replay.assert_not_called()


class PlacementTests(unittest.TestCase):
    def test_all_six_hosts_require_exact_cluster_and_efa(self):
        cfg = example_config()
        group = dict(GroupName=cfg['placement_group'], GroupId='pg-owned', Strategy='cluster', State='available')
        instances = [dict(InstanceId=instance_id, InstanceType=cfg['instance_type'],
            Placement=dict(AvailabilityZone=cfg['availability_zone'], GroupName=group['GroupName'], GroupId=group['GroupId']),
            NetworkInterfaces=[dict(InterfaceType='efa', Status='in-use', Attachment=dict(Status='attached'))])
            for instance_id in [cfg['coordinator_instance_id'], *(node['instance_id'] for node in cfg['nodes'])]]
        matrix.validate_placement(cfg, instances, [group])
        cfg = cfg | dict(transports=['efa-direct'])
        matrix.validate_placement(cfg, instances, [group])
        for index in range(6):
            for mutation in ('zone', 'group', 'group-id', 'hardware', 'interface', 'detached'):
                changed = copy.deepcopy(instances)
                instance = changed[index]
                if mutation in ('zone', 'group', 'group-id'):
                    field = {'zone': 'AvailabilityZone', 'group': 'GroupName', 'group-id': 'GroupId'}[mutation]
                    instance['Placement'][field] = 'foreign'
                elif mutation == 'hardware':
                    instance['InstanceType'] = 'i4i.xlarge'
                elif mutation == 'interface':
                    instance['NetworkInterfaces'][0]['InterfaceType'] = 'interface'
                else:
                    instance['NetworkInterfaces'][0]['Attachment']['Status'] = 'detached'
                with self.subTest(index=index, mutation=mutation), self.assertRaises(ValueError):
                    matrix.validate_placement(cfg, changed, [group])
        for change in (dict(Strategy='spread'), dict(State='pending'), dict(GroupName='foreign')):
            with self.subTest(change=change), self.assertRaises(ValueError):
                matrix.validate_placement(cfg, instances, [group | change])
        with self.assertRaises(ValueError):
            matrix.validate_placement(cfg, instances[:-1], [group])


if __name__ == '__main__':
    unittest.main()
