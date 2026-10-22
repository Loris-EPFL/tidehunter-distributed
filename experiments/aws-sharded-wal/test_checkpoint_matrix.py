"""Offline false-success gates; no AWS calls or provider/device assumptions."""
import copy
import csv
import json
import io
from pathlib import Path
import tarfile
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


    def cold_probe_fixture(self, root, lanes=2, targeted=False, authority=False):
        case_id = ('authority32-cold-efa-direct-current-v8192-b1-w1-r0' if authority else
                   'cold-efa-direct-current-v8192-b32-w32-r0' if targeted else
                   'cold-efa-direct-current-v256-b1-w1-r0')
        directory = Path(root) / (case_id + '-tcp')
        directory.mkdir()
        spec = dict(transport='efa-direct', cold=True, cached=False, diagnostics=False,
                    shape=[8192, 32, 32] if targeted else [8192 if authority else 256, 1, 1],
                    repetition=0, authority_resident_batches=32 if authority else None)
        if targeted:
            spec.update(wal_layout='small', workload_batches=3200, workload_reads=8192,
                        preparation_chunk_batches=16, shard_cache_bytes=0, transport_floor=False)
        elif not authority:
            spec.update(workload_batches=None, cache_hit_contention=False, transport_floor=False)
        nodes = [dict(partition=p, private_ip=f'10.0.1.{p + 1}') for p in range(5)]
        cfg = dict(run_root='/frozen', nodes=nodes, binaries={'storage-node': 'c' * 64})
        geometry = dict(fragment_bytes=1048576, wal_file_bytes=134217728,
                        group_limits=dict(max_batches=32, max_encoded_bytes=1048568))
        resident = 32 if authority else 3336 if targeted else 456
        lifetime = 648 if authority else 3336 if targeted else 456
        case_root = f'/frozen/cases/{case_id}'
        def tls(identity, server_name):
            return dict(transport='efa-direct', ca=case_root + '/tls/ca.der',
                        cert=case_root + '/tls/' + identity + '.der',
                        key=case_root + '/tls/' + identity + '.pk8', server_name=server_name)
        (directory.parent / (case_id + '-remote.yaml')).write_text(json.dumps(dict(
            storage=dict(database=[42] * 16, partitions=list(range(5)),
                         batch_limits=dict(max_operations=32, max_encoded_bytes=327680),
                         max_reference_metadata_bytes=16777216, max_epochs=4,
                         max_batches_per_partition=resident, **geometry),
            endpoints=[dict(partition=node['partition'], address=node['private_ip'] + ':7700',
                            server_name=f"tidehunter-node-{node['partition']}") for node in nodes],
            tls=tls('tidehunter-authority', 'unused-client-template'), timeout=dict(secs=10, nanos=0),
            max_admitted_batches=lifetime, max_index_bytes=134217728,
            max_pending_batches=128, max_pending_bytes=67108864, pipelined_writes=True,
            read_cache_bytes=0, read_memory_allowance_bytes=536870912,
            preparation_workers=2, scan_prefetch_pages=2, read_lanes=lanes,
            max_pending_reads=256, max_pending_read_bytes=67108864, write_profile_limit=0)))
        # Faithful captured argv, intentionally not generated by the helper under test.
        command = ['/frozen/cloud-bin/current/sharded_wal_bench',
                   '--output', f'/frozen/checkpoint-results/kv/{case_id}-tcp', '--backend', 'tcp',
                   '--values', '8192' if targeted or authority else '256',
                   '--batch-sizes', '32' if targeted else '1', '--windows', '32' if targeted else '1',
                   '--batches', '512' if authority else '3200' if targeted else '320', '--mixed', '64',
                   '--reads', '8192' if targeted else '4096', '--repetitions', '1']
        if targeted:
            command += ['--wal-layout', 'small']
        prepare = ['/frozen/cloud-bin/current/sharded_wal_bench',
                   '--prepare-external-case', f'/frozen/cases/{case_id}',
                   '--guest-case-root', f'/frozen/cases/{case_id}',
                   '--remote-endpoints', '10.0.1.1:7700,10.0.1.2:7700,10.0.1.3:7700,10.0.1.4:7700,10.0.1.5:7700',
                   '--tcp-read-cache-bytes', '0', '--transport', 'efa-direct',
                   '--tcp-server-read-workers', '2', '--tcp-wal-member-read', 'async',
                   '--tcp-resident-first', 'true', '--tcp-polling', 'adaptive',
                   '--tcp-shard-cache-bytes', '0'] + command[5:]
        (directory.parent / (case_id + '-prepare.command.json')).write_text(json.dumps(dict(
            exit_code=0, binary_sha256='c' * 64, command=prepare)))
        command += ['--read-resource-evidence', 'true', '--cold-read-pass', 'true', '--read-window', '32',
                    '--remote-config', f'/frozen/cases/{case_id}/remote.yaml',
                    '--tcp-read-cache-bytes', '0', '--transport', 'efa-direct',
                    '--tcp-preparation-chunk-batches', '16']
        (directory.parent / (case_id + '-tcp-process.command.json')).write_text(json.dumps(dict(
            exit_code=0, binary_sha256='c' * 64, command=command)))
        (directory / 'campaign.txt').write_text(
            'cold_read_pass=true\ntransport_diagnostics=false\ntransport=efa-direct\n'
            + ('point_reads=8192\n' if targeted else 'point_reads=4096\n'))
        identity = dict(backend='experimental_efa_direct_authority',
                        value_bytes=8192 if targeted or authority else 256, batch_size=32 if targeted else 1,
                        window=32 if targeted else 1, repetition=0)
        def csv_file(name, rows):
            with (directory / name).open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        csv_file('remote_read_profiles.csv', [
            identity | dict(state=state, sample=sample, whole_call_ns=100)
            for state in ('cache-evicted', 'reopened-cache-cleared') for sample in range(256)])
        phases = ('cache_evicted_get', 'cache_evicted_get_many',
                  'cache_evicted_get_window', 'cache_evicted_diagnostics')
        fields = ('cache_hits', 'cache_misses', 'wal_reads',
                  'resident_read_successes', 'async_read_submissions')
        csv_file('read_path_evidence.csv', [
            identity | dict(phase=phase, boundary=boundary, role=role, partition=partition)
            | {field: int(boundary == 'after') for field in fields}
            for phase in phases[:-1] for boundary in ('before', 'after')
            for role, partition in [('frontend', ''), *(('storage', str(p)) for p in range(5))]])
        csv_file('read_io.csv', [identity | dict(
            phase=phase, partition=p, pid=16000 + p, physical_read_bytes=4096)
            for phase in phases for p in range(5)])
        node_root = directory.parent / 'nodes' / case_id
        node_root.mkdir(parents=True)
        shape = (
            'key_spaces:\n- inner:\n    id: 0\n    name: root\n    key_indexing: !Fixed 8\n'
            '    mutexes: 16\n    key_type: !Uniform {cells_per_mutex: 64}\n'
            '    config: {disable_unload: false, max_dirty_keys: null, l0_max_entries: null, '
            'bloom_filter: null, value_cache_size: 0, index_format: Header, unloaded_iterator: true}\n'
            '    destroyed: false\n')
        for node in nodes:
            partition = node['partition']
            start = json.dumps(dict(pid=16000 + partition, ready=node['private_ip'] + ':7700',
                                    build='current', binary_sha256='c' * 64)).encode()
            with tarfile.open(node_root / f'node-{partition}.tar.gz', 'w:gz') as archive:
                member = tarfile.TarInfo('start.json')
                member.size = len(start)
                archive.addfile(member, io.BytesIO(start))
                launch = json.dumps(dict(
                    node=dict(root=f'{case_root}/node-{partition}/storage',
                        partition=dict(inventory=dict(database=[42] * 16, epoch=1,
                            segment_generation=1, partitions=list(range(5))),
                            partition=partition, max_batches=resident, **geometry,
                            batch_limits=dict(max_operations=32, max_encoded_bytes=327680),
                            max_reference_metadata_bytes=16777216),
                        shape_yaml=shape, max_epochs=4, read_workers=2, wal_member_read='async',
                        read_resident_first=True, read_cache_bytes=0, read_memory_allowance_bytes=536870912,
                        read_max_inflight_requests=32, read_max_inflight_bytes=67108864, read_max_device_reads=128),
                    tls=tls(f'tidehunter-node-{partition}', f'tidehunter-node-{partition}'),
                    limits=dict(max_frame_bytes=16777216, max_connections=16, max_reads=256,
                        recovery_page_batches=128, io_timeout=dict(secs=60, nanos=0), polling='adaptive'),
                    listen=node['private_ip'] + ':7700', ready_file=f'{case_root}/node-{partition}/ready',
                    stop_file=f'{case_root}/node-{partition}/stop', drop_first_append_response=False)).encode()
                member = tarfile.TarInfo('launch.yaml')
                member.size = len(launch)
                archive.addfile(member, io.BytesIO(launch))
        records, servers, profiles = [], {}, []
        for p, node in enumerate(nodes):
            count = 52 if p == 0 else 51
            endpoint = node['private_ip'] + ':7700'
            server = []
            for lane in range(lanes):
                reads = (count + lanes - 1 - lane) // lanes
                peer = f'10.0.0.1:{43000 + 8 * p + lane}->{endpoint}'
                records.append(dict(schema=1, reason='profile-disable', window=1 + lanes * p + lane,
                                    data_posts=reads, credit_posts=reads, piggyback_advances=0, peer=peer))
                server.append(dict(schema=1, reason='profile-disable', window=lane + 1,
                                   data_posts=reads + 1, credit_posts=1, piggyback_advances=reads,
                                   peer='->'.join(reversed(peer.split('->')))))
            server.append(dict(schema=1, reason='profile-disable', window=lanes + 1, data_posts=1,
                               credit_posts=0, piggyback_advances=1,
                               peer=f'{endpoint}->10.0.0.1:{45000 + p}'))
            servers[p] = server
            profiles.append([p, dict(requests=count, client=dict(requests=count),
                client_transport=dict(send_post_calls=2 * count, send_post_would_block=0, send_post_errors=0),
                transport=dict(send_post_calls=count + 2 * lanes + 1, send_post_would_block=0, send_post_errors=0))])
        (directory / 'node_profiles.yaml').write_text(json.dumps(
            identity | dict(phase='cache-evicted-read-diagnostics', nodes=profiles)))
        def parsed(rows):
            return matrix.parse_fabric_local_diagnostics(io.BytesIO(b''.join(
                matrix.FABRIC_LOCAL_PREFIX + b' ' + json.dumps(row).encode() + b'\n' for row in rows)))
        return directory, spec, cfg, parsed(records), {p: parsed(rows) for p, rows in servers.items()}

    def test_cold_auxiliary_matches_successful_not_attempted_posts(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, servers = self.cold_probe_fixture(root)
            path = directory / 'node_profiles.yaml'
            profiles = json.loads(path.read_text())
            for _, profile in profiles['nodes']:
                for key in ('client_transport', 'transport'):
                    profile[key]['send_post_calls'] += 4
                    profile[key]['send_post_would_block'] = 3
                    profile[key]['send_post_errors'] = 1
            path.write_text(json.dumps(profiles))
            certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
            for partition, evidence in servers.items():
                checkpoint_kv.validate_cold_probe_storage(evidence, certificate, partition)

    def test_cold_auxiliary_rejects_self_consistent_unadmitted_lane_populations(self):
        for lanes in (1, 3):
            with self.subTest(lanes=lanes), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, servers = self.cold_probe_fixture(root, lanes=lanes)
                case_id = directory.name[:-4]
                paths = [directory.parent / (case_id + '-prepare.command.json'),
                         directory.parent / (directory.name + '-process.command.json')]
                receipts = [path.read_bytes() for path in paths]
                profiles = dict(json.loads((directory / 'node_profiles.yaml').read_text())['nodes'])
                for node in cfg['nodes']:
                    partition = node['partition']
                    endpoint = node['private_ip'] + ':7700'
                    selected = [row for row in client['records'] if row['peer'].endswith('->' + endpoint)]
                    checkpoint_kv.validate_cold_probe_windows(
                        selected, profiles[partition]['client_transport'], lanes)
                    checkpoint_kv.validate_cold_probe_windows(
                        servers[partition]['records'], profiles[partition]['transport'], lanes + 1)
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                self.assertEqual([path.read_bytes() for path in paths], receipts)

    def test_cold_auxiliary_rejects_every_changed_missing_or_unknown_effective_policy_field(self):
        def mutations(value, path=()):
            if isinstance(value, dict):
                yield 'unknown', path, None
                for key, child in value.items():
                    yield 'missing', path + (key,), None
                    yield from mutations(child, path + (key,))
            elif isinstance(value, list):
                yield 'shortened', path, None
                for index, child in enumerate(value):
                    yield from mutations(child, path + (index,))
            else:
                if type(value) is bool:
                    replacement = not value
                elif type(value) is int:
                    replacement = value + 1
                elif isinstance(value, str):
                    replacement = value + '-foreign'
                else:
                    replacement = 0
                yield 'changed', path, replacement
        for role in ('remote', 'owner'):
            with self.subTest(role=role), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                case_id = directory.name[:-4]
                receipts = [directory.parent / (case_id + '-prepare.command.json'),
                            directory.parent / (directory.name + '-process.command.json')]
                original_receipts = [path.read_bytes() for path in receipts]
                if role == 'remote':
                    path = directory.parent / (case_id + '-remote.yaml')
                    original = json.loads(path.read_text())
                else:
                    path = directory.parent / 'nodes' / case_id / 'node-0.tar.gz'
                    with tarfile.open(path) as archive:
                        start = archive.extractfile('start.json').read()
                        original = json.load(archive.extractfile('launch.yaml'))
                for change, keys, replacement in mutations(original):
                    with self.subTest(change=change, keys=keys):
                        document = copy.deepcopy(original)
                        target = document
                        for key in keys[:-1]:
                            target = target[key]
                        if change == 'unknown':
                            container = target[keys[-1]] if keys else target
                            container['unadmitted_runtime_control'] = 1
                        elif change == 'missing':
                            del target[keys[-1]]
                        elif change == 'shortened':
                            target[keys[-1]].pop()
                        else:
                            target[keys[-1]] = replacement
                        if role == 'remote':
                            path.write_text(json.dumps(document))
                        else:
                            with tarfile.open(path, 'w:gz') as archive:
                                for name, payload in (('start.json', start),
                                                      ('launch.yaml', json.dumps(document).encode())):
                                    member = tarfile.TarInfo(name)
                                    member.size = len(payload)
                                    archive.addfile(member, io.BytesIO(payload))
                        with self.assertRaises(ValueError):
                            checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                        self.assertEqual([path.read_bytes() for path in receipts], original_receipts)

    def test_cold_auxiliary_admits_shape_semantics_not_serialization_and_rejects_foreign_controls(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
            path = directory.parent / 'nodes' / directory.name[:-4] / 'node-0.tar.gz'
            with tarfile.open(path) as archive:
                start = archive.extractfile('start.json').read()
                original = json.load(archive.extractfile('launch.yaml'))
            changes = [
                ('key_spaces:', '# equivalent formatting\nkey_spaces:', True),
                ('!Fixed 8', '!Fixed 16', False),
                ('mutexes: 16', 'mutexes: 32', False),
                ('cells_per_mutex: 64', 'cells_per_mutex: 128', False),
                ('disable_unload: false', 'disable_unload: true', False),
                ('max_dirty_keys: null', 'max_dirty_keys: 1', False),
                ('l0_max_entries: null', 'l0_max_entries: 1', False),
                ('value_cache_size: 0', 'value_cache_size: 8', False),
                ('index_format: Header', 'index_format: Plain', False),
                ('unloaded_iterator: true', 'unloaded_iterator: false', False),
                ('destroyed: false', 'destroyed: true', False),
                ('disable_unload: false', 'unadmitted_flush_policy: true, disable_unload: false', False)]
            for before, after, admitted in changes:
                with self.subTest(after=after):
                    document = copy.deepcopy(original)
                    self.assertIn(before, document['node']['shape_yaml'])
                    document['node']['shape_yaml'] = document['node']['shape_yaml'].replace(before, after)
                    with tarfile.open(path, 'w:gz') as archive:
                        for name, payload in (('start.json', start), ('launch.yaml', json.dumps(document).encode())):
                            member = tarfile.TarInfo(name)
                            member.size = len(payload)
                            archive.addfile(member, io.BytesIO(payload))
                    if admitted:
                        checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                    else:
                        with self.assertRaises(ValueError):
                            checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def credit_only_probe_windows(self, evidence, peers=None):
        records = copy.deepcopy(evidence['records'])
        for row in records:
            if peers is None or row['peer'] in peers:
                row['credit_posts'] += row['data_posts']
                row['data_posts'] = 0
                row['piggyback_advances'] = 0
        result = matrix.parse_fabric_local_diagnostics(io.BytesIO(b''.join(
            matrix.FABRIC_LOCAL_PREFIX + b' ' + json.dumps(row).encode() + b'\n' for row in records)))
        self.assertEqual(sum(row['data_posts'] + row['credit_posts'] for row in records),
                         sum(row['data_posts'] + row['credit_posts'] for row in evidence['records']))
        return result

    def test_cold_auxiliary_rejects_credit_only_substitution_for_requested_read_data(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, servers = self.cold_probe_fixture(root)
            certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
            converted_client = self.credit_only_probe_windows(client)
            with self.assertRaisesRegex(ValueError, 'client read-lane DATA'):
                checkpoint_kv.cold_probe_certificate(directory, spec, cfg, converted_client)
            for partition, evidence in servers.items():
                read_peers = {'->'.join(reversed(peer.split('->')))
                              for peer in certificate['owners'][partition]['client_peers']}
                for all_windows in (False, True):
                    with self.subTest(partition=partition, all_windows=all_windows):
                        converted = self.credit_only_probe_windows(
                            evidence, None if all_windows else read_peers)
                        # A writer/control DATA post cannot establish read-peer traffic.
                        if not all_windows:
                            self.assertGreater(sum(row['data_posts'] for row in converted['records']
                                                   if row['peer'] not in read_peers), 0)
                        with self.assertRaisesRegex(ValueError, 'storage read-peer DATA'):
                            checkpoint_kv.validate_cold_probe_storage(converted, certificate, partition)

    def test_cold_auxiliary_allows_credit_only_lane_when_owner_has_other_read_data(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, servers = self.cold_probe_fixture(root)
            client_peer = client['records'][0]['peer']
            client = self.credit_only_probe_windows(client, {client_peer})
            certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
            storage_peer = '->'.join(reversed(client_peer.split('->')))
            servers[0] = self.credit_only_probe_windows(servers[0], {storage_peer})
            for partition, evidence in servers.items():
                checkpoint_kv.validate_cold_probe_storage(evidence, certificate, partition)

    def test_cold_auxiliary_allows_credit_only_windows_for_zero_request_owner(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, servers = self.cold_probe_fixture(root)
            path = directory / 'node_profiles.yaml'
            profiles = json.loads(path.read_text())
            idle, active = profiles['nodes'][0][1], profiles['nodes'][1][1]
            active['requests'] += idle['requests']
            active['client']['requests'] = active['requests']
            idle['requests'] = idle['client']['requests'] = 0
            path.write_text(json.dumps(profiles))
            endpoint = cfg['nodes'][0]['private_ip'] + ':7700'
            client_peers = {row['peer'] for row in client['records'] if row['peer'].endswith('->' + endpoint)}
            client = self.credit_only_probe_windows(client, client_peers)
            certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
            servers[0] = self.credit_only_probe_windows(servers[0])
            for partition, evidence in servers.items():
                checkpoint_kv.validate_cold_probe_storage(evidence, certificate, partition)

    def test_cold_auxiliary_accepts_full_and_targeted_captured_commands(self):
        for targeted, authority in ((False, False), (True, False), (False, True)):
            with self.subTest(targeted=targeted, authority=authority), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(
                    root, targeted=targeted, authority=authority)
                self.assertIn(spec, checkpoint_kv.plan(
                    dict(repetitions=1, diagnostics=False, scope='targeted' if targeted else 'full'),
                    ['efa-direct']))
                certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                self.assertEqual(certificate['samples'], 256)

    def test_cold_auxiliary_requires_successful_original_admitted_preparation(self):
        defects = ('missing', 'invalid_json', 'null', 'list', 'empty', 'failed', 'missing_exit',
                   'boolean_exit', 'wrong_binary', 'missing_binary', 'missing_command',
                   'empty_command', 'non_list_command', 'non_string_argument')
        for defect in defects:
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                measurement_path = directory.parent / (directory.name + '-process.command.json')
                measurement = measurement_path.read_bytes()
                path = directory.parent / (directory.name[:-4] + '-prepare.command.json')
                receipt = json.loads(path.read_text())
                if defect == 'missing':
                    path.unlink()
                elif defect == 'invalid_json':
                    path.write_text('{')
                else:
                    if defect == 'null':
                        receipt = None
                    elif defect == 'list':
                        receipt = []
                    elif defect == 'empty':
                        receipt = {}
                    elif defect == 'failed':
                        receipt['exit_code'] = 1
                    elif defect == 'missing_exit':
                        del receipt['exit_code']
                    elif defect == 'boolean_exit':
                        receipt['exit_code'] = False
                    elif defect == 'wrong_binary':
                        receipt['binary_sha256'] = 'd' * 64
                    elif defect == 'missing_binary':
                        del receipt['binary_sha256']
                    elif defect == 'missing_command':
                        del receipt['command']
                    elif defect == 'empty_command':
                        receipt['command'] = []
                    elif defect == 'non_list_command':
                        receipt['command'] = 'sharded_wal_bench'
                    else:
                        receipt['command'][0] = None
                    path.write_text(json.dumps(receipt))
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                self.assertEqual(measurement_path.read_bytes(), measurement)

    def test_cold_auxiliary_rejects_every_unadmitted_preparation_argument(self):
        for targeted, authority in ((False, False), (True, False), (False, True)):
            with self.subTest(targeted=targeted, authority=authority), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(
                    root, targeted=targeted, authority=authority)
                checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                measurement_path = directory.parent / (directory.name + '-process.command.json')
                measurement = measurement_path.read_bytes()
                path = directory.parent / (directory.name[:-4] + '-prepare.command.json')
                receipt = json.loads(path.read_text())
                original = receipt['command']
                mutations = [('binary', ['/foreign/sharded_wal_bench'] + original[1:])]
                for position in range(1, len(original), 2):
                    flag = original[position]
                    changed = list(original)
                    changed[position + 1] = 'foreign'
                    mutations.append(('changed:' + flag, changed))
                    mutations.append(('missing:' + flag, original[:position] + original[position + 2:]))
                    mutations.append(('duplicate:' + flag, original + original[position:position + 2]))
                mutations += [
                    ('reordered', original[:1] + original[3:5] + original[1:3] + original[5:]),
                    ('unknown', original + ['--unknown-control', 'true']),
                    ('extra-positional', original + ['foreign']),
                    ('incomplete', original + ['--tcp-read-lanes']),
                    ('unadmitted-lanes', original + ['--tcp-read-lanes', '3']),
                    ('unadmitted-pipeline', original + ['--tcp-pipeline', 'false']),
                    ('unadmitted-shard-memory', original + ['--shard-read-memory-allowance-bytes', '134217728']),
                    ('unadmitted-client-memory', original + ['--read-memory-allowance-bytes', '134217728'])]
                for change, command in mutations:
                    with self.subTest(change=change):
                        path.write_text(json.dumps(receipt | {'command': command}))
                        with self.assertRaises(ValueError):
                            checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                        self.assertEqual(measurement_path.read_bytes(), measurement)

    def test_cold_auxiliary_rejects_preparation_overrides_with_consistent_effective_evidence(self):
        for control, value in (('--tcp-read-lanes', '3'), ('--tcp-pipeline', 'false'),
                               ('--shard-read-memory-allowance-bytes', '134217728')):
            with self.subTest(control=control), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, servers = self.cold_probe_fixture(root)
                certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                for partition, evidence in servers.items():
                    checkpoint_kv.validate_cold_probe_storage(evidence, certificate, partition)
                measurement_path = directory.parent / (directory.name + '-process.command.json')
                measurement = measurement_path.read_bytes()
                path = directory.parent / (directory.name[:-4] + '-prepare.command.json')
                receipt = json.loads(path.read_text())
                receipt['command'] += [control, value]
                path.write_text(json.dumps(receipt))
                if control == '--tcp-pipeline':
                    path = directory.parent / (directory.name[:-4] + '-remote.yaml')
                    remote = json.loads(path.read_text())
                    remote['pipelined_writes'] = False
                    path.write_text(json.dumps(remote))
                elif control == '--shard-read-memory-allowance-bytes':
                    for partition in range(5):
                        path = directory.parent / 'nodes' / directory.name[:-4] / f'node-{partition}.tar.gz'
                        with tarfile.open(path) as archive:
                            start = archive.extractfile('start.json').read()
                            launch = json.load(archive.extractfile('launch.yaml'))
                        launch['node']['read_memory_allowance_bytes'] = int(value)
                        with tarfile.open(path, 'w:gz') as archive:
                            for name, payload in (('start.json', start), ('launch.yaml', json.dumps(launch).encode())):
                                member = tarfile.TarInfo(name)
                                member.size = len(payload)
                                archive.addfile(member, io.BytesIO(payload))
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                self.assertEqual(measurement_path.read_bytes(), measurement)

    def test_cold_auxiliary_rejects_foreign_effective_policy_even_when_receipts_agree(self):
        geometry_changes = [
            (('max_batches_per_partition',), ('max_batches',), 512),
            (('fragment_bytes',), ('fragment_bytes',), 16 * 1024**2),
            (('wal_file_bytes',), ('wal_file_bytes',), 256 * 1024**2),
            (('group_limits', 'max_batches'), ('group_limits', 'max_batches'), 64),
            (('group_limits', 'max_encoded_bytes'), ('group_limits', 'max_encoded_bytes'), 12 * 1024**2)]
        for scope in ('remote', 'owner', 'matching-remote-and-all-owners'):
            for remote_path, owner_path, value in geometry_changes:
                with self.subTest(scope=scope, field=remote_path), tempfile.TemporaryDirectory() as root:
                    directory, spec, cfg, client, _ = self.cold_probe_fixture(root, authority=True)
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                    def update(document, path):
                        for key in path[:-1]:
                            document = document[key]
                        document[path[-1]] = value
                    if scope != 'owner':
                        path = directory.parent / (directory.name[:-4] + '-remote.yaml')
                        remote = json.loads(path.read_text())
                        update(remote['storage'], remote_path)
                        path.write_text(json.dumps(remote))
                    if scope != 'remote':
                        for partition in range(5):
                            path = directory.parent / 'nodes' / directory.name[:-4] / f'node-{partition}.tar.gz'
                            with tarfile.open(path) as archive:
                                start = archive.extractfile('start.json').read()
                                launch = json.load(archive.extractfile('launch.yaml'))
                            update(launch['node']['partition'], owner_path)
                            with tarfile.open(path, 'w:gz') as archive:
                                for name, payload in (('start.json', start),
                                                      ('launch.yaml', json.dumps(launch).encode())):
                                    member = tarfile.TarInfo(name)
                                    member.size = len(payload)
                                    archive.addfile(member, io.BytesIO(payload))
                    with self.assertRaisesRegex(ValueError, 'effective KV policy'):
                        checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_binds_explicit_owner_controls_and_lifetime_admission(self):
        changes = [(('node', 'read_workers'), 1), (('node', 'wal_member_read'), 'mapped'),
                   (('node', 'read_resident_first'), False), (('node', 'read_resident_first'), 1),
                   (('node', 'read_cache_bytes'), 8 * 1024**2), (('limits', 'polling'), 'busy'),
                   (('tls', 'transport'), 'tcp'), (('node', 'partition', 'partition'), 1),
                   (('listen',), '10.9.9.9:7700')]
        for keys, value in changes:
            with self.subTest(keys=keys, value=value), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root, authority=True)
                path = directory.parent / 'nodes' / directory.name[:-4] / 'node-0.tar.gz'
                with tarfile.open(path) as archive:
                    start = archive.extractfile('start.json').read()
                    launch = json.load(archive.extractfile('launch.yaml'))
                target = launch
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                with tarfile.open(path, 'w:gz') as archive:
                    for name, payload in (('start.json', start), ('launch.yaml', json.dumps(launch).encode())):
                        member = tarfile.TarInfo(name)
                        member.size = len(payload)
                        archive.addfile(member, io.BytesIO(payload))
                with self.assertRaisesRegex(ValueError, 'effective KV policy'):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
        for field, value in (('max_admitted_batches', 32), ('read_cache_bytes', 8 * 1024**2)):
            with self.subTest(remote_field=field), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root, authority=True)
                path = directory.parent / (directory.name[:-4] + '-remote.yaml')
                remote = json.loads(path.read_text())
                remote[field] = value
                path.write_text(json.dumps(remote))
                with self.assertRaisesRegex(ValueError, 'effective KV policy'):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_requires_one_bounded_regular_launch_policy(self):
        for change in ('missing', 'duplicate', 'symlink', 'oversized', 'duplicate_yaml_key'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                path = directory.parent / 'nodes' / directory.name[:-4] / 'node-0.tar.gz'
                with tarfile.open(path) as archive:
                    start = archive.extractfile('start.json').read()
                    launch = archive.extractfile('launch.yaml').read()
                if change == 'oversized':
                    launch = b' ' * (64 * 1024 + 1)
                elif change == 'duplicate_yaml_key':
                    launch = b'node: {}\nnode: {}\n'
                with tarfile.open(path, 'w:gz') as archive:
                    member = tarfile.TarInfo('start.json')
                    member.size = len(start)
                    archive.addfile(member, io.BytesIO(start))
                    if change != 'missing':
                        for _ in range(2 if change == 'duplicate' else 1):
                            member = tarfile.TarInfo('launch.yaml')
                            if change == 'symlink':
                                member.type = tarfile.SYMTYPE
                                member.linkname = 'foreign.yaml'
                                archive.addfile(member)
                            else:
                                member.size = len(launch)
                                archive.addfile(member, io.BytesIO(launch))
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_rejects_every_command_change_against_unchanged_plan(self):
        for targeted in (False, True):
            with self.subTest(targeted=targeted), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root, targeted=targeted)
                path = directory.parent / (directory.name + '-process.command.json')
                receipt = json.loads(path.read_text())
                original = receipt['command']
                checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                mutations = [('binary', ['/foreign/sharded_wal_bench'] + original[1:])]
                # Include every load/geometry option, paths, backend and switches,
                # not just the flags a previous partial whitelist happened to know.
                for position in range(1, len(original), 2):
                    flag = original[position]
                    changed = list(original)
                    changed[position + 1] = 'foreign'
                    mutations.append(('value:' + flag, changed))
                    mutations.append(('missing:' + flag, original[:position] + original[position + 2:]))
                    mutations.append(('duplicate:' + flag, original + original[position:position + 2]))
                mutations += [
                    ('reordered', original[:1] + original[3:5] + original[1:3] + original[5:]),
                    ('unknown', original + ['--unknown-control', 'true']),
                    ('extra-positional', original + ['foreign']),
                    ('explicit-default-layout', original + ['--wal-layout', 'small']),
                    ('foreign-layout', original + ['--wal-layout', 'sui']),
                    ('unplanned-read-lanes', original + ['--tcp-read-lanes', '2']),
                    ('unplanned-native-cache', original + ['--native-read-cache-bytes', '0']),
                    ('unplanned-hot', original + ['--hot-read-pass', 'true']),
                    ('unplanned-diagnostics', original + ['--transport-diagnostics', 'false']),
                    ('unplanned-contention', original + ['--cache-hit-contention', 'true'])]
                for change, command in mutations:
                    with self.subTest(change=change):
                        path.write_text(json.dumps(receipt | {'command': command}))
                        with self.assertRaises(ValueError):
                            checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_clean_cold_auxiliary_is_separate_and_bound_to_real_profile_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, servers = self.cold_probe_fixture(root)
            certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
            self.assertEqual(certificate['samples'], 256)
            self.assertEqual(certificate['phase'], 'cache-evicted-read-diagnostics')
            for partition, evidence in servers.items():
                # Exercise the real bounded archive/log consumer as well as phase attribution.
                blob = b''.join(matrix.FABRIC_LOCAL_PREFIX + b' ' + json.dumps(row).encode() + b'\n'
                                for row in evidence['records'])
                archive_bytes = io.BytesIO()
                with tarfile.open(fileobj=archive_bytes, mode='w:gz') as archive:
                    member = tarfile.TarInfo('node.stderr.log')
                    member.size = len(blob)
                    archive.addfile(member, io.BytesIO(blob))
                archive_bytes.seek(0)
                with tarfile.open(fileobj=archive_bytes, mode='r:gz') as archive:
                    parsed = matrix.fabric_local_archive_log(archive, 'node.stderr.log', required=True)
                checkpoint_kv.validate_cold_probe_storage(parsed, certificate, partition)
            with self.assertRaises(ValueError):
                matrix.validate_fabric_local_population(client, 'wholly-clean-warm', forbidden=True)
            for change in (dict(cold=False), dict(diagnostics=True), dict(transport='tcp')):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec | change, cfg, client)

    def test_cold_auxiliary_rejects_foreign_deployment_or_process_configuration(self):
        for change in ('deployment_owner', 'deployment_lanes', 'deployment_transport',
                       'process_profiled', 'process_owner', 'process_reads', 'process_lanes',
                       'process_duplicate', 'process_exit'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                if change.startswith('deployment'):
                    path = directory.parent / 'cold-efa-direct-current-v256-b1-w1-r0-remote.yaml'
                    deployment = json.loads(path.read_text())
                    if change == 'deployment_owner':
                        deployment['endpoints'][0]['address'] = '10.9.9.9:7700'
                    elif change == 'deployment_lanes':
                        deployment['read_lanes'] = 0
                    else:
                        deployment['tls']['transport'] = 'tcp'
                    path.write_text(json.dumps(deployment))
                else:
                    path = directory.parent / 'cold-efa-direct-current-v256-b1-w1-r0-tcp-process.command.json'
                    process = json.loads(path.read_text())
                    command = process['command']
                    if change == 'process_profiled':
                        command += ['--transport-diagnostics', 'true']
                    elif change == 'process_owner':
                        command[command.index('--remote-config') + 1] = '/frozen/cases/foreign/remote.yaml'
                    elif change == 'process_reads':
                        command[command.index('--reads') + 1] = '8192'
                    elif change == 'process_lanes':
                        command += ['--tcp-read-lanes', '3']
                    elif change == 'process_duplicate':
                        command += ['--reads', '4096']
                    else:
                        process['exit_code'] = 1
                    path.write_text(json.dumps(process))
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_binds_physical_io_to_original_owner_and_case(self):
        changes = [('backend', 'experimental_tcp_authority'), ('value_bytes', '8192'),
                   ('batch_size', '32'), ('window', '32'), ('repetition', '1'),
                   ('pid', '16001'), ('all_owner_pid', '99999'), ('auxiliary_pid', '99999'),
                   ('phase', 'foreign-phase'), ('partition', '5'),
                   ('missing', None), ('duplicate', None), ('extra', None), ('oversized', None)]
        for field, value in changes:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                path = directory / 'read_io.csv'
                with path.open() as stream:
                    rows = list(csv.DictReader(stream))
                if field == 'missing':
                    rows.pop()
                elif field == 'duplicate':
                    rows[1] = dict(rows[0])
                elif field == 'extra':
                    rows.append(dict(rows[0]))
                elif field == 'oversized':
                    rows[0]['padding'] = 'x' * (4096 * (len(rows) + 1))
                elif field == 'all_owner_pid':
                    for row in rows:
                        if row['partition'] == '0':
                            row['pid'] = value
                elif field == 'auxiliary_pid':
                    rows[-1]['pid'] = value
                else:
                    rows[0][field] = value
                with path.open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_requires_positive_aggregate_io_in_every_cold_phase(self):
        phases = ('cache_evicted_get', 'cache_evicted_get_many',
                  'cache_evicted_get_window', 'cache_evicted_diagnostics')
        for phase in (*phases, None):
            with self.subTest(zero_phase=phase), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                path = directory / 'read_io.csv'
                with path.open() as stream:
                    rows = list(csv.DictReader(stream))
                for row in rows:
                    if row['phase'] == phase or (phase is None and row['partition'] == '0'):
                        row['physical_read_bytes'] = '0'
                with path.open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
                if phase is None:
                    # One idle owner is valid; only the phase aggregate must be positive.
                    certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
                    self.assertEqual(certificate['samples'], 256)
                else:
                    with self.assertRaises(ValueError):
                        checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_requires_complete_positive_cold_and_reopened_api_samples(self):
        for change in ('tail_missing', 'tail_absent', 'tail_duplicate', 'tail_ordinal',
                       'tail_zero', 'tail_negative', 'cold_zero', 'extra'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                path = directory / 'remote_read_profiles.csv'
                with path.open() as stream:
                    rows = list(csv.DictReader(stream))
                if change == 'tail_missing':
                    rows.pop()
                elif change == 'tail_absent':
                    rows = rows[:256]
                elif change == 'tail_duplicate':
                    rows[-1]['sample'] = '0'
                elif change == 'tail_ordinal':
                    rows[-1]['sample'] = '256'
                elif change in ('tail_zero', 'tail_negative'):
                    rows[-1]['whole_call_ns'] = '0' if change == 'tail_zero' else '-1'
                elif change == 'cold_zero':
                    rows[0]['whole_call_ns'] = '0'
                else:
                    rows.append(dict(rows[-1]))
                with path.open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_rejects_consistent_receipts_from_a_non_admitted_binary(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
            # The process and every original owner agree on c, but admission requires d.
            with self.assertRaises(ValueError):
                checkpoint_kv.cold_probe_certificate(directory, spec, cfg | {'binaries': cfg['binaries'] | {'storage-node': 'd' * 64}}, client)

    def test_cold_auxiliary_requires_one_bounded_original_owner_start_record(self):
        for change in ('missing', 'duplicate', 'symlink', 'oversized', 'duplicate_pid',
                       'pid_bool', 'pid_zero', 'pid_negative', 'pid_foreign',
                       'endpoint', 'build', 'binary'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                path = directory.parent / 'nodes' / 'cold-efa-direct-current-v256-b1-w1-r0' / 'node-0.tar.gz'
                with tarfile.open(path, 'r:gz') as archive:
                    start = json.load(archive.extractfile('start.json'))
                    launch = archive.extractfile('launch.yaml').read()
                if change.startswith('pid_'):
                    start['pid'] = {'pid_bool': True, 'pid_zero': 0,
                                    'pid_negative': -1, 'pid_foreign': 99999}[change]
                elif change == 'endpoint':
                    start['ready'] = '10.9.9.9:7700'
                elif change == 'build':
                    start['build'] = 'baseline'
                elif change == 'binary':
                    start['binary_sha256'] = 'd' * 64
                payload = json.dumps(start).encode()
                if change == 'oversized':
                    payload = b' ' * 4097
                elif change == 'duplicate_pid':
                    payload = payload[:-1] + b', "pid": 16000}'
                with tarfile.open(path, 'w:gz') as archive:
                    member = tarfile.TarInfo('launch.yaml')
                    member.size = len(launch)
                    archive.addfile(member, io.BytesIO(launch))
                    if change != 'missing':
                        for _ in range(2 if change == 'duplicate' else 1):
                            member = tarfile.TarInfo('start.json')
                            if change == 'symlink':
                                member.type = tarfile.SYMTYPE
                                member.linkname = 'foreign.json'
                                archive.addfile(member)
                            else:
                                member.size = len(payload)
                                archive.addfile(member, io.BytesIO(payload))
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_rejects_missing_or_foreign_certification_artifacts(self):
        for change in ('missing_sample', 'duplicate_sample', 'state', 'profile_phase',
                       'profile_owner', 'profile_requests', 'metadata', 'boundary', 'phase_order'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg, client, _ = self.cold_probe_fixture(root)
                if change in ('missing_sample', 'duplicate_sample', 'state'):
                    path = directory / 'remote_read_profiles.csv'
                    with path.open() as stream:
                        rows = list(csv.DictReader(stream))
                    if change == 'missing_sample':
                        rows.pop(0)
                    elif change == 'duplicate_sample':
                        rows[1]['sample'] = '0'
                    else:
                        rows[0]['state'] = 'primary-clean'
                    with path.open('w') as stream:
                        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                        writer.writeheader()
                        writer.writerows(rows)
                elif change.startswith('profile_'):
                    path = directory / 'node_profiles.yaml'
                    profile = json.loads(path.read_text())
                    if change == 'profile_phase':
                        profile['phase'] = 'warm-scalar-read-diagnostics'
                    elif change == 'profile_owner':
                        profile['nodes'].pop()
                    else:
                        profile['nodes'][0][1]['requests'] -= 1
                    path.write_text(json.dumps(profile))
                elif change == 'metadata':
                    path = directory / 'campaign.txt'
                    path.write_text(path.read_text().replace('transport_diagnostics=false', 'transport_diagnostics=true'))
                else:
                    path = directory / 'read_path_evidence.csv'
                    lines = path.read_text().splitlines()
                    if change == 'boundary':
                        lines.pop()
                    else:
                        # Move the window phase before the other ordinary cold phases.
                        lines = lines[:1] + lines[-12:] + lines[1:-12]
                    path.write_text('\n'.join(lines) + '\n')
                with self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)

    def test_cold_auxiliary_rejects_unattributed_or_incomplete_local_windows(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg, client, servers = self.cold_probe_fixture(root)
            certificate = checkpoint_kv.cold_probe_certificate(directory, spec, cfg, client)
            for change in ('missing', 'extra', 'reason', 'peer', 'post', 'absent_peer'):
                changed = copy.deepcopy(client)
                if change == 'missing':
                    changed['records'].pop()
                elif change == 'extra':
                    changed['records'].append(changed['records'][0] | dict(window=99, peer='10.0.0.1:49999->10.0.1.1:7700'))
                elif change == 'reason':
                    changed['records'][0]['reason'] = 'session-drop'
                elif change == 'peer':
                    changed['records'][0]['peer'] = '10.0.0.1:43000->10.9.9.9:7700'
                elif change == 'post':
                    changed['records'][0]['data_posts'] += 1
                else:
                    changed['records'][0]['peer'] = None
                with self.subTest(role='client', change=change), self.assertRaises(ValueError):
                    checkpoint_kv.cold_probe_certificate(directory, spec, cfg, changed)
            for change in ('missing', 'extra', 'reason', 'peer', 'post'):
                changed = copy.deepcopy(servers[0])
                if change == 'missing':
                    changed['records'].pop()
                elif change == 'extra':
                    changed['records'].append(changed['records'][0] | dict(window=99, peer='10.0.1.1:7700->10.0.0.1:49999'))
                elif change == 'reason':
                    changed['records'][0]['reason'] = 'profile-reset'
                elif change == 'peer':
                    changed['records'][0]['peer'] = '10.0.1.1:7700->10.0.0.2:43000'
                else:
                    changed['records'][0]['credit_posts'] += 1
                with self.subTest(role='storage', change=change), self.assertRaises(ValueError):
                    checkpoint_kv.validate_cold_probe_storage(changed, certificate, 0)

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
        self.assertEqual(len(planned), 30)
        for transport in ('tcp', 'efa', 'efa-direct'):
            rows = [spec for spec in planned if spec['transport'] == transport]
            self.assertEqual(len(rows), 10)
            self.assertEqual(sum(spec['transport_floor'] for spec in rows), 1)

    def diagnostic_fixture(self, root, transport='efa-native', mode='tcp', cold=True):
        directory = Path(root)
        spec = dict(shape=[256, 1, 1], repetition=3, transport=transport,
                    cold=cold, diagnostics=True, cached=False, authority_resident_batches=None)
        native = mode == 'native-sync'
        identity = dict(backend='native_value_wal_barrier' if native else matrix.TRANSPORT_BACKENDS[transport],
                        value_bytes=256, batch_size=1, window=1, repetition=0)
        def table(filename, fields, applicable):
            states = ['warm'] + (['cache-evicted'] if cold else []) + [
                'reopened-warm' if native else 'reopened-cache-cleared']
            with (directory / filename).open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=[*identity, 'state', 'sample', *fields])
                writer.writeheader()
                if applicable:
                    for state in states:
                        for sample in range(256):
                            values = dict.fromkeys(fields, 0)
                            values.update(index_ns=1, whole_call_ns=100)
                            if native:
                                values.update(wal_and_decode_ns=2, value_cache_hit='false')
                            else:
                                values.update(cache_misses=1, exchange_ns=10, server_record_read_ns=2)
                            writer.writerow(identity | dict(state=state, sample=sample) | values)
        table('native_read_profiles.csv',
              ['index_ns', 'wal_and_decode_ns', 'value_cache_hit', 'whole_call_ns'], native)
        table('remote_read_profiles.csv', [
            'index_ns', 'dispatch_ns', 'wait_ns', 'actor_queue_ns', 'actor_service_ns', 'actor_return_ns',
            'request_encode_ns', 'exchange_ns', 'response_decode_ns', 'record_verify_ns',
            'server_handler_ns', 'server_inventory_wait_ns', 'server_authority_lookup_ns',
            'server_record_read_ns', 'cache_hits', 'cache_misses', 'whole_call_ns', 'inline_read_ns',
            'server_response_recheck_wait_ns', 'server_response_recheck_ns',
            'server_response_recheck_samples', 'server_response_recheck_deferrals'], not native)
        phases = ['load-write-diagnostics', 'warm-scalar-read-diagnostics',
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
            with self.assertRaisesRegex(ValueError, 'uncached read'):
                checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))

    def test_diagnostic_idle_owners_are_not_missing_active_owner_profiles(self):
        def idle(profile):
            return {key: idle(value) if isinstance(value, dict) else 0
                    for key, value in profile.items()}
        for transport in ('tcp', 'efa', 'efa-direct', 'efa-native'):
            with self.subTest(transport=transport), tempfile.TemporaryDirectory() as root:
                spec, records = self.diagnostic_fixture(root, transport=transport)
                path = Path(root) / 'node_profiles.yaml'
                owner = records[2]['nodes'][0]
                owner[1] = idle(owner[1])
                def save():
                    path.write_text(''.join('---\n' + json.dumps(row) + '\n' for row in records))
                save()
                checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))
                owner[1]['client']['requests'] = 1
                save()
                with self.assertRaisesRegex(ValueError, 'owner/client activity'):
                    checkpoint_kv.validate_diagnostic_evidence(root, spec, 'tcp', list(range(5)))
                owner[1]['requests'] = 1
                save()
                with self.assertRaisesRegex(ValueError, 'positive timing population'):
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
                with self.assertRaisesRegex(ValueError, 'positive timing population'):
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
                        for _, profile in records[2]['nodes']:
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
                batch_size=1, window=1, repetition=0, transport='efa-direct',
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
        spec = dict(diagnostics=True, transport='tcp')
        identity = dict(backend='experimental_tcp_authority', transport='tcp',
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
