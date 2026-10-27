"""Consumer-boundary evidence tests; fixtures never read ignored campaign logs."""
import csv
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import analyze_cloud as audit
import scalar_transport as profile
from portable_transport import select_transports, transport_for_case, write_transport_config


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def table(path, rows):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def threads(pid):
    counters = dict(user_ticks=0, system_ticks=0, voluntary_context_switches=0,
                    nonvoluntary_context_switches=0)
    return dict(pid=pid, ticks_per_second=100, threads={f'{pid}:1': dict(
        tid=pid, start_ticks=1, samples=2, first=counters,
        last={key: 1 for key in counters})})


def sensor(instance, after=False, partition=None):
    value = 100 if after else 1
    result = dict(instance_id=instance, utc=f'2026-10-11T00:00:0{int(after)}+00:00',
        meminfo='MemTotal: 1000000 kB\nMemAvailable: 500000 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n',
        vmstat='pswpin 0\npswpout 0\n',
        efa_counters={'/sys/class/infiniband/rdmap0s1/ports/1/hw_counters/send_bytes': str(value)},
        findmnt=json.dumps({'filesystems': [{'source': '/dev/nvme1n1'}]}),
        lsblk=json.dumps({'blockdevices': [{'path': '/dev/nvme1n1', 'type': 'disk',
                                            'model': 'Amazon EC2 NVMe Instance Storage'}]}),
        diskstats=f'259 1 nvme1n1 0 0 {value} 0 0 0 {value} 0 0 {value} 0\n')
    if partition is not None:
        result['partition'] = partition
    return result


def endpoint():
    return {**{f'fabric_open_{key}_max': 128 for key in
               ('requested_tx', 'requested_rx', 'selected_tx', 'selected_rx')},
            'fabric_open_provider_version_max': 13500416,
            'fabric_open_mtu_max': 4096,
            **{f'fabric_open_{stage}_worker_sum_ns': 1 for stage in profile.FABRIC_STAGES}}


def recovery_stats(spec, loaded=False, reused=False):
    result = dict(checkpoint_loaded=loaded, checkpoint_load_ns=1, proof_validation_ns=1,
        tail_replay_ns=1, checkpoint_commit_ns=1, connection_setup_ns=1, fencing_ns=1,
        tail_batches=spec.get('tail_batches', 0) if loaded else 0,
        tail_bytes=1 if loaded and spec.get('tail_batches') else 0,
        tls_config_ns=1, read_lane_setup_ns=1, connect_worker_ns=1, status_worker_ns=1,
        tls_worker_ns=1, fabric_worker_ns=1 if spec['transport'] == 'efa' else None)
    if spec['build'] == 'current':
        result.update(created_connections=0 if reused else 10, reused_connections=10 if reused else 0,
                      transport_checkout_ns=1, read_lane_wait_ns=1)
        if spec['transport'] == 'efa' and not reused:
            result.update(endpoint())
    return result


class Evidence:
    """Small actual CSV/JSON/tar archives, not mocks of the validators under test.

    Schemas follow the October 11 scalar TCP/recovery and preserved RAM artifacts;
    timings/counts are deliberately synthetic and deterministic.
    """
    def __init__(self, root, scope='full'):
        self.root = root
        self.cfg = dict(campaign='scalar-transport', checkpoint_scope=scope, efa=True, transports=['tcp'],
            run_id='th-0123456789abcdef', run_root='/mnt/tidehunter/th-0123456789abcdef',
            instance_type='g6.16xlarge', region='us-east-1', repetitions=3,
            export_reserve_seconds=600, deadline_utc='2026-10-11T03:00:00+00:00',
            coordinator_instance_id='i-00000000000000001',
            nodes=[dict(partition=p, instance_id=f'i-{p+2:017x}', private_ip=f'172.31.32.{p+10}')
                   for p in range(5)])
        save(root / 'campaign-config.json', self.cfg)
        requested_client = transport_for_case('tcp')
        requested_path = root / 'cloud-build-requested-transport.json'
        write_transport_config(requested_path, requested_client)
        requested_owners_path = root / 'cloud-build-requested-owner-transports.json'
        requested_owners_path.write_text(json.dumps(
            select_transports(requested_client, None, len(self.cfg['nodes']))) + '\n')
        requested_client_hash = hashlib.sha256(requested_path.read_bytes()).hexdigest()
        requested_owners_hash = hashlib.sha256(requested_owners_path.read_bytes()).hexdigest()
        tags = dict(Project='TideHunter', ManagedBy='sharded-wal-benchmark', RunId=self.cfg['run_id'])
        save(root / 'owned-instances.json', [dict(InstanceId=n['instance_id'],
            InstanceType='g6.16xlarge', Placement={'AvailabilityZone': 'us-east-1a'},
            NetworkInterfaces=[{'InterfaceType': 'efa'}], PrivateIpAddress=n.get('private_ip', '172.31.32.9'),
            Tags=[dict(Key=k, Value=v) for k, v in tags.items()])
            for n in [dict(instance_id=self.cfg['coordinator_instance_id'])] + self.cfg['nodes']])
        self.builds = {}
        for label, digest, source in [('baseline', 'a' * 64, profile.BASELINE_SOURCE_SHA256),
                                      ('current', 'b' * 64, profile.CURRENT_SOURCE_SHA256)]:
            features = 'experimental-fabric,test-utils'
            command = ['cargo', '+1.90.0', 'build', '--manifest-path',
                       self.base + f'/build-source/{label}/Cargo.toml', '--features', features]
            receipt = dict(label=label, binary_sha256=digest, source_tree_sha256=source,
                source_manifest_sha256='c' * 64, features=features,
                local_binary_sha256=profile.BASELINE_BINARY_SHA256 if label == 'baseline' else 'd' * 64,
                libfabric_version=profile.FABRIC_VERSION, libfabric_sha256='e' * 64,
                recovery_binary_sha256=('f' if label == 'current' else '0') * 64,
                target_directory=self.base + f'/build-target/{label}', command=command)
            self.builds[label] = receipt
            save(root / f'cloud-build-{label}.json', receipt)
            save(root / f'build-{label}.command.json', dict(exit_code=0, command=command))
            save(root / f'capability-{label}.command.json', dict(exit_code=0, command=[self.binary(label), '--help']))
            (root / f'capability-{label}.stdout.log').write_text(
                '--remote-config --prepare-external-case --transport-config --owner-transports\n')
        smoke_root = self.base + '/build-smoke/external-case'
        request_root = self.base + '/build-smoke/requested-configs/external-case'
        smoke = [self.binary('current'), '--prepare-external-case', smoke_root,
                 '--guest-case-root', smoke_root, '--remote-endpoints', self.endpoints,
                 '--transport-config', request_root + '/transport.json',
                 '--owner-transports', request_root + '/owner-transports.json',
                 '--values', '1024', '--batch-sizes', '1',
                 '--windows', '1', '--batches', '128', '--mixed', '32', '--reads', '512', '--repetitions', '1']
        save(root / 'cloud-build-preparation-smoke.command.json', dict(exit_code=0, command=smoke))
        save(root / 'cloud-build-pair-verified.json', dict(
            binary_sha256={label: r['binary_sha256'] for label, r in self.builds.items()},
            distinct_sources=True, distinct_binaries=True, capability_checks=True, prepare_command=smoke,
            prepared_configuration_sha256={name: '1' * 64 for name in
                ['remote.yaml', 'owner_transports.json'] + [f'node-{p}/launch.yaml' for p in range(5)]},
            requested_transport_sha256=requested_client_hash,
            requested_owner_transports_sha256=requested_owners_hash))

    @property
    def base(self):
        return self.cfg['run_root']

    @property
    def endpoints(self):
        return ','.join(n['private_ip'] + ':7700' for n in self.cfg['nodes'])

    def binary(self, label, recovery=False):
        return self.base + f'/cloud-bin/{label}/sharded_wal_' + ('recovery_bench' if recovery else 'bench')

    def archive(self, spec, partition, members):
        archive = self.root / 'nodes' / spec['case_id'] / f'node-{partition}.tar.gz'
        archive.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, 'w:gz') as stream:
            for name, text in members.items():
                data = text.encode()
                member = tarfile.TarInfo(name)
                member.size = len(data)
                stream.addfile(member, io.BytesIO(data))
        save(archive.with_name(archive.name + '.receipt.json'), dict(
            sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), bytes=archive.stat().st_size,
            key=f"test/results/nodes/{spec['case_id']}/node-{partition}.tar.gz"))

    def node_members(self, spec, partition):
        node = self.cfg['nodes'][partition]
        full = spec.get('restart') == 'full'
        pid = partition + (200 if full else 100)
        start = dict(pid=partition + 100, deadline_utc=self.cfg['deadline_utc'], ready='ready',
                     build=spec['build'], binary_sha256=self.builds[spec['build']]['binary_sha256'])
        members = {'before.json': json.dumps(sensor(node['instance_id'], partition=partition)),
            'after.json': json.dumps(sensor(node['instance_id'], True, partition)),
            'efa-provider.txt': 'prov_name: efa\n',
            'launch.yaml': 'read_workers: 2\nwal_member_read: async\nread_cache_bytes: 0\n'
                           'read_memory_allowance_bytes: 536870912\n',
            'process-status.txt': f'Pid: {pid}\nVmRSS: 100 kB\nVmHWM: 200 kB\nVmSwap: 0 kB\n',
            f'threads-{pid}.json': json.dumps(threads(pid)), 'start.json': json.dumps(start)}
        if full:
            members['restart.json'] = json.dumps(dict(case_id=spec['case_id'], partition=partition,
                deadline_utc=self.cfg['deadline_utc'], data_preserved=True, old_pid=partition + 100,
                new_pid=pid, ready='ready', ready_removed_before_start=True, node_restart_ns=100,
                build=spec['build'], binary_sha256=start['binary_sha256']))
        return members

    def prepare(self, spec):
        case = spec['case_id']
        save(self.root / (case + '-plan.json'), spec)
        case_root = self.base + '/cases/' + case
        command = [self.binary(spec['build']), '--prepare-external-case', case_root,
                   '--guest-case-root', case_root, '--remote-endpoints', self.endpoints]
        transport_config = self.base + '/cases/requested-configs/' + case + '/transport.json' if spec['build'] == 'current' else None
        command += profile.flags(spec, 'tcp', prepare=True, transport_config=transport_config)
        if spec['build'] == 'current':
            command += ['--owner-transports', self.base + '/cases/requested-configs/' + case + '/owner-transports.json']
        save(self.root / (case + '-prepare.command.json'), dict(exit_code=0, command=command,
             binary_sha256=self.builds[spec['build']]['binary_sha256']))
        if spec['build'] == 'current':
            transport = transport_for_case(spec['transport'])
            owner_transports = select_transports(transport, None, len(self.cfg['nodes']))
            write_transport_config(self.root / (case + '-transport-config.json'), transport)
            save(self.root / (case + '-owner-transports-requested.json'), owner_transports)
            save(self.root / (case + '-owner-transports.json'), owner_transports)
            remote = ('read_lanes: 1\nread_cache_bytes: 0\nread_memory_allowance_bytes: 536870912\n'
                      + 'tls:\n  transport: ' + json.dumps(transport) + '\n')
        else:
            remote = 'read_lanes: 1\nread_cache_bytes: 0\nread_memory_allowance_bytes: 536870912\n'
        (self.root / (case + '-remote.yaml')).write_text(remote)
        if spec['build'] == 'current':
            for node, owner_transport in zip(self.cfg['nodes'], owner_transports):
                (self.root / (case + f"-node-{node['partition']}-launch.yaml")).write_text(
                    'tls:\n  transport: ' + json.dumps(owner_transport) + '\n')
            launch_hashes = {str(node['partition']): hashlib.sha256(
                (self.root / (case + f"-node-{node['partition']}-launch.yaml")).read_bytes()).hexdigest()
                for node in self.cfg['nodes']}
            save(self.root / (case + '-prepared-transports.json'), dict(schema=1,
                requested_transport_sha256=hashlib.sha256(
                    (self.root / (case + '-transport-config.json')).read_bytes()).hexdigest(),
                requested_owner_transports_sha256=hashlib.sha256(
                    (self.root / (case + '-owner-transports-requested.json')).read_bytes()).hexdigest(),
                owner_transports_sha256=hashlib.sha256(
                    (self.root / (case + '-owner-transports.json')).read_bytes()).hexdigest(),
                launch_sha256=launch_hashes))
        for partition in range(5):
            self.archive(spec, partition, self.node_members(spec, partition))

    def process(self, name, spec, command, recovery=False):
        save(self.root / (name + '-process.command.json'), dict(exit_code=0, command=command, pid=42,
             peak_sampled_main_process_rss_kib=100,
             binary_sha256=self.builds[spec['build']]['recovery_binary_sha256' if recovery else 'binary_sha256']))
        save(self.root / (name + '-process.threads.json'), threads(42))

    def controller(self, name):
        for stage in ('before', 'after'):
            save(self.root / (name + '-' + stage + '.json'),
                 sensor(self.cfg['coordinator_instance_id'], stage == 'after'))

    def benchmark(self, spec, mode='tcp'):
        self.prepare(spec)
        name = spec['case_id'] + '-' + mode
        path = self.root / name
        path.mkdir()
        (path / 'COMPLETE').write_text('verified\n')
        command = [self.binary(spec['build']), '--output', self.base + '/results/' + name, '--backend', mode]
        transport_config = (self.base + '/cases/requested-configs/' + spec['case_id'] + '/transport.json'
                            if spec['build'] == 'current' else None)
        command += profile.flags(spec, mode, external=mode == 'tcp', transport_config=transport_config)
        if mode == 'tcp':
            command += ['--remote-config', self.base + '/cases/' + spec['case_id'] + '/remote.yaml']
        self.process(name, spec, command)
        self.controller(name)
        dimensions = dict(backend='native_value_wal_barrier' if mode == 'native-sync' else
            'experimental_' + spec['transport'] + '_authority', value_bytes=spec['value'],
            batch_size=spec['batch'], window=spec['window'], repetition=0)
        counts = dict.fromkeys(('open', 'full_scan', 'maintenance_flush_index', 'shutdown', 'reopen',
            'checkpoint_get', 'delete_window', 'mixed_get', 'mixed_write_window',
            'checkpoint_intervening_write', 'point_get_many'), 1)
        counts.update(load_write_window=spec['batches'] * spec['batch'], point_get=spec['reads'],
            point_get_window=spec['reads'], reopen_get_audit=spec['batches'] * spec['batch'])
        if mode == 'tcp' and spec['build'] == 'current':
            counts.update(reopen_reused_transport=1, reopen_reused_get_audit=spec['batches'] * spec['batch'])
        if spec['cold']:
            counts.update(dict.fromkeys(('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'), 1))
        table(path / 'samples.csv', [dimensions | dict(phase=p, sample=0, latency_ns=100, logical_operations=n)
                                    for p, n in counts.items()])
        table(path / 'summary.csv', [dimensions | dict(phase=p, samples=1, logical_operations=n,
            measured_api_ns=100, phase_wall_ns=1000, p50_ns=100, p95_ns='', p99_ns='',
            instrumented_phase_ops_per_sec=n * 1e6, transport=spec['transport'],
            durability='value_wal_file_and_directory_barrier', sample_unit='whole_phase',
            rate_scope='shared_read_write_wall' if p.startswith('mixed_') else 'phase_wall')
            for p, n in counts.items()])
        stages = []
        for phase in ('open_recovery', 'reopen_recovery', 'reopen_reused_transport_recovery'):
            if phase.startswith('reopen_reused') and spec['build'] != 'current':
                continue
            stats = recovery_stats(spec, phase != 'open_recovery', phase.startswith('reopen_reused'))
            stages.extend(dimensions | dict(phase=phase, counter=k, delta=int(v))
                          for k, v in stats.items() if v is not None)
        table(path / 'stages.csv', stages)
        for filename in ('remote_read_profiles.csv', 'exchange_profiles.csv', 'native_read_profiles.csv'):
            table(path / filename, [dimensions | dict(state='cache-evicted', whole_call_ns=100, value_cache_hit='false')])
        if spec['cold']:
            table(path / 'read_io.csv', [dimensions | dict(phase=phase, partition=p,
                pid=p + 100 if mode == 'tcp' else 42, physical_read_bytes=4096)
                for phase in ('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window')
                for p in (range(5) if mode == 'tcp' else [0])])
        roles = [('frontend', '', 42)] + [('storage', str(p), p + 100) for p in range(5)]
        if mode == 'tcp':
            counters = dict.fromkeys(('cache_budget_bytes', 'cache_metadata_bytes', 'cache_retained_bytes',
                'cache_hits', 'cache_misses', 'cache_evictions', 'wal_reads', 'wal_read_bytes',
                'inflight_requests', 'peak_inflight_requests', 'inflight_bytes', 'peak_inflight_bytes',
                'inflight_reads', 'peak_inflight_reads', 'cache_bypass_disabled', 'cache_bypass_eviction_limit',
                'cache_bypass_scan', 'cache_bypass_generation', 'cache_admitted_charge_bytes',
                'cache_served_accounted_bytes'), 0)
            table(path / 'read_path_evidence.csv', [dimensions | counters | dict(phase=phase, boundary=boundary,
                role=role, partition=p) for phase in ('point_get', 'point_get_window')
                for boundary in ('before', 'after') for role, p, _ in roles])
            table(path / 'write_windows.csv', [dimensions | dict(phase=phase, sample=0, transport=spec['transport'],
                preparation_admission_ns=1, storage_wait_ns=1, publication_ns=1, dispatch_ns=1,
                checkpoint_ns=1, foreground_checkpoint_ns=1, api_ns=100, checkpoint_count=1,
                foreground_checkpoint_count=1) for phase in
                ('load_write_window', 'delete_window', 'checkpoint_intervening_write', 'mixed_write_window')])
        else:
            roles = [('native', '', 42)]
        stages = ['opened', 'loaded', 'point_get_window'] + (['cache_evicted_get_window'] if spec['cold'] else [])
        table(path / 'read_resources.csv', [dict(stage=stage, role=role, partition=p, pid=pid,
            rss_bytes=100, peak_rss_bytes=120, user_cpu_ns=i * 100, system_cpu_ns=i * 20)
            for i, stage in enumerate(stages) for role, p, pid in roles])
        return name

    def recovery(self, spec):
        self.prepare(spec)
        name = spec['case_id'] + '-recovery'
        path = self.root / name
        path.mkdir()
        (path / 'COMPLETE').write_text('verified\n')
        self.controller(name)
        for action in ('seed', 'reopen'):
            row = dict(schema=1, action=action, value_bytes=8192, batch_size=32, checkpoint_batches=64,
                tail_batches=spec['tail_batches'], key_count=2048, verified_keys=2048 if action == 'reopen' else 0,
                abrupt_exit=action == 'seed', exact_retry_verified=action == 'reopen', open_ns=100,
                checkpoint_create_ns=0 if action == 'reopen' else 10,
                tail_append_ns=0 if action == 'reopen' else 10,
                verification_ns=10 if action == 'reopen' else 0,
                recovery_stats=recovery_stats(spec, action == 'reopen'))
            save(path / action / 'recovery.json', row)
            (path / action / 'COMPLETE').write_text('verified\n')
            command = [self.binary(spec['build'], True), '--action', action, '--remote-config',
                self.base + '/cases/' + spec['case_id'] + '/remote.yaml', '--output',
                self.base + '/results/' + name + '/' + action, '--value-bytes', '8192',
                '--checkpoint-batches', '64', '--tail-batches', str(spec['tail_batches'])]
            self.process(name + '-' + action, spec, command, recovery=True)
        save(path / 'service-restore.json', dict(restart=spec['restart'],
            os_cache_evicted=False, controller_service_restore_ns=1000, nodes_ready_ns=500))
        return name


class ScalarEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.evidence = Evidence(self.root)

    def read(self, name):
        return audit.read_case(self.root, name, profile.expected_cases()[name], 'scalar-transport')

    def test_complete_qualification_accepts_actual_archive_schemas(self):
        for spec in profile.qualification_cases():
            self.evidence.benchmark(spec)
        receipt = audit.validate_qualification(self.root)
        self.assertTrue(receipt['passed'], receipt['errors'])
        self.assertEqual(len(receipt['case_ids']), 4)
        self.assertEqual(receipt['expected_node_archives'], 20)

    def test_foreground_checkpoint_wait_is_not_an_exclusive_checkpoint_subset(self):
        spec = next(c for c in profile.qualification_cases() if c['build'] == 'current')
        name = self.evidence.benchmark(spec)
        path = self.root / name / 'write_windows.csv'
        rows = audit.cluster_ram.read_csv(path)
        for row in rows:
            row['foreground_checkpoint_ns'] = '90'
            row['checkpoint_ns'] = '1'
        table(path, rows)
        self.read(name)
        rows[0]['checkpoint_ns'] = '101'
        table(path, rows)
        with self.assertRaisesRegex(ValueError, 'exclusive write parent spans'):
            self.read(name)

    def test_current_qualification_rejects_missing_or_unsupported_endpoint_evidence(self):
        spec = next(c for c in profile.qualification_cases() if c['build'] == 'current')
        name = self.evidence.benchmark(spec)
        path = self.root / name / 'stages.csv'
        rows = audit.cluster_ram.read_csv(path)
        self.read(name)
        mutations = [(f'fabric_open_{key}_max', value) for key in
                     ('requested_tx', 'requested_rx', 'selected_tx', 'selected_rx') for value in (None, '256')]
        mutations += [('fabric_open_provider_version_max', str((2 << 16) | 6)),
                      ('fabric_open_provider_version_max', None), ('fabric_open_mtu_max', '2048')]
        mutations += [(f'fabric_open_{stage}_worker_sum_ns', None) for stage in profile.FABRIC_STAGES]
        for counter, value in mutations:
            with self.subTest(counter=counter, value=value):
                changed = [dict(row) for row in rows]
                target = next(row for row in changed if row['counter'] == counter and row['phase'] == 'open_recovery')
                if value is None:
                    changed.remove(target)
                else:
                    target['delta'] = value
                table(path, changed)
                with self.assertRaises(ValueError):
                    self.read(name)
        table(path, rows)

    def test_wrong_source_process_and_node_executables_fail_closed(self):
        spec = next(c for c in profile.qualification_cases() if c['build'] == 'current')
        name = self.evidence.benchmark(spec)
        self.read(name)
        for filename, key in [('cloud-build-current.json', 'source_tree_sha256'),
                              ('cloud-build-current.json', 'binary_sha256'),
                              (spec['case_id'] + '-prepare.command.json', 'binary_sha256'),
                              (name + '-process.command.json', 'binary_sha256')]:
            path = self.root / filename
            original = audit.read_json(path)
            with self.subTest(filename=filename, key=key):
                save(path, original | {key: '9' * 64})
                with self.assertRaises(ValueError):
                    self.read(name)
                save(path, original)
        members = self.evidence.node_members(spec, 0)
        start = json.loads(members['start.json'])
        for change in ({'binary_sha256': '9' * 64}, {'build': 'baseline'}):
            with self.subTest(change=change):
                self.evidence.archive(spec, 0, members | {'start.json': json.dumps(start | change)})
                with self.assertRaisesRegex(ValueError, 'node executable'):
                    self.read(name)

    def test_unplanned_scope_case_and_archive_are_rejected(self):
        spec = next(c for c in profile.main_cases() if c['workload'] == 'scalar' and c['build'] == 'current')
        name = self.evidence.benchmark(spec)
        for scope in ('capacity', 'recovery'):
            with self.subTest(scope=scope):
                save(self.root / 'campaign-config.json', self.evidence.cfg | {'checkpoint_scope': scope})
                with self.assertRaises(ValueError):
                    self.read(name)
                with self.assertRaises(ValueError):
                    audit.node_evidence(self.root, spec['case_id'], 0)
        save(self.root / 'campaign-config.json', self.evidence.cfg)
        with self.assertRaisesRegex(ValueError, 'unplanned'):
            profile.read_cloud_case(self.root, name, spec | {'lanes': 2}, 'tcp')
        info = profile.expected_cases()[name]
        with self.assertRaisesRegex(ValueError, 'unplanned'):
            audit.read_case(self.root, name, (info[0], 'baseline', *info[2:]), 'scalar-transport')

    def test_baseline_blank_percentiles_and_absent_optional_fields_stay_absent(self):
        spec = next(c for c in profile.qualification_cases() if c['build'] == 'baseline')
        name = self.evidence.benchmark(spec)
        path = self.root / name / 'summary.csv'
        rows = audit.cluster_ram.read_csv(path)
        for row in rows:
            for field in ('transport', 'sample_unit', 'rate_scope'):
                del row[field]
        table(path, rows)
        metrics = self.read(name)
        self.assertEqual(metrics['summary', 'open', 'p50_ns'], 100)
        self.assertNotIn(('summary', 'open', 'p95_ns'), metrics)
        self.assertNotIn(('stage', 'open_recovery', 'created_connections'), metrics)
        table(path, [row | {'transport': 'tcp'} for row in rows])
        with self.assertRaisesRegex(ValueError, 'transport identity'):
            self.read(name)

    def test_both_transports_and_restart_scopes_require_cold_process_recovery(self):
        for spec in profile.recovery_cases():
            with self.subTest(build=spec['build'], transport=spec['transport'], restart=spec['restart'], tail=spec['tail_batches']):
                name = self.evidence.recovery(spec)
                metrics = self.read(name)
                self.assertEqual(metrics['recovery', 'reopen', 'stats_tail_batches'], spec['tail_batches'])
                if spec['build'] == 'current':
                    self.assertEqual(metrics['recovery', 'reopen', 'stats_created_connections'], 10)
                    for action in ('seed', 'reopen'):
                        path = self.root / name / action / 'recovery.json'
                        row = audit.read_json(path)
                        save(path, row | {'recovery_stats': row['recovery_stats'] |
                                         {'created_connections': 0, 'reused_connections': 10}})
                        with self.assertRaisesRegex(ValueError, 'fresh transports'):
                            self.read(name)
                        save(path, row)
                if spec['restart'] == 'full':
                    members = self.evidence.node_members(spec, 0)
                    restart = json.loads(members['restart.json'])
                    self.evidence.archive(spec, 0, members | {'restart.json': json.dumps(restart | {'binary_sha256': '9' * 64})})
                    with self.assertRaisesRegex(ValueError, 'restarted node executable'):
                        self.read(name)

    def test_recovery_rejects_wrong_driver_geometry_and_transport(self):
        spec = next(c for c in profile.recovery_cases() if c['build'] == 'current' and c['transport'] == 'efa')
        name = self.evidence.recovery(spec)
        self.read(name)
        path = self.root / (name + '-reopen-process.command.json')
        original = audit.read_json(path)
        save(path, original | {'binary_sha256': self.evidence.builds['current']['binary_sha256']})
        with self.assertRaisesRegex(ValueError, 'recovery process executable'):
            self.read(name)
        save(path, original)
        path = self.root / name / 'reopen/recovery.json'
        original = audit.read_json(path)
        stats = dict(original['recovery_stats'])
        del stats['fabric_open_selected_rx_max']
        save(path, original | {'recovery_stats': stats})
        with self.assertRaisesRegex(ValueError, 'geometry'):
            self.read(name)
        save(path, original)
        path = self.root / (spec['case_id'] + '-prepare.command.json')
        row = audit.read_json(path)
        row['command'][row['command'].index('--transport-config') + 1] = '/wrong/transport.json'
        save(path, row)
        with self.assertRaises(ValueError):
            self.read(name)

    def test_pooled_and_cold_reopen_reports_never_alias(self):
        runs = {}
        for spec in [c for c in profile.main_cases() if c['workload'] == 'scalar'
                     and c['transport'] == 'tcp' and c['value'] == 1024 and c['rep'] == 0]:
            name = self.evidence.benchmark(spec)
            metrics = self.read(name)
            runs[name] = (profile.expected_cases()[name], metrics)
        rows = audit.compare_checkpoint(runs, profile)
        cold = next(r for r in rows if r['build'] == 'current' and r['phase'] == 'reopen'
                    and r['metric'] == 'phase_wall_ns')
        reused = next(r for r in rows if r['build'] == 'current' and r['phase'] == 'reopen_reused_transport'
                      and r['metric'] == 'phase_wall_ns')
        self.assertEqual(cold['new_over_old'], 1)
        self.assertIsNone(reused['old_median'])
        self.assertIsNone(reused['new_over_old'])
        current = next(c for c in profile.main_cases() if c['workload'] == 'scalar'
                       and c['transport'] == 'tcp' and c['value'] == 1024 and c['rep'] == 0 and c['build'] == 'current')
        name = current['case_id'] + '-tcp'
        path = self.root / name / 'stages.csv'
        rows = audit.cluster_ram.read_csv(path)
        table(path, rows + [rows[0] | dict(phase='reopen_reused_transport_recovery',
                                         counter='fabric_open_requested_tx_max', delta='128')])
        with self.assertRaisesRegex(ValueError, 'invent new EFA'):
            self.read(name)


if __name__ == '__main__':
    unittest.main()
