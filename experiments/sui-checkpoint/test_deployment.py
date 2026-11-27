"""Generated Sui deployment documents must parse as the current engine schema.

Every case writes `config.json` and one launch file per owner, checks the exact
field sets in Python, and then hands the same files to the sealed
`kv-research --experimental-validate-config` entry point (no target, socket or
WAL is opened). The binary comes from `KV_RESEARCH_BIN`, or a release build of
this checkout.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import yaml

import deployment

CONFIG_FIELDS = {'root', 'databases'}
DATABASE_FIELDS = {'storage', 'endpoints', 'tls', 'timeout', 'max_admitted_batches',
                   'max_pending_batches', 'max_pending_bytes', 'pipelined_writes',
                   'write_profile_limit', 'read_cache_bytes', 'read_memory_allowance_bytes',
                   'read_lanes', 'max_pending_reads', 'max_pending_read_bytes', 'live_nvme'}
STORAGE_FIELDS = {'partitions', 'batch_limits', 'group_limits', 'fragment_bytes',
                  'wal_file_bytes', 'max_batches_per_partition',
                  'max_reference_metadata_bytes'}
LIVE_FIELDS = {'host_nqn', 'host_id', 'owner_identities', 'max_inflight_requests',
               'max_inflight_bytes', 'connect_timeout_ms', 'io_timeout_ms',
               'leased_durability', 'inline_reads', 'queue_geometry'}
LAUNCH_FIELDS = {'partition', 'host', 'tls', 'limits', 'listen', 'ready_file', 'stop_file',
                 'launch_nonce', 'durability'}
HOST_FIELDS = {'root', 'max_databases', 'live_target_pool', 'wal_member_read_policy',
               'wal_prewrite', 'max_device_reads_per_status', 'durability'}
POOL_FIELDS = {'address', 'transport', 'public_port_base', 'public_service_base',
               'commissioning_port_base', 'commissioning_service_base'}
CAMPAIGN_NAMESPACES = ('live/checkpoints', 'live/store/epoch_0', 'live/store/perpetual')
# Removed from RemoteDatabaseConfig one or more schema generations ago.
REMOVED = {'preparation_workers', 'scan_prefetch_pages', 'max_index_bytes',
           'history_persistence', 'max_epochs', 'drop_first_append_response'}


class GeneratedDocumentsAreCurrent(unittest.TestCase):
    def binary(self):
        candidate = os.environ.get('KV_RESEARCH_BIN')
        if candidate:
            path = Path(candidate)
        else:
            path = Path(__file__).resolve().parents[3] / 'target/release/kv-research'
        if not path.is_file():
            self.fail(f'build kv-research (or set KV_RESEARCH_BIN); {path} is missing')
        return path

    def assert_documents(self, directory, config, nodes, *, transport, owners, tier, wal):
        config = json.loads((directory / 'config.json').read_text())
        self.assertEqual(set(config), CONFIG_FIELDS)
        canonical = deployment.canonical_transport(transport)
        for namespace, database in config['databases'].items():
            self.assertEqual(set(database), DATABASE_FIELDS, namespace)
            self.assertEqual(set(database['storage']), STORAGE_FIELDS, namespace)
            self.assertNotIn('database', database['storage'])
            self.assertEqual(set(database['live_nvme']), LIVE_FIELDS, namespace)
            self.assertEqual(database['tls']['transport'], canonical)
            self.assertEqual(database['live_nvme']['leased_durability'], tier)
            self.assertEqual(sorted(database['live_nvme']['owner_identities']),
                             [str(partition) for partition in range(owners)])
            for value in database['live_nvme']['owner_identities'].values():
                self.assertEqual(set(value), {'host_nqn', 'host_id'})
                self.assertTrue(value['host_nqn'].startswith('nqn.'))
            self.assertFalse(REMOVED & set(database))
            self.assertEqual(database['storage']['fragment_bytes'],
                             (128 if wal == 'engine_default' else 16) * deployment.MIB)
            expected_live = 'rdma' if canonical['kind'] == 'ofi' and \
                canonical['fabric']['backend'] == 'verbs' else 'tcp'
            for node in (row for row in nodes if row['namespace'] == namespace):
                launch = yaml.safe_load(Path(node['launch']).read_text())
                self.assertEqual(set(launch), LAUNCH_FIELDS)
                host = launch['host']
                required = HOST_FIELDS | ({'trusted_coordinator_id'}
                                          if deployment.is_trusted(canonical) else set())
                self.assertEqual(set(host), required)
                self.assertEqual(set(host['live_target_pool']), POOL_FIELDS)
                self.assertEqual(host['live_target_pool']['transport'], expected_live)
                self.assertEqual(host['live_target_pool']['address'], node['host'])
                self.assertEqual(host['wal_member_read_policy'], 'async')
                self.assertEqual(launch['durability'], {'tier': tier})
                self.assertEqual(launch['tls']['transport'], canonical)
                self.assertEqual(node['owner_identity'],
                                 database['live_nvme']['owner_identities'][str(node['partition'])])

    def assert_pools(self, config, nodes):
        """Every database process on one host owns a distinct port window."""
        index = {namespace: number for number, namespace
                 in enumerate(config['databases'])}
        groups = {}
        for node in nodes:
            pool = node['live_target_pool']
            groups.setdefault((node['host'], node['partition']), []).append(
                (index[node['namespace']], node['namespace'], pool))
            self.assertEqual(pool['public_port_base'],
                             1000 + deployment.LIVE_TARGET_WINDOW * node['partition']
                             + deployment.LIVE_TARGET_STRIDE * index[node['namespace']])
            self.assertEqual(pool['public_service_base'],
                             4420 + deployment.LIVE_TARGET_WINDOW * node['partition']
                             + deployment.LIVE_TARGET_STRIDE * index[node['namespace']])
        for (host, partition), rows in groups.items():
            numbers = [pool[field] for _, _, pool in rows for field in POOL_FIELDS
                       if field not in ('address', 'transport')]
            self.assertEqual(len(numbers), len(set(numbers)),
                             f'{host} partition {partition} shares a pool')
            self.assertEqual(len(rows), len(config['databases']))
            for _number, _namespace, pool in rows:
                for field, base in (('public_port_base', 1000),
                                    ('commissioning_port_base', 1016),
                                    ('public_service_base', 4420),
                                    ('commissioning_service_base', 4436)):
                    floor = base + deployment.LIVE_TARGET_WINDOW * partition
                    self.assertTrue(floor <= pool[field] < floor + deployment.LIVE_TARGET_SLOTS,
                                    f'{host} partition {partition} {field} leaves its window')

    def validate_with_engine(self, binary, directory, config, nodes):
        command = [str(binary), '--experimental-validate-config',
                   str(directory / 'config.json'),
                   *[node['launch'] for node in nodes]]
        completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, timeout=60)
        self.assertEqual(completed.returncode, 0,
                         f'engine rejected generated documents: {completed.stderr}')

    def test_every_generated_document_parses_as_the_current_schema(self):
        binary = self.binary()
        cases = [('tcp', 2, 'harness', 'native'),
                 ('tcp-trusted', 2, 'engine_default', 'synchronous'),
                 ('efa', 2, 'harness', 'native'),
                 ('efa-native', 5, 'engine_default', 'synchronous'),
                 ('fabric-tcp-native', 2, 'harness', 'native')]
        for transport, owners, wal, tier in cases:
            with self.subTest(transport=transport, owners=owners, wal=wal, tier=tier):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    schema = {name: {'key_spaces': []} for name in CAMPAIGN_NAMESPACES}
                    hardware = transport in deployment.HARDWARE_TRANSPORTS
                    software = transport in deployment.SOFTWARE_TRANSPORTS
                    hosts = ([f'10.0.0.{index + 2}' for index in range(owners)]
                             if hardware else ['127.0.0.1'] * owners)
                    config, nodes = deployment.prepare(root / 'deployment', schema,
                        root / 'remote', shape_yamls={name: 'key_spaces: []\n' for name in schema},
                        hosts=hosts, owners=owners, port=24000, transport=transport,
                        coordinator_cache=8 * deployment.MIB, shard_cache=0,
                        guest_root=root / 'guest' if hardware and not software else None,
                        owner_tier=tier, owner_wal=wal)
                    self.assert_documents(root / 'deployment', config, nodes,
                                          transport=transport, owners=owners, tier=tier, wal=wal)
                    self.assert_pools(config, nodes)
                    self.validate_with_engine(binary, root / 'deployment', config, nodes)

    def test_the_campaign_never_shares_a_pool_on_one_host(self):
        """Three databases and two owners: six processes, six distinct pools."""
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(deployment, 'certificates',
                                  lambda path, owners: path.mkdir()):
            root = Path(tmp)
            schema = {name: {} for name in CAMPAIGN_NAMESPACES}
            config, nodes = deployment.prepare(
                root / 'deployment', schema, root / 'remote',
                shape_yamls={name: 'key_spaces: []\n' for name in schema},
                hosts=['127.0.0.1'] * 2, owners=2, port=24000, transport='tcp',
                coordinator_cache=0, live_transport='rdma')
            self.assert_pools(config, nodes)
            table = {}
            for node in nodes:
                launch = yaml.safe_load(Path(node['launch']).read_text())
                self.assertEqual(launch['host']['max_databases'], 1)
                table[node['namespace'], node['partition']] = tuple(
                    launch['host']['live_target_pool'][field]
                    for field in ('public_port_base', 'public_service_base',
                                  'commissioning_port_base', 'commissioning_service_base'))
            self.assertEqual(table, {
                ('live/checkpoints', 0): (1000, 4420, 1016, 4436),
                ('live/store/epoch_0', 0): (1002, 4422, 1018, 4438),
                ('live/store/perpetual', 0): (1004, 4424, 1020, 4440),
                ('live/checkpoints', 1): (1064, 4484, 1080, 4500),
                ('live/store/epoch_0', 1): (1066, 4486, 1082, 4502),
                ('live/store/perpetual', 1): (1068, 4488, 1084, 4504)})

    def test_the_old_shared_pool_is_refused_at_generation(self):
        """A zero stride is today's layout: every database of a host on one pool."""
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(deployment, 'LIVE_TARGET_STRIDE', 0), \
                mock.patch.object(deployment, 'certificates',
                                  lambda path, owners: path.mkdir()):
            root = Path(tmp)
            schema = {name: {} for name in CAMPAIGN_NAMESPACES}
            with self.assertRaisesRegex(ValueError, 'overlap across databases'):
                deployment.prepare(
                    root / 'deployment', schema, root / 'remote',
                    shape_yamls={name: 'key_spaces: []\n' for name in schema},
                    hosts=['127.0.0.1'] * 2, owners=2, port=24000, transport='tcp',
                    coordinator_cache=0)
            self.assertFalse((root / 'deployment' / 'config.json').exists())

    def test_a_pool_outside_its_window_is_refused(self):
        pools = [('h', 0, deployment.live_target_pool('127.0.0.1', 'tcp', 0, 0))]
        deployment.check_live_target_pools(pools)
        outside = dict(pools[0][2], public_port_base=1000 + deployment.LIVE_TARGET_WINDOW)
        with self.assertRaisesRegex(ValueError, 'public_port_base leaves'):
            deployment.check_live_target_pools([('h', 0, outside)])
        with self.assertRaisesRegex(ValueError, 'at most 8 databases'):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                schema = {f'db-{index}': {} for index in range(9)}
                deployment.prepare(
                    root / 'deployment', schema, root / 'remote',
                    shape_yamls={name: 'key_spaces: []\n' for name in schema},
                    hosts=['127.0.0.1'], port=24000, transport='tcp',
                    coordinator_cache=0)

    def test_each_owner_launch_carries_a_fresh_nonce(self):
        """The engine refuses an external receipt whose launch nonce is empty."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, nodes = deployment.prepare(
                root / 'deployment', {'db': {}}, root / 'remote',
                shape_yamls={'db': 'key_spaces: []\n'}, hosts=['127.0.0.1'] * 2,
                port=24000, transport='tcp', coordinator_cache=0)
            nonces = [yaml.safe_load(Path(node['launch']).read_text())['launch_nonce']
                      for node in nodes]
        self.assertEqual(len(nonces), 2)
        self.assertEqual(len(set(nonces)), 2)
        self.assertTrue(all(len(nonce) == 32 for nonce in nonces))

    def test_obsolete_live_keys_and_mismatched_hosts_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for live in ({'addresses': ['127.0.0.1'] * 2},
                         {'export_mode': 'per_owner'},
                         {'host_nqn': 'nqn.x', 'host_id': [0] * 16}):
                with self.subTest(live=live), self.assertRaises(ValueError):
                    deployment.prepare(root / 'x', {'db': {}}, root / 'remote',
                        shape_yamls={'db': 'key_spaces: []\n'}, hosts=['127.0.0.1'] * 2,
                        port=24000, transport='tcp', coordinator_cache=0, live_nvme=live)
            with self.assertRaises(ValueError):
                deployment.prepare(root / 'y', {'db': {}}, root / 'remote',
                    shape_yamls={'db': 'key_spaces: []\n'},
                    hosts=['127.0.0.1', '127.0.0.1', '127.0.0.1'], owners=2,
                    port=24000, transport='tcp', coordinator_cache=0)


if __name__ == '__main__':
    unittest.main()
