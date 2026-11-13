import unittest
from unittest.mock import patch

import ack_contract
import cloud_layout
import industry_service
import local
from test_cloud_layout import fleet
from test_industry_service import valid_nodes
from test_valkey_lifecycle import evidence


class AcknowledgementContractTests(unittest.TestCase):
    def test_run_b_relaxed_arms_keep_five_storage_nodes(self):
        value = fleet()
        value['valkey_rdma_package'] = True
        for index, node in enumerate(value['nodes']):
            node['rdma_address'] = f'10.2.0.{index + 1}'
        metadata = {'source_commit': 'fe8d406c0b5a64ec897f6455e4de684629883764',
                    'artifacts': {'src/valkey-server': {'sha256': 'a' * 64}}}
        for backend, contract, fsync in (('kvrocks', 'page_cache', None),
                                         ('valkey_rdma', 'os_buffered', 'no'),
                                         ('valkey_rdma', 'periodic_sync', 'everysec')):
            with self.subTest(backend=backend, contract=contract), patch.object(
                    cloud_layout.valkey_service, 'identity', return_value=metadata):
                plan = cloud_layout.build_plan(value, backend, contract)
            self.assertEqual(plan['physical_storage_hosts'], 5)
            stores = [node for node in plan['nodes'] if node['index'] >= 0]
            self.assertEqual(len(stores), 5)
            for node in stores:
                self.assertEqual(node['ack_contract'], contract)
                if backend == 'kvrocks':
                    self.assertIn('rocksdb.write_options.sync no\n', node['files']['kvrocks.conf'])
                    self.assertIn('rocksdb.write_options.disable_wal no\n', node['files']['kvrocks.conf'])
                else:
                    args = node['native_valkey']['args']
                    self.assertEqual(args[args.index('--appendfsync') + 1], fsync)
                    self.assertEqual(args[args.index('--cluster-enabled') + 1], 'yes')

    def test_valkey_arguments_and_bidirectional_readback(self):
        for backend in ("valkey_tcp", "valkey_rdma"):
            for contract, fsync in (("synced", "always"), ("os_buffered", "no"), ("periodic_sync", "everysec")):
                args = local.valkey_server_args("127.0.0.1", 6379, 16379, True, 1024, contract)
                self.assertEqual(args[args.index("--appendfsync") + 1], fsync)
                nodes, endpoints = evidence()
                for node in nodes:
                    node["effective_config"]["appendfsync"] = fsync
                local.admit_valkey(nodes, endpoints, True, ack_contract=contract)
                for other in ("synced", "os_buffered", "periodic_sync"):
                    if other != contract:
                        with self.assertRaises(RuntimeError):
                            local.admit_valkey(nodes, endpoints, True, ack_contract=other)
                self.assertEqual(ack_contract.selected(backend, contract), contract)

    def test_kvrocks_arguments_and_bidirectional_readback(self):
        for contract, sync in (("synced", "yes"), ("page_cache", "no")):
            config = industry_service.kvrocks_config("127.0.0.1", 6379, 1024, contract)
            self.assertIn(f"rocksdb.write_options.sync {sync}\n", config)
            self.assertIn("rocksdb.write_options.disable_wal no\n", config)
            nodes, endpoints = valid_nodes("kvrocks")
            for node in nodes:
                node["effective_config"]["rocksdb.write_options.sync"] = sync
            industry_service.admit("kvrocks", nodes, endpoints, ack_contract=contract)
            with self.assertRaises(RuntimeError):
                industry_service.admit("kvrocks", nodes, endpoints, ack_contract="page_cache" if contract == "synced" else "synced")

    def test_unsupported_contracts_fail_before_service_creation(self):
        for backend in ("garnet", "cassandra", "tikv", "foundationdb", "valkey_tcp", "valkey_rdma"):
            with self.assertRaisesRegex(ValueError, "page_cache ack_contract is not supported"):
                cloud_layout.node_config(fleet(), backend, 0, "page_cache")
        with self.assertRaisesRegex(ValueError, "ack_contract must"):
            ack_contract.selected("kvrocks", "unknown")

    def test_default_configuration_and_relaxed_layout(self):
        for backend, contract in (("valkey_tcp", "periodic_sync"), ("kvrocks", "page_cache")):
            self.assertEqual(cloud_layout.build_plan(fleet(), backend),
                             cloud_layout.build_plan(fleet(), backend, "synced"))
            plan = cloud_layout.build_plan(fleet(), backend, contract)
            self.assertEqual(plan["ack_policy"], ack_contract.acknowledgement(backend, contract))
            self.assertEqual(plan["nodes"][1]["ack_contract"], contract)
            self.assertIn("--ack-contract", plan["nodes"][1]["actions"]["start"])
            if backend == "kvrocks":
                self.assertIn("rocksdb.write_options.sync no", plan["nodes"][1]["files"]["kvrocks.conf"])
