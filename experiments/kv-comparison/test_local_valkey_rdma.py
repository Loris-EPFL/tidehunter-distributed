import os
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import local_valkey_rdma


class LocalValkeyRdmaTests(unittest.TestCase):
    def test_paired_tcp_and_rdma_use_distinct_explicit_client_options(self):
        rdma = [f"10.2.0.7:{6380+i}" for i in range(5)]
        tcp = local_valkey_rdma.options_for_backend("valkey_tcp", 5, rdma)
        remote = local_valkey_rdma.options_for_backend("valkey_rdma", 5, rdma)
        self.assertEqual(tcp["client"], "libvalkey")
        self.assertEqual(tcp["nodes"], [f"127.0.0.{21+i}:{6379+i}" for i in range(5)])
        self.assertEqual(remote["nodes"], rdma)
        self.assertEqual(tcp["required_primary_nodes"], remote["required_primary_nodes"])

    def test_cleanup_attempts_all_units_and_records_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            state = {"directory": directory, "services": [{"unit": "first"}, {"unit": "second"}]}
            seen = []
            def stop(config):
                name = config["native_valkey"]["unit"]
                seen.append(name)
                if name == "second":
                    raise RuntimeError("foreign")
                return [{"unit": name}]
            with patch.object(local_valkey_rdma.valkey_service, "stop", side_effect=stop):
                with self.assertRaisesRegex(RuntimeError, "second"):
                    local_valkey_rdma.stop(state)
            self.assertEqual(seen, ["second", "first"])
            self.assertTrue((Path(directory) / "cleanup.json").is_file())

    def test_explicit_fabric_address_and_primary_ports(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(os.environ, {"VALKEY_RDMA_RUNTIME": directory, "VALKEY_RDMA_ADDRESS": "10.2.0.7"}), \
                patch.object(local_valkey_rdma.valkey_service, "identity", return_value={}):
            options = local_valkey_rdma.options(5)
            self.assertEqual(options["nodes"], [f"10.2.0.7:{6380+i}" for i in range(5)])
            self.assertEqual((options["required_primary_nodes"], options["required_replication_factor"]), (5, 1))
            self.assertTrue(options["cluster"])
            with patch.dict(os.environ, {"VALKEY_RDMA_ADDRESS": "127.0.0.1"}):
                with self.assertRaisesRegex(ValueError, "non-loopback"):
                    local_valkey_rdma.options(1)

    def test_missing_explicit_runtime_is_rejected(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "VALKEY_RDMA_RUNTIME"):
                local_valkey_rdma.options(1)
