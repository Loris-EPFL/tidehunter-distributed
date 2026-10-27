"""Behavioral checks; no services, RDMA hardware or subprocesses are used."""

import argparse
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import qualify


TOKEN = "a" * 32
FIRST = bytes(range(256)) + b"\x00original\r\n" + TOKEN.encode("ascii")
SECOND = TOKEN.encode("ascii") + b"overwrite\x00\r\n" + bytes(reversed(range(256)))


class ScriptedReplies:
    """Protocol observations for testing failure/cleanup decisions, not an RDMA backend."""

    def __init__(self, replies, evidence):
        self.replies = iter(replies)
        self.calls = []
        self.evidence = evidence

    def call(self, *command, **options):
        self.calls.append((command, options))
        result = next(self.replies)
        if isinstance(result, Exception):
            self.evidence["commands"].append({"operation": command[0], "failure": "command_timeout"})
            raise result
        return result


class QualificationBehavior(unittest.TestCase):
    def setUp(self):
        identifier = patch.object(qualify.uuid, "uuid4", return_value=SimpleNamespace(hex=TOKEN))
        identifier.start()
        self.addCleanup(identifier.stop)

    def test_exact_binary_overwrite_delete_and_missing(self):
        evidence = {"commands": []}
        client = ScriptedReplies([None, "OK", FIRST, "OK", SECOND, 1, None, 0, 0, None], evidence)
        qualify.qualify_values(client, evidence)
        self.assertTrue(evidence["functional"]["passed"])
        self.assertTrue(evidence["cleanup"]["verified_absent"])
        self.assertFalse(evidence["cleanup"]["mutation_outcome_uncertain"])
        key = evidence["functional"]["key"]
        self.assertRegex(key, r"^tidehunter:valkey-ib-qualification:[a-f0-9]{32}:value$")
        self.assertEqual(client.calls[1][0], ("SET", key, FIRST, "NX", "PX", "120000"))
        self.assertEqual(client.calls[3][0], ("SET", key, SECOND, "XX", "PX", "120000"))
        self.assertEqual(client.calls[2][1], {"raw": True})
        cleanup, options = client.calls[-2]
        self.assertEqual(cleanup, ("EVAL", qualify.CLEANUP_SCRIPT, "1", key, FIRST, SECOND))
        self.assertEqual(options, {"cleanup": True})
        for command, _ in client.calls:
            self.assertNotIn("FLUSHDB", command)
            self.assertNotIn("FLUSHALL", command)
            self.assertNotIn("SCAN", command)

    def test_non_nil_initial_observation_never_mutates_or_cleans(self):
        evidence = {"commands": []}
        client = ScriptedReplies([""], evidence)
        with self.assertRaisesRegex(qualify.QualificationError, "initial namespaced"):
            qualify.qualify_values(client, evidence)
        self.assertEqual(len(client.calls), 1)
        self.assertFalse(evidence["cleanup"]["attempted"])

    def test_binary_mismatch_still_cleans_owned_key(self):
        evidence = {"commands": []}
        client = ScriptedReplies([None, "OK", FIRST + b"\n", 1, None], evidence)
        with self.assertRaisesRegex(qualify.QualificationError, "binary GET"):
            qualify.qualify_values(client, evidence)
        self.assertFalse(evidence["functional"]["passed"])
        self.assertTrue(evidence["cleanup"]["verified_absent"])
        self.assertEqual(client.calls[-2][0][0], "EVAL")

    def test_nx_collision_is_not_unconditionally_deleted(self):
        evidence = {"commands": []}
        client = ScriptedReplies([None, None, -1], evidence)
        with self.assertRaisesRegex(qualify.QualificationError, "SET NX"):
            qualify.qualify_values(client, evidence)
        self.assertFalse(evidence["cleanup"]["verified_absent"])
        self.assertIn("not owned", evidence["cleanup"]["error"])
        self.assertFalse(any(command[0] == "DEL" for command, _ in client.calls))

    def test_cleanup_requires_actual_nil_not_empty_string(self):
        evidence = {"commands": []}
        client = ScriptedReplies([None, "OK", FIRST, "OK", SECOND, 1, None, 0, 0, ""], evidence)
        with self.assertRaisesRegex(qualify.QualificationError, "cleanup missing"):
            qualify.qualify_values(client, evidence)
        self.assertFalse(evidence["cleanup"]["verified_absent"])

    def test_timed_out_set_is_not_declared_clean_after_missing_read(self):
        evidence = {"commands": []}
        client = ScriptedReplies([None, qualify.QualificationError("SET timeout"), 0, None], evidence)
        with self.assertRaisesRegex(qualify.QualificationError, "SET timeout"):
            qualify.qualify_values(client, evidence)
        self.assertTrue(evidence["cleanup"]["mutation_outcome_uncertain"])
        self.assertFalse(evidence["cleanup"]["verified_absent"])
        self.assertIn("PX TTL", evidence["cleanup"]["note"])

    def test_boolean_is_not_an_exact_integer_delete_reply(self):
        with self.assertRaises(qualify.QualificationError):
            qualify.exact(True, 1, "DEL")


class MissingObservations(unittest.TestCase):
    def test_inventory_reports_missing_kernel_device_and_ipoib(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, missing = qualify.inventory("mlx5_0", "ib0", root / "sys", root / "dev")
        text = "\n".join(missing)
        self.assertIn("RDMA-CM character device", text)
        self.assertIn("Requested verbs device is absent", text)
        self.assertIn("no observed ACTIVE InfiniBand port", text)
        self.assertIn("No accessible uverbs", text)
        self.assertIn("not IPoIB", text)
        self.assertFalse(result["device_use_proven"])
        self.assertFalse(result["hardware_path_attested"])

    def test_efa_is_explicitly_incompatible_even_when_requested(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, missing = qualify.inventory("efa_0", "ib0", root / "sys", root / "dev")
        self.assertTrue(any("EFA/SRD" in item for item in missing))

    def test_preflight_reports_absent_client_and_diagnostics_without_running_them(self):
        args = qualify.parser().parse_args(["preflight", "--cli", "unavailable-valkey-cli"])
        evidence = {"commands": [], "missing": []}
        with patch.object(qualify.shutil, "which", return_value=None):
            qualify.probe(args, evidence, qualify.Recorder(evidence, 1))
        self.assertIn("valkey-cli executable is unavailable", evidence["missing"])
        self.assertTrue(any("diagnostic executable is unavailable: rdma" in item
                            for item in evidence["missing"]))
        self.assertEqual(evidence["commands"], [])

    def test_endpoint_and_device_validation_rejects_uris_options_and_loopback(self):
        for value in ("valkey://user:secret@host:6379", "127.0.0.1", "0.0.0.0", "::1"):
            with self.subTest(host=value), self.assertRaises(argparse.ArgumentTypeError):
                qualify.numeric_host(value)
        for value in ("../mlx5_0", "/dev/infiniband/uverbs0", "--device", ".."):
            with self.subTest(device=value), self.assertRaises(argparse.ArgumentTypeError):
                qualify.safe_name(value)

    def test_provenance_requires_pinned_source_and_matching_actual_cli_hash(self):
        source = {"release": qualify.RELEASE, "source_commit": qualify.SOURCE_COMMIT,
                  "source_url": "https://github.com/valkey-io/valkey/tree/" + qualify.SOURCE_COMMIT,
                  "cli_sha256": "a" * 64, "server_sha256": "b" * 64}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "provenance.json"
            path.write_text(json.dumps(source))
            result = qualify.validate_provenance(path, "a" * 64)
            self.assertFalse(result["server_binary_hash_observed_locally"])
            self.assertEqual(result["trust"], "operator_supplied_not_independently_authenticated")
            with self.assertRaisesRegex(qualify.QualificationError, "differs"):
                qualify.validate_provenance(path, "c" * 64)
            source["source_url"] = "https://user:secret@github.com/valkey-io/valkey"
            path.write_text(json.dumps(source))
            with self.assertRaisesRegex(qualify.QualificationError, "source_url"):
                qualify.validate_provenance(path, "a" * 64)

    def test_runtime_rejects_wrong_observed_server_version_before_mutations(self):
        evidence = {"commands": []}
        client = ScriptedReplies([b"valkey_version:0.0.0\r\nredis_build_id:abc\r\nserver_mode:standalone\r\n"], evidence)
        with self.assertRaisesRegex(qualify.QualificationError, "pinned Valkey version"):
            qualify.runtime_identity(client, argparse.Namespace(port=6379), evidence)
        self.assertEqual(len(client.calls), 1)


if __name__ == "__main__":
    unittest.main()
