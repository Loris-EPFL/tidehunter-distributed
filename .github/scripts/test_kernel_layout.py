"""The layout and owner-restart proofs' pure parts: scope, teardown, spec and verdict."""
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / ".github/scripts"))
import kernel_nvme_tcp_ci as kernel


def write_launch(path, root, port):
    """One generated owner launch: its host root and its public pool slot."""
    Path(path).write_text(json.dumps(
        {"host": {"root": str(root), "live_target_pool": {"public_port_base": port}}}))


def write_links(port, names):
    port.mkdir(parents=True)
    (port / "subsystems").mkdir()
    for index, name in enumerate(names):
        (port / "subsystems" / name).symlink_to(f"../../../subsystems/sub-{index}")


class LayoutDefect(unittest.TestCase):
    def configfs(self, root):
        (root / "ports").mkdir(parents=True)
        (root / "subsystems").mkdir()
        (root / "hosts").mkdir()
        return root

    def test_a_distinct_pool_per_database_is_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            write_links(root / "ports" / "1000",
                        [kernel.LAYOUT_SCOPE + "aa" * 16 + "-p00000000-sub-1"])
            write_links(root / "ports" / "1002",
                        [kernel.LAYOUT_SCOPE + "bb" * 16 + "-p00000000-sub-2"])
            for node, hexes, port in (("a", "aa" * 16, 1000), ("b", "bb" * 16, 1002)):
                write_launch(root / f"{node}.yaml", root / "roots" / hexes, port)
                (root / "roots" / hexes / f"db-{hexes}").mkdir(parents=True)
            nodes = [{"namespace": "a", "partition": 0, "launch": str(root / "a.yaml")},
                     {"namespace": "b", "partition": 0, "launch": str(root / "b.yaml")}]
            with patch.object(kernel, "CONFIGFS", root):
                self.assertIsNone(kernel.layout_defect(
                    nodes, {"a/0": "aa" * 16, "b/0": "bb" * 16}))

    def test_a_foreign_subsystem_on_a_port_is_a_defect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            write_links(root / "ports" / "1000", [
                kernel.LAYOUT_SCOPE + "aa" * 16 + "-p00000000-sub-1",
                kernel.LAYOUT_SCOPE + "cc" * 16 + "-p00000000-sub-3"])
            node = {"namespace": "a", "partition": 0, "launch": str(root / "a.yaml")}
            write_launch(node["launch"], root / "roots", 1000)
            (root / "roots" / f"db-{'aa' * 16}").mkdir(parents=True)
            with patch.object(kernel, "CONFIGFS", root):
                defect = kernel.layout_defect([node], {"a/0": "aa" * 16})
        self.assertIn("holds another database's subsystem", defect)

    def test_a_port_that_published_nothing_is_a_defect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            node = {"namespace": "a", "partition": 0, "launch": str(root / "a.yaml")}
            write_launch(node["launch"], root / "roots", 1000)
            (root / "roots" / f"db-{'aa' * 16}").mkdir(parents=True)
            with patch.object(kernel, "CONFIGFS", root):
                defect = kernel.layout_defect([node], {"a/0": "aa" * 16})
        self.assertIn("published no subsystem", defect)

    def test_teardown_reports_the_residual_objects_of_each_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            residual = kernel.LAYOUT_SCOPE + "aa" * 16 + "-p00000000-sub-1"
            (root / "subsystems" / residual).mkdir()
            write_links(root / "ports" / "1000", [residual])
            with patch.object(kernel, "CONFIGFS", root):
                found = kernel.layout_teardown_defect({"aa" * 16})
        self.assertEqual(len(found), 2)


class NegativeSpecs(unittest.TestCase):
    """Both negative runs must name their own single endpoint and receipt."""

    def test_each_negative_owner_carries_its_own_count_and_receipt(self):
        deployment, specs = kernel.campaign_modules()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            documents = {}
            with patch.object(deployment, "certificates",
                              lambda path, owners: path.mkdir()):
                for name, namespace, port in (("a", kernel.LAYOUT_NAMESPACES[0],
                                               kernel.LAYOUT_PORT),
                                              ("b", kernel.LAYOUT_NAMESPACES[1],
                                               kernel.LAYOUT_PORT + 10)):
                    config, _nodes = deployment.prepare(
                        root / name, {namespace: {}}, root / ("remote-" + name),
                        shape_yamls={namespace: "key_spaces: []\n"},
                        hosts=["127.0.0.1"], owners=1, port=port, transport="tcp",
                        coordinator_cache=0, live_transport="tcp")
                    documents[name] = config["databases"][namespace]
            for index, name in ((0, "a"), (1, "b")):
                receipt = specs.receipt_path(root / ("negative-" + name), "native", 0)
                spec = kernel.negative_spec(specs, documents[name],
                                            root / ("negative-" + name), index, receipt)
                options = spec["backend"]["options"]
                with self.subTest(name=name):
                    self.assertEqual(options["owners"], 1)
                    self.assertEqual(len(options["external_remote_config"]["endpoints"]), 1)
                    self.assertEqual(options["external_owner_durability_receipts"],
                                     [str(receipt)])
                    self.assertEqual(spec["backend"]["options"]["path"],
                                     str(root / ("negative-" + name) / "stage3" / "db"
                                         / f"owner-layout-{index}" / "coordinator"))


class RestartReopen(unittest.TestCase):
    """The owner-restart proof: geometry, spec shape and the reopen verdict."""

    def setUp(self):
        self.deployment, self.specs = kernel.campaign_modules()

    def database(self, root):
        name = kernel.LAYOUT_NAMESPACES[0]
        with patch.object(self.deployment, "certificates",
                          lambda path, owners: path.mkdir()):
            config, nodes = self.deployment.prepare(
                root / "deployment", {name: {"key_spaces": []}}, root / "remote",
                shape_yamls={name: "key_spaces: []\n"},
                hosts=["127.0.0.1"], owners=1, port=kernel.LAYOUT_PORT,
                transport="tcp", coordinator_cache=0, live_transport="tcp")
        self.assertEqual(len(nodes), 1)
        return config["databases"][name], nodes[0]

    def test_the_killed_run_gates_and_the_reopen_reuses_its_coordinator(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            document, node = self.database(root)
            kernel.restart_geometry(document)
            storage = document["storage"]
            self.assertEqual(storage["wal_file_bytes"], 8 * 1024**2)
            self.assertEqual(storage["fragment_bytes"], 4 * 1024**2)
            # The qualification bounds must fit the fragment, not the campaign
            # defaults (8 MiB batches, 12 MiB groups) that prepare() writes.
            self.assertEqual(storage["batch_limits"],
                             {"max_operations": 32, "max_encoded_bytes": 3 * 1024**2})
            self.assertEqual(storage["group_limits"],
                             {"max_batches": 32, "max_encoded_bytes": 4 * 1024**2 - 8})
            coordinator = root / "db" / "coordinator"
            killed = kernel.restart_spec(
                self.specs, document, root, case_id="owner-restart",
                coordinator=coordinator, gated=True, reopen=False)
            reopened = kernel.restart_spec(
                self.specs, document, root, case_id="owner-restart-reopen",
                coordinator=coordinator, gated=False, reopen=True)
        # The killed run must stop at a write gate with preloaded writes.
        self.assertIn("measurement_gate", killed)
        self.assertTrue(killed["phases"]["preload"])
        self.assertEqual(killed["workload"]["mix"]["put"], 100)
        self.assertEqual(killed["backend"]["options"]["path"], str(coordinator))
        # The prepared database has one partition, so both specs name one owner
        # and one receipt; the shared builder defaults to the campaign's two.
        for spec in (killed, reopened):
            options = spec["backend"]["options"]
            self.assertEqual(options["owners"], 1)
            self.assertEqual(options["external_owner_durability_receipts"],
                             [str(self.specs.receipt_path(root, "native", 0))])
        # The reopen must not wait at any gate and must not preload: its first
        # live use is the replay under test.
        self.assertNotIn("measurement_gate", reopened)
        self.assertNotIn("measurement_completion_gate", reopened)
        self.assertFalse(reopened["phases"]["preload"])
        self.assertEqual(reopened["phases"]["warmup_operations"], 0)
        self.assertEqual(reopened["backend"]["options"]["path"], str(coordinator))
        # A stale ready marker would make the binary refuse its own gate.
        self.assertFalse(Path(killed["measurement_gate"]["ready_file"]).exists())
        self.assertEqual(killed["backend"]["options"]["external_remote_config"],
                         reopened["backend"]["options"]["external_remote_config"])
        self.assertEqual(node["namespace"], kernel.LAYOUT_NAMESPACES[0])

    def storage(self, *, fragment=4 * 1024**2, wal=8 * 1024**2,
                batch=3 * 1024**2, group=None):
        return {"batch_limits": {"max_operations": 32, "max_encoded_bytes": batch},
                "group_limits": {"max_batches": 32,
                                 "max_encoded_bytes": (fragment - 8) if group is None else group},
                "fragment_bytes": fragment, "wal_file_bytes": wal}

    def test_restart_geometry_admission_rejects_the_campaign_bounds(self):
        # The campaign's 12 MiB group cannot fit a 4 MiB fragment: this is the
        # spec that failed before the write gate.
        with self.assertRaisesRegex(RuntimeError, "fragment does not hold the group"):
            kernel.admit_restart_geometry(self.storage(group=12 * 1024**2, batch=8 * 1024**2))
        # A batch above its group is refused as well.
        with self.assertRaisesRegex(RuntimeError, "group bound does not hold the batch"):
            kernel.admit_restart_geometry(self.storage(batch=6 * 1024**2))
        # The qualification values pass.
        kernel.admit_restart_geometry(self.storage())

    def evidence(self, *, tail=3, kind="reopen", retired=1, adoptable=1):
        recovery = {"tail_batches": tail}
        if retired is not None:
            recovery["retired_leftover_controllers"] = retired
        if adoptable is not None:
            recovery["retired_adoptable_leftovers"] = adoptable
        return {"start": {"coordinator_open": {"kind": kind, "recovery": recovery}}}

    def defect(self, evidence=None, stderr=""):
        return kernel.reopen_defect(
            self.evidence() if evidence is None else evidence, stderr)

    def test_a_reopen_that_retired_the_leftover_controller_is_clean(self):
        self.assertIsNone(self.defect())

    def test_a_reopen_that_retired_nothing_adoptable_is_a_defect(self):
        defect = self.defect(self.evidence(retired=1, adoptable=0))
        self.assertIn("retired no adoptable pre-restart controller", defect)

    def test_a_missing_adoptable_count_is_a_defect(self):
        defect = self.defect(self.evidence(adoptable=None))
        self.assertIn("retired_adoptable_leftovers=None", defect)

    def test_an_empty_replay_is_a_defect(self):
        defect = self.defect(self.evidence(tail=0))
        self.assertIn("replayed no WAL batch", defect)

    def test_the_lost_controller_and_short_read_markers_fail_the_reopen(self):
        for marker in kernel.RESTART_FORBIDDEN_STDERR:
            with self.subTest(marker=marker):
                defect = self.defect(stderr="error: " + marker)
                self.assertIn(marker, defect)

    def test_the_open_must_be_a_reopen(self):
        defect = self.defect(self.evidence(kind="create"))
        self.assertIn("not 'reopen'", defect)
        defect = self.defect({})
        self.assertIn("no measurement start boundary", defect)

    def test_controller_identities_use_the_sysfs_inode_not_the_ordinal(self):
        # Secondary evidence for the post-close leftover scan: a recycled
        # ordinal must not be mistaken for the same controller object.
        scope = "aa" * 16
        document = {"controllers": {
            "nvme3": {"subsysnqn": kernel.LAYOUT_SCOPE + scope + "-p00000000-sub-1"},
            "nvme4": {"subsysnqn": kernel.LAYOUT_SCOPE + "bb" * 16 + "-p00000000-sub-1"},
            "nvme5": {"subsysnqn": kernel.LAYOUT_SCOPE + scope + "-p00000001-sub-1"},
            # Listed by the inventory but gone from sysfs: no stable identity.
            "nvme6": {"subsysnqn": kernel.LAYOUT_SCOPE + scope + "-p00000000-sub-1"}}}
        with tempfile.TemporaryDirectory() as tmp:
            class_root = Path(tmp) / "nvme-class"
            for name in ("nvme3", "nvme5"):
                (class_root / name).mkdir(parents=True)
            meta = (class_root / "nvme3").stat()
            with patch.object(kernel, "NVME_CONTROLLERS", class_root):
                identities = kernel.controller_identities(document, scope, 0)
            self.assertEqual([row["controller"] for row in identities], ["nvme3"])
            self.assertEqual(identities[0]["sysfs_device"], meta.st_dev)
            self.assertEqual(identities[0]["sysfs_inode"], meta.st_ino)


class RestartOrchestration(unittest.TestCase):
    """The restart sequence with mocked processes: one owner, one kill, one reopen."""

    class Process:
        def __init__(self, name, pid, events):
            self.name = name
            self.pid = pid
            self.events = events
            self.returncode = None

        def poll(self):
            return self.returncode

        def kill(self):
            self.events.append(("kill", self.name))
            self.returncode = -9

        def wait(self, timeout=None):
            self.events.append(("wait", self.name))
            if self.returncode is None:
                self.returncode = 0
            return self.returncode

    def test_kill_at_the_write_gate_restarts_the_owner_and_reopens_once(self):
        deployment, specs = kernel.campaign_modules()
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state"
            state.mkdir()
            root = state / "owner-restart"
            events = []
            gate_path = {}
            coordinator_process = {}

            def owner(binary, node, log):
                Path(node["ready"]).parent.mkdir(parents=True, exist_ok=True)
                Path(node["ready"]).write_text("ready\n")
                Path(str(node["ready"]) + ".durability.json").write_text("{}\n")
                events.append(("owner", "start"))
                return self.Process("owner", 1000 + len(events), events)

            def coordinator(binary, spec_path, output, stdout_path, stderr_path):
                spec = json.loads(Path(spec_path).read_text())
                gate = Path(spec["measurement_gate"]["ready_file"])
                gate_path["path"] = gate
                gate.parent.mkdir(parents=True, exist_ok=True)
                gate.write_text("ready\n")
                events.append(("coordinator", "gate"))
                coordinator_process["p"] = self.Process("coordinator", 4242, events)
                return coordinator_process["p"]

            opened = {}

            def admission(binary, spec, run_root, name, timeout=120):
                opened["admitted"] = spec
                return subprocess.CompletedProcess([], 0, "", "")

            def reopen(binary, spec, run_root, name):
                events.append(("coordinator", "reopen"))
                opened["spec"] = spec
                output = Path(run_root) / f"{name}-out"
                output.mkdir(parents=True)
                evidence = {"start": {
                    "coordinator_open": {
                        "kind": "reopen",
                        "recovery": {"tail_batches": 4,
                                     "retired_leftover_controllers": 1,
                                     "retired_adoptable_leftovers": 1}}}}
                (output / "measurement-read-evidence.json").write_text(
                    json.dumps(evidence))
                return (subprocess.CompletedProcess([], 0, "", ""),
                        Path(run_root) / f"{name}.json", output)

            def killpg(pid, sig):
                events.append(("killpg", pid))
                self.assertEqual(sig, kernel.signal.SIGKILL)
                # The kernel ends the process group; the fake reports its exit.
                coordinator_process["p"].returncode = -9

            args = SimpleNamespace(state=state, binary=Path("/bin/true"))
            with patch.object(deployment, "certificates",
                              lambda path, owners: path.mkdir()), \
                    patch.object(kernel, "campaign_modules",
                                 return_value=(deployment, specs)), \
                    patch.object(kernel, "start_owner", side_effect=owner), \
                    patch.object(kernel, "validate_coordinator", side_effect=admission), \
                    patch.object(kernel, "start_coordinator", side_effect=coordinator), \
                    patch.object(kernel, "run_coordinator", side_effect=reopen), \
                    patch.object(kernel, "kill_owner",
                                 side_effect=lambda process: process.kill()
                                 or process.wait()), \
                    patch.object(kernel, "inventory",
                                 side_effect=[{"controllers": {}}]), \
                    patch.object(kernel, "database_scope",
                                 side_effect=lambda nodes: {
                                     kernel.owner_name(node): "aa" * 16
                                     for node in nodes}), \
                    patch.object(kernel, "layout_teardown_defect", return_value=[]), \
                    patch.object(kernel.os, "killpg", side_effect=killpg):
                exit_code = kernel.restart(args)

            # The action owns no second creation of its proof root: reaching
            # restart_positive would raise FileExistsError on a duplicate.
            self.assertEqual(exit_code, 0)
            self.assertTrue((root / "restart.json").is_file())
            receipt = json.loads((root / "restart.json").read_text())
            self.assertTrue(receipt["complete"])
            self.assertIn(("killpg", 4242), events)
            # The owner starts, the coordinator is killed at its gate, the owner
            # starts again, and the reopen runs once on the same coordinator.
            owners = [event for event in events if event[0] == "owner"]
            self.assertEqual(owners, [("owner", "start"), ("owner", "start")])
            self.assertEqual([event for event in events if event[0] == "coordinator"],
                             [("coordinator", "gate"), ("coordinator", "reopen")])
            self.assertIn(("kill", "owner"), events)
            evidence = receipt["restart"]
            self.assertTrue(evidence["complete"])
            self.assertEqual(evidence["tail_batches"], 4)
            self.assertEqual(evidence["retired_leftover_controllers"], 1)
            self.assertEqual(evidence["retired_adoptable_leftovers"], 1)
            self.assertEqual(evidence["killed_returncode"], -9)
            # The engine admitted the killed spec with the qualification bounds.
            admitted = opened["admitted"]
            storage = admitted["backend"]["options"]["external_remote_config"]["storage"]
            self.assertEqual(storage["fragment_bytes"], 4 * 1024**2)
            self.assertEqual(storage["group_limits"]["max_encoded_bytes"],
                             4 * 1024**2 - 8)
            self.assertEqual(admitted["backend"]["options"]["owners"], 1)
            # The killed run stops at a gate; the reopen is one-owner and ungated.
            killed = json.loads((root / "owner-restart.json").read_text())
            for spec in (killed, opened["spec"]):
                options = spec["backend"]["options"]
                self.assertEqual(options["owners"], 1)
                self.assertEqual(options["external_owner_durability_receipts"],
                                 [str(specs.receipt_path(root, "native", 0))])
            self.assertIn("measurement_gate", killed)
            self.assertNotIn("measurement_gate", opened["spec"])
            self.assertEqual(opened["spec"]["backend"]["options"]["path"],
                             str(root / "stage3" / "db" / "owner-restart" / "coordinator"))
            self.assertTrue(gate_path["path"].is_file())
            self.assertTrue(specs.receipt_path(root, "native", 0).is_file())
            self.assertFalse(specs.receipt_path(root, "native", 1).exists())

    def test_a_gate_timeout_kills_the_started_coordinator(self):
        deployment, specs = kernel.campaign_modules()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "restart"
            events = []
            coordinator_process = {}

            def owner(binary, node, log):
                Path(node["ready"]).parent.mkdir(parents=True, exist_ok=True)
                Path(node["ready"]).write_text("ready\n")
                Path(str(node["ready"]) + ".durability.json").write_text("{}\n")
                return self.Process("owner", 1001, events)

            def coordinator(binary, spec_path, output, stdout_path, stderr_path):
                # The write gate never appears, so the proof times out.
                coordinator_process["p"] = self.Process("coordinator", 4242, events)
                return coordinator_process["p"]

            def admission(binary, spec, run_root, name, timeout=120):
                return subprocess.CompletedProcess([], 0, "", "")

            with patch.object(deployment, "certificates",
                              lambda path, owners: path.mkdir()), \
                    patch.object(kernel, "start_owner", side_effect=owner), \
                    patch.object(kernel, "validate_coordinator", side_effect=admission), \
                    patch.object(kernel, "start_coordinator", side_effect=coordinator), \
                    patch.object(kernel, "LAYOUT_SECONDS", 0):
                with self.assertRaisesRegex(RuntimeError, "write gate expired"):
                    kernel.restart_positive(deployment, specs, Path("/bin/true"), root)
            self.assertIn(("kill", "coordinator"), events)
            self.assertIsNotNone(coordinator_process["p"].returncode)

    def test_an_inadmissible_spec_fails_before_the_coordinator_starts(self):
        deployment, specs = kernel.campaign_modules()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "restart"
            events = []

            def owner(binary, node, log):
                Path(node["ready"]).parent.mkdir(parents=True, exist_ok=True)
                Path(node["ready"]).write_text("ready\n")
                Path(str(node["ready"]) + ".durability.json").write_text("{}\n")
                return self.Process("owner", 1001, events)

            def admission(binary, spec, run_root, name, timeout=120):
                return subprocess.CompletedProcess(
                    [], 1, "", "invalid external remote owner configuration")

            with patch.object(deployment, "certificates",
                              lambda path, owners: path.mkdir()), \
                    patch.object(kernel, "start_owner", side_effect=owner), \
                    patch.object(kernel, "validate_coordinator", side_effect=admission):
                with self.assertRaisesRegex(RuntimeError, "restart spec admission failed"):
                    kernel.restart_positive(deployment, specs, Path("/bin/true"), root)
            self.assertNotIn(("coordinator", "gate"), events)


class CampaignModules(unittest.TestCase):
    def test_the_deployment_and_spec_builders_load(self):
        deployment, specs = kernel.campaign_modules()
        self.assertEqual(deployment.MAX_DATABASES_PER_PARTITION, 8)
        self.assertEqual(specs.OWNERS, kernel.LAYOUT_OWNERS)

    def test_one_spec_names_one_database_and_its_two_receipts(self):
        deployment, specs = kernel.campaign_modules()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(deployment, "certificates",
                              lambda path, owners: path.mkdir()):
                config, _nodes = deployment.prepare(
                    root / "deployment", {name: {} for name in kernel.LAYOUT_NAMESPACES},
                    root / "remote",
                    shape_yamls={name: "key_spaces: []\n" for name in kernel.LAYOUT_NAMESPACES},
                    hosts=["127.0.0.1"] * kernel.LAYOUT_OWNERS,
                    owners=kernel.LAYOUT_OWNERS, port=kernel.LAYOUT_PORT,
                    transport="tcp", coordinator_cache=0, live_transport="tcp")
            name = kernel.LAYOUT_NAMESPACES[0]
            spec = kernel.layout_spec(specs, config["databases"][name], root, 0,
                                      reopen=True)
        options = spec["backend"]["options"]
        self.assertEqual(options["owners"], kernel.LAYOUT_OWNERS)
        self.assertEqual(options["external_remote_config"], config["databases"][name])
        self.assertEqual(options["external_owner_durability_receipts"],
                         [str(specs.receipt_path(root, "native", partition))
                          for partition in range(kernel.LAYOUT_OWNERS)])
        self.assertFalse(spec["phases"]["preload"])
        # The reopen performs live writes, so the restarted owners publish
        # their namespaces again before the port check.
        self.assertEqual(spec["workload"]["mix"]["put"], 100)
        self.assertEqual(spec["phases"]["measurement_operations"], 26)
        # No driver releases a measurement gate in CI; a gated spec would wait.
        self.assertNotIn("measurement_gate", spec)
        self.assertNotIn("measurement_completion_gate", spec)
        self.assertEqual(spec["backend"]["options"]["path"],
                         str(root / "stage3" / "db" / "owner-layout-0" / "coordinator"))


if __name__ == "__main__":
    unittest.main()
