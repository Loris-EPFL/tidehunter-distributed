import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
import campaign
import mainnet_replay_resp as replay
import distributed_mainnet_ci as distributed
import kernel_nvme_tcp_ci as kernel


def write_cleanup_state(state, proof, prewrite=None, before=None):
    """Write the inventory receipt and the proof files that cleanup reads."""
    kernel.dump(state / "before.json", {} if before is None else before)
    for name, value in (("proof", proof), ("proof-prewrite", prewrite)):
        if value is None:
            continue
        (state / name).mkdir()
        kernel.dump(state / name / "proof.json", value)


def read_loss(read_resumed_inode=12, resumes=1):
    """The read phase of one tier's controller-loss evidence."""
    return {
        "kind": "read_controller_loss",
        "role": "read",
        "deleted": {"controller": "nvme3", "sysfs_inode": 11},
        "resumed": {
            "controller_instance": 4,
            "sysfs_inode": read_resumed_inode,
            "read_controller_resumes": resumes,
        },
        "reads": 256,
        "pages": 8,
        "digest": "00",
    }


def write_tier_proofs(output, *, resumed_inode=2, read=None):
    """Write one qualification receipt per tier, as the worker does."""
    if read is None:
        read = read_loss()
    for tier in ("native", "synchronous"):
        native = output / f"kernel-live-test-{tier}" / tier / "native"
        native.mkdir(parents=True)
        kernel.dump(native / "qualification.json", {
            "controller_loss": {
                "deleted": {"controller": "nvme1", "sysfs_inode": 1},
                "resumed": {"controller_instance": 2, "sysfs_inode": resumed_inode},
                "read": read,
            },
            "checks": {"w1_controller_loss": True},
        })


class InjectedSupervisor:
    """A supervisor double: it answers one request per tier."""

    def __init__(self, root, deadline):
        self.records = []

    def start(self):
        pass

    def stop(self):
        pass

    def join(self, timeout=None):
        # Each tier asks for its write controller and its read controller.
        self.records = [{"injected": True, "role": role}
                        for _tier in range(2) for role in ("write", "read")]

    error = None


class MissingSupervisor(InjectedSupervisor):
    """A supervisor double that answered no request."""

    def join(self, timeout=None):
        self.records = []


class ReplayCITests(unittest.TestCase):
    def test_campaign_replay_is_opt_in(self):
        for flag, expected in (([], False), (["--mainnet-replay-resp"], True)):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as temporary:
                argv = ["campaign.py", "--manifest", str(ROOT / "experiments/kv-comparison/ci.json"),
                        "--backends", "valkey_tcp", "--binary", sys.executable,
                        "--output", str(Path(temporary) / "campaign"), *flag]
                with patch.object(sys, "argv", argv), patch.object(campaign, "source_snapshot"), \
                        patch.object(campaign, "run_one") as run:
                    campaign.main()
                self.assertTrue(run.called)
                self.assertEqual(run.call_args.args[0].mainnet_replay_resp, expected)

    def test_replay_reuses_state_and_records_every_reset(self):
        state = {"backend": "valkey_tcp", "backend_options": {"nodes": ["127.0.0.1:6380"]}}
        result = {"verification": {"hash": "same-full-content"}}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(campaign.local, "inspect_owned") as inspect, \
                patch.object(replay.industry_service, "request", return_value="OK") as request, \
                patch.object(replay, "check_nonempty_rejection", return_value={"nonempty_rejected": True}) as freshness, \
                patch.object(replay, "check_result", return_value=result) as checked, \
                patch.object(campaign.subprocess, "run") as child, \
                patch.object(campaign.local, "start") as start, patch.object(campaign.local, "stop") as stop:
            output = Path(temporary) / "replay"
            evidence = campaign.qualify_mainnet_resp(Path("/actual/binary"), state, output)
            configs = [json.loads((output / f"N{n}.json").read_text()) for n in (1, 4)]
            self.assertEqual([c["execution"]["concurrency"] for c in configs], [1, 4])
            self.assertTrue(all(c["workload"]["repetitions"] == 2 for c in configs))
            self.assertEqual(evidence["reset"]["acknowledged_flushes"], ["127.0.0.1:6380"] * 3)
            self.assertEqual(inspect.call_count, 3)
            self.assertEqual(request.call_count, 3)
            freshness.assert_called_once()
            self.assertEqual(checked.call_count, 2)
            self.assertTrue(all(call.kwargs["check"] for call in child.call_args_list))
            self.assertTrue(all(call.kwargs["timeout"] == 75 for call in child.call_args_list))
            start.assert_not_called()
            stop.assert_not_called()

    def test_failed_reset_stops_before_replay(self):
        state = {"backend": "kvrocks", "backend_options": {"nodes": ["127.0.0.1:6380"]}}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(campaign.local, "inspect_owned"), \
                patch.object(replay.industry_service, "request", return_value="ERROR"), \
                patch.object(replay, "check_nonempty_rejection") as freshness:
            with self.assertRaisesRegex(RuntimeError, "reset failed"):
                campaign.qualify_mainnet_resp(Path("/actual/binary"), state, Path(temporary) / "replay")
            freshness.assert_not_called()

    def test_distributed_selection_gates_only_the_leased_replay_module(self):
        for passed in (6, 7):
            with self.subTest(passed=passed), patch.object(distributed, "cargo", return_value=(
                    f"test result: ok. {passed} passed; 0 failed; 0 ignored; 100 filtered out;")) as cargo:
                self.assertEqual(distributed.main(), 0)
                cargo.assert_called_once()
                self.assertEqual(cargo.call_args.args[6:],
                                 ("--lib", "backends::tidehunter::replay", "--", "--nocapture"))

    def test_distributed_selection_fails_closed_on_empty_or_failed_output(self):
        for output in ("", "test result: ok. 0 passed; 0 failed; 0 ignored;",
                       "test result: FAILED. 6 passed; 1 failed; 0 ignored;"):
            with self.subTest(output=output), patch.object(distributed, "cargo", return_value=output):
                with self.assertRaisesRegex(RuntimeError, "nonempty passing suite"):
                    distributed.main()

    def test_kernel_termination_enters_maintained_cleanup(self):
        with patch.object(kernel.signal, "signal") as handler:
            with self.assertRaisesRegex(RuntimeError, "interrupted by signal"):
                kernel.interrupted(15, None)
            self.assertEqual(handler.call_count, 2)

    def test_kernel_live_qualification_gates_wal_replay_and_cleanup(self):
        import argparse
        import local_dry_run_targets
        for complete, clean, restored in ((True, True, True), (False, True, True),
                                          (False, False, True), (True, True, False)):
            with self.subTest(complete=complete, clean=clean, restored=restored), \
                    tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                args = argparse.Namespace(state=state, binary=state / "binary")
                calls = []

                def worker(output, binary, route, seconds, deadline, *,
                           owner_wal_file_bytes, controller_loss):
                    self.assertEqual((binary, route), (args.binary, "tcp"))
                    self.assertEqual(owner_wal_file_bytes, 8 * 1024**2)
                    self.assertTrue(controller_loss)
                    self.assertEqual(output.name, "wal_replay")
                    calls.append(output.name)
                    write_tier_proofs(output)
                    return ({"complete": complete, "fixture_removed": clean}, output / "evidence")

                with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                        patch.object(kernel, "ControllerLossSupervisor", InjectedSupervisor), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(kernel, "inventory", side_effect=[{}, {} if restored else {"leak": True}, {}]):
                    if clean and restored:
                        self.assertEqual(kernel.qualify(args), 0 if complete else 1)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.qualify(args)
                expected = ["wal_replay"]
                self.assertEqual(calls, expected)
                summary = json.loads((state / "single-owner/summary.json").read_text())
                self.assertEqual(list(summary), expected)
                self.assertEqual(summary["wal_replay"]["inventory_restored"], restored)
                self.assertEqual(summary["wal_replay"]["controller_loss"],
                                 {"injections": 4, "tiers": 2})

    def test_a_controller_loss_without_a_proven_resume_gates_the_qualification(self):
        import argparse
        import local_dry_run_targets
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            args = argparse.Namespace(state=state, binary=state / "binary")

            def worker(output, *_args, **_kwargs):
                write_tier_proofs(output, resumed_inode=1)
                return ({"complete": True, "fixture_removed": True}, output / "evidence")

            with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                    patch.object(kernel, "ControllerLossSupervisor", InjectedSupervisor), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(kernel, "inventory", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "controller-loss resume unproven"):
                    kernel.qualify(args)

    def test_an_uninjected_controller_loss_gates_the_qualification(self):
        import argparse
        import local_dry_run_targets
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            args = argparse.Namespace(state=state, binary=state / "binary")

            def worker(output, *_args, **_kwargs):
                write_tier_proofs(output)
                return ({"complete": True, "fixture_removed": True}, output / "evidence")

            with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                    patch.object(kernel, "ControllerLossSupervisor", MissingSupervisor), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(kernel, "inventory", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "injection was not confirmed"):
                    kernel.qualify(args)

    def test_controller_loss_injection_deletes_only_the_requested_controller(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sysfs = root / "sys-class-nvme"
            controller = sysfs / "nvme7"
            controller.mkdir(parents=True)
            (controller / "state").write_text("live\n")
            live_inode = controller.stat().st_ino
            kept = sysfs / "nvme8"
            kept.mkdir()
            (kept / "state").write_text("live\n")
            namespace = root / "dev" / "nvme7n1"
            namespace.parent.mkdir()
            namespace.write_text("")
            fd = root / "proc" / "4242" / "fd"
            fd.mkdir(parents=True)
            (fd / "3").symlink_to(namespace)

            def request(name, *, inode=live_inode, pid=4242, role="write"):
                path = root / name
                path.write_text(json.dumps({
                    "kind": "w1_controller_loss_request", "pid": pid, "role": role,
                    "target": {"controller": "nvme7", "sysfs_inode": inode,
                               "nqn": "nqn.2026-01.test:owner",
                               "namespace_path": str(namespace), "state": "live"}}))
                return path

            deleted = []
            def delete(path):
                deleted.append(path.name)
                shutil.rmtree(path)
            roots = dict(controller_root=sysfs, proc_root=root / "proc", delete=delete)
            # A stale inode and a process without the namespace are refused
            # before any delete is issued.
            for name, values, message in (
                    ("stale.json", dict(inode=live_inode + 1), "inode differs"),
                    ("no-descriptor.json", dict(pid=4243), "no descriptor"),
                    ("bad-role.json", dict(role="sideways"), "unexpected controller-loss role")):
                with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, message):
                    kernel.inject_controller_loss(request(name, **values), **roots)
            self.assertEqual(deleted, [])
            record = kernel.inject_controller_loss(request("ready.json"), **roots)
            self.assertEqual(deleted, ["nvme7"])
            self.assertTrue(record["injected"])
            self.assertEqual(record["coordinator_descriptors"], ["3"])
            self.assertFalse(controller.exists(), "the requested controller is deleted")
            self.assertTrue(kept.is_dir(), "an unrelated controller is never touched")
            evidence = json.loads((root / "controller-loss-injection.json").read_text())
            self.assertEqual(evidence, record)
            self.assertEqual(record["role"], "write")
            # The read route asks for its own controller, and its evidence never
            # overwrites the write route's.
            controller.mkdir()
            (controller / "state").write_text("live\n")
            live_inode = controller.stat().st_ino
            read_request = request("read-controller-loss-ready.json",
                                   inode=live_inode, role="read")
            read_record = kernel.inject_controller_loss(read_request, **roots)
            self.assertTrue(read_record["injected"])
            self.assertEqual(read_record["role"], "read")
            self.assertEqual(
                json.loads((root / "controller-loss-injection.json").read_text()), record)
            self.assertEqual(
                json.loads((root / "read-controller-loss-injection.json").read_text()),
                read_record)


    def test_prewrite_pass_gates_the_run(self):
        import argparse
        import local_multiowner_check
        import local_dry_run_targets
        original_root = local_dry_run_targets.ROOT
        for clean in (True, False):
            with self.subTest(clean=clean), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                binary = state / "binary"
                binary.touch()
                args = argparse.Namespace(state=state, binary=binary)
                calls = []
                def proof():
                    self.assertEqual(local_dry_run_targets.ROOT, [])
                    prewrite = "--wal-prewrite" in sys.argv
                    # Both passes state their choice; neither relies on the default.
                    self.assertNotEqual(prewrite, "--no-wal-prewrite" in sys.argv)
                    calls.append(prewrite)
                    output = Path(sys.argv[sys.argv.index("--output") + 1])
                    output.mkdir()
                    kernel.dump(output / "proof.json", {
                        "complete": True,
                        "cleanup_complete": clean if prewrite else True,
                        "fixture_removed": clean if prewrite else True,
                        "errors": [],
                    })
                    return 1 if prewrite else 0
                with patch.object(kernel, "inventory", return_value={}), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(sys, "argv", ["test"]), \
                        patch.object(local_multiowner_check, "main", side_effect=proof):
                    self.assertEqual(kernel.run(args), 1)
                    self.assertIs(local_dry_run_targets.ROOT, original_root)
                    self.assertEqual(calls, [False, True])
                    prewrite = json.loads((state / "prewrite-result.json").read_text())
                    self.assertTrue(prewrite["attempted"])
                    self.assertFalse(prewrite["passed"])
                    self.assertFalse(json.loads((state / "result.json").read_text())["passed"])
                    if clean:
                        self.assertEqual(kernel.cleanup(args), 0)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.cleanup(args)
                        self.assertFalse(json.loads((state / "cleanup.json").read_text())["complete"])

    def test_both_passes_gate_the_run(self):
        import argparse
        import local_multiowner_check
        import local_dry_run_targets
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                self.assertEqual(local_dry_run_targets.ROOT, [])
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 0)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertTrue(json.loads((state / "result.json").read_text())["passed"])

    def test_changed_final_inventory_gates_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", side_effect=[{}, {}, {"leak": True}]), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertEqual(prewrite["defect"],
                                 "last pass left the host inventory changed")
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_prewrite_proof_fixture_removal_gates_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                prewrite = "--wal-prewrite" in sys.argv
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": not prewrite, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertEqual(prewrite["defect"],
                                 "proof-prewrite proof fixture_removed is not true")
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_missing_prewrite_proof_gates_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                if "--wal-prewrite" not in sys.argv:
                    output = Path(sys.argv[sys.argv.index("--output") + 1])
                    output.mkdir()
                    kernel.dump(output / "proof.json", {
                        "complete": True, "cleanup_complete": True,
                        "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertIn("proof-prewrite proof missing", prewrite["defect"])
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_proof_errors_gate_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": ["kernel log interval incomplete"]})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_first_pass_failure_gates_even_when_prewrite_passes(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                prewrite = "--wal-prewrite" in sys.argv
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0 if prewrite else 1
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                self.assertTrue(json.loads((state / "prewrite-result.json").read_text())["passed"])
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_prewrite_pass_does_not_start_after_incomplete_cleanup(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            def proof():
                output = state / "proof"
                output.mkdir()
                kernel.dump(output / "proof.json", {"cleanup_complete": False})
                return 1
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof) as run:
                self.assertEqual(kernel.run(argparse.Namespace(state=state, binary=binary)), 1)
                run.assert_called_once()
                self.assertFalse(json.loads((state / "prewrite-result.json").read_text())["attempted"])


    def test_cleanup_accepts_an_incomplete_proof_with_a_clean_teardown(self):
        import argparse
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            write_cleanup_state(state, {"complete": False, "cleanup_complete": True,
                                        "fixture_removed": True,
                                        "errors": ["admission benchmark failed"]})
            with patch.object(kernel, "inventory", return_value={}):
                self.assertEqual(kernel.cleanup(argparse.Namespace(state=state)), 0)
            cleanup = json.loads((state / "cleanup.json").read_text())
            self.assertEqual(cleanup, {"complete": True, "defects": [],
                                       "inventory_restored": True})

    def test_cleanup_rejects_teardown_defects_only(self):
        import argparse
        for proof, defect in (
                ({"complete": True, "cleanup_complete": False, "fixture_removed": True,
                  "errors": []}, "proof proof cleanup_complete is not true"),
                ({"complete": False, "cleanup_complete": True, "fixture_removed": False,
                  "errors": ["admission benchmark failed"]},
                 "proof proof fixture_removed is not true")):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                write_cleanup_state(state, proof)
                with patch.object(kernel, "inventory", return_value={}):
                    with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                        kernel.cleanup(argparse.Namespace(state=state))
                cleanup = json.loads((state / "cleanup.json").read_text())
                self.assertFalse(cleanup["complete"])
                self.assertEqual(cleanup["defects"], [defect])

    def test_cleanup_rejects_a_missing_proof(self):
        import argparse
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            write_cleanup_state(state, None)
            with patch.object(kernel, "inventory", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                    kernel.cleanup(argparse.Namespace(state=state))
            cleanup = json.loads((state / "cleanup.json").read_text())
            self.assertFalse(cleanup["complete"])
            self.assertTrue(cleanup["defects"][0].startswith("proof proof missing"))

    def test_cleanup_uses_teardown_evidence_for_the_prewrite_pass(self):
        import argparse
        clean = {"complete": True, "cleanup_complete": True,
                 "fixture_removed": True, "errors": []}
        incomplete = {"complete": False, "cleanup_complete": True,
                      "fixture_removed": True, "errors": ["admission benchmark failed"]}
        for prewrite, complete in ((incomplete, True), (dict(clean, fixture_removed=False), False)):
            with self.subTest(complete=complete), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                write_cleanup_state(state, clean, prewrite)
                with patch.object(kernel, "inventory", return_value={}):
                    if complete:
                        self.assertEqual(kernel.cleanup(argparse.Namespace(state=state)), 0)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.cleanup(argparse.Namespace(state=state))
                recorded = json.loads((state / "prewrite-cleanup.json").read_text())
                self.assertEqual(recorded, {"complete": complete})

    def test_run_still_rejects_an_incomplete_proof_with_a_clean_teardown(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            def proof():
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": False, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(argparse.Namespace(state=state, binary=binary)), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertFalse(prewrite["attempted"])
                self.assertEqual(prewrite["defect"], "proof proof complete is not true")
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])


    def test_a_read_controller_loss_without_a_proven_resume_gates_the_qualification(self):
        import argparse
        import local_dry_run_targets
        for read in (read_loss(read_resumed_inode=11), read_loss(resumes=0),
                     {"kind": "read_controller_loss", "role": "read"}):
            with self.subTest(read=read), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                args = argparse.Namespace(state=state, binary=state / "binary")

                def worker(output, *_args, **_kwargs):
                    write_tier_proofs(output, read=read)
                    return ({"complete": True, "fixture_removed": True}, output / "evidence")

                with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                        patch.object(kernel, "ControllerLossSupervisor", InjectedSupervisor), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(kernel, "inventory", return_value={}):
                    with self.assertRaisesRegex(RuntimeError,
                                                "read controller-loss resume unproven"):
                        kernel.qualify(args)

    def test_a_controller_loss_must_answer_both_roles(self):
        import argparse
        import local_dry_run_targets
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            args = argparse.Namespace(state=state, binary=state / "binary")

            def worker(output, *_args, **_kwargs):
                write_tier_proofs(output)
                return ({"complete": True, "fixture_removed": True}, output / "evidence")

            class WriteOnlySupervisor(InjectedSupervisor):
                def join(self, timeout=None):
                    self.records = [{"injected": True, "role": "write"} for _ in range(4)]

            with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                    patch.object(kernel, "ControllerLossSupervisor", WriteOnlySupervisor), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(kernel, "inventory", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "both the write and read routes"):
                    kernel.qualify(args)

    def test_a_poisoned_authority_verdict_gates_the_qualification(self):
        import argparse
        import local_dry_run_targets
        for poisoned in ("read authority is poisoned", "write core requires recovery"):
            with self.subTest(poisoned=poisoned), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                args = argparse.Namespace(state=state, binary=state / "binary")

                def worker(output, *_args, **_kwargs):
                    write_tier_proofs(output)
                    (output / "kernel-live-test-native" / "progress.json").write_text(
                        json.dumps({"detail": poisoned}))
                    return ({"complete": True, "fixture_removed": True}, output / "evidence")

                with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                        patch.object(kernel, "ControllerLossSupervisor", InjectedSupervisor), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(kernel, "inventory", return_value={}):
                    with self.assertRaisesRegex(RuntimeError, "recorded in"):
                        kernel.qualify(args)

    def test_controller_loss_supervisor_answers_write_and_read_requests(self):
        import time
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "tier" / "native").mkdir(parents=True)
            write = root / "tier" / "native" / "controller-loss-ready.json"
            read = root / "tier" / "native" / "read-controller-loss-ready.json"
            write.write_text(json.dumps({"role": "write"}))
            read.write_text(json.dumps({"role": "read"}))
            seen = []

            def fake_inject(path):
                role = json.loads(Path(path).read_text())["role"]
                seen.append(role)
                return {"injected": True, "role": role}

            supervisor = kernel.ControllerLossSupervisor(root, time.monotonic() + 5, poll=0.01)
            with patch.object(kernel, "inject_controller_loss", side_effect=fake_inject):
                supervisor.start()
                try:
                    deadline = time.monotonic() + 5
                    while len(seen) < 2 and time.monotonic() < deadline:
                        time.sleep(0.01)
                finally:
                    supervisor.stop()
                    supervisor.join(timeout=5)
            self.assertIsNone(supervisor.error)
            self.assertEqual(seen, ["write", "read"])
            self.assertEqual([record["role"] for record in supervisor.records],
                             ["write", "read"])

class NvmetTraceTests(unittest.TestCase):
    def test_counts_double_completion_and_repeated_enqueue(self):
        trace = "\n".join([
            "  nvmet_req_init: ctrl_id=1 qid=1 cid=16 disk=loop0 status=0",
            "  nvmet_tcp_qr: (nvmet_tcp_queue_response+0x0/0x10) req=0xffff888",
            "  nvmet_tcp_qr: (nvmet_tcp_queue_response+0x0/0x10) req=0xffff888",
            "  nvmet_req_complete: ctrl_id=1 qid=1 cid=16 status=0",
            "  nvmet_req_complete: ctrl_id=1 qid=1 cid=16 status=0",
            "  nvmet_req_complete: ctrl_id=2 qid=1 cid=16 status=0",
        ])
        completes, enqueues = kernel.NvmetTrace()._counts(trace)
        self.assertEqual(completes[("1", "1", "16")], 2,
                         "a double completion of one command is counted twice")
        self.assertEqual(completes[("2", "1", "16")], 1)
        self.assertEqual(enqueues["0xffff888"], 2,
                         "two queue-response enqueues of one request are counted")

    def test_capture_failure_writes_counts_and_duplicates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "tracefs"
            root.mkdir()
            (root / "trace").write_text(
                "nvmet_req_complete: ctrl_id=1 qid=1 cid=16 status=0\n"
                "nvmet_req_complete: ctrl_id=1 qid=1 cid=16 status=0\n")
            trace = kernel.NvmetTrace()
            trace.root = root
            trace.started = 0.0
            trace.capture_failure(Path(temporary) / "out")
            self.assertEqual(
                json.loads((Path(temporary) / "out" / "complete-counts.json").read_text()),
                {"1|1|16": 2})
            self.assertEqual(
                json.loads((Path(temporary) / "out" / "duplicates.json").read_text()),
                {"1|1|16": 2})
            self.assertTrue((Path(temporary) / "out" / "trace.txt").is_file())


if __name__ == "__main__":
    unittest.main()
