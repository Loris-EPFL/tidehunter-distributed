"""The R1 test producer's JSONL envelope and live retirement sequence."""

from copy import deepcopy
from hashlib import sha256
import json
import unittest

from q1_r1_events import collect_r1_events
from test_q1_producers import temporary_directory


class R1Events(unittest.TestCase):
    def setUp(self):
        self.root = temporary_directory(self)
        self.scope = {"run_id": "q1-run", "case_id": "capacity_r3", "tier": "native", "policy": "dead_only",
                      "retention_sha256": "1" * 64, "database_id": "2" * 32}
        self.streams = {}
        self.paths = {}
        self.events = {}
        for i, role in enumerate(("coordinator", "owner_O0", "owner_O1"), 1):
            process = {"producer_role": role, "incarnation": f"{i:032x}", "boot_id": f"boot-{i}",
                       "process_id": 50 + i, "process_start_ticks": 200 + i}
            name = f"capacity/r1-events/{role}/{process['incarnation']}.jsonl"
            self.streams[name] = process
            self.paths[role] = name
            self.events[role] = []
        self.build_chain(0)
        self.build_chain(1)

    def append(self, role, kind, file, payload, *, after=(), attempt=None):
        seq = len(self.events[role])
        process = self.streams[self.paths[role]]
        self.events[role].append({
            "seq": seq, "monotonic_ns": (10000 if role == "coordinator" else 1) + seq, "kind": kind,
            "after": [{"role": role, "incarnation": process["incarnation"], "seq": index} for index in after],
            **dict(zip(("partition", "fencing_generation", "file_start", "file_end", "device", "inode"), file)),
            "attempt_id": attempt, "payload": payload,
        })
        return seq

    def build_chain(self, partition, *, revoke=True):
        file = (partition, 2, 0, 4096, 91 + partition, 104 + partition)
        group = (*file[:2], None, None, None, None)
        role = f"owner_O{partition}"
        roll = self.append(role, "file_rolled", (*file[:4], None, None),
                           {"to_file_start": 4096, "to_device": file[4], "to_inode": file[5] + 1})
        seal = self.append(role, "seal_durable", file, {"root_generation": 4, "last_batch_sequence": 6}, after=[roll])
        before_root = seal
        if revoke:
            drain = self.append(role, "target_io_drained", file,
                                {"record_pins": 0, "export_pins": 0, "drain": "namespace_enable_0"}, after=[seal])
            before_root = self.append(role, "target_removed", file, {"tombstone_durable": True}, after=[drain])
        retired = self.append(role, "retirement_root_durable", file, {"seal_generation": 4, "root_generation": 5},
                              after=[before_root])
        unlink = self.append(role, "wal_unlinked", file, {"seal_generation": 4, "phase": "retirement"}, after=[retired])
        synced = self.append(role, "wal_directory_synced", group, {"files": [0], "phase": "retirement"}, after=[unlink])
        tombstone = self.append(role, "target_tombstone_synced", file,
                                {"tombstone_removed": revoke, "stream": None}, after=[synced])
        self.append(role, "cleanup_root_durable", file, {"root_generation": 6, "stream": None}, after=[tombstone])
        claim = self.append("coordinator", "victim_claimed", file,
                            {"seal_generation": 4, "checkpoint_generation": 4, "metadata_digest": "a" * 64,
                             "replay_from": 8192, "durable_end": 8192, "callback_from": 8192, "covered_next": 12,
                             "persisted_cut_next": 11, "install_seq": 8}, attempt=partition)
        cq = self.append("coordinator", "initiator_cq_drained", file, {"basis": "no_reference_index_pins_to_terminal_cqe"},
                         after=[claim], attempt=partition)
        self.append("coordinator", "retirement_reply", group, {"outcome": "completed", "files": [0]},
                    after=[cq], attempt=partition)

    def export(self, *, incomplete=None, lost=None):
        for name, process in self.streams.items():
            role = process["producer_role"]
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            records = self.events[role]
            lines = [{"schema_version": 1, **self.scope, **process, "mono_clock": "monotonic_ns"}, *records]
            if role != incomplete:
                lines.append({"terminal": {"complete": True, "events": len(records), "lost": int(role == lost)}})
            path.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in lines))

    def collect(self):
        return collect_r1_events(self.root, self.scope, self.streams, max_events=20, max_bytes=30_000, max_line=2000)

    def test_joins_exact_files_and_builds_archive_manifest_without_cross_clock_order(self):
        self.export()
        manifest, joined = self.collect()
        self.assertEqual(joined, {((i, 2, 0, 4096, 91 + i, 104 + i), 4) for i in (0, 1)})
        for descriptor in manifest["streams"]:
            data = (self.root / descriptor["path"]).read_bytes()
            self.assertEqual(descriptor["bytes"], len(data))
            self.assertEqual(descriptor["sha256"], sha256(data).hexdigest())
            self.assertTrue(descriptor["complete"])
            self.assertEqual(descriptor["lost"], 0)

    def test_lost_or_incomplete_incarnation_cannot_qualify(self):
        self.export(lost="owner_O0")
        with self.assertRaisesRegex(ValueError, "lossless"):
            self.collect()
        self.export(incomplete="coordinator")
        with self.assertRaisesRegex(ValueError, "truncated"):
            self.collect()

    def test_busy_or_different_owner_inode_cannot_complete_retirement(self):
        reply = self.events["coordinator"][2]
        reply["payload"]["outcome"] = "busy"
        self.export()
        with self.assertRaisesRegex(ValueError, "matching coordinator proof"):
            self.collect()
        reply["payload"]["outcome"] = "completed"
        for record in self.events["owner_O0"]:
            if record["inode"] is not None:
                record["inode"] += 10
        self.export()
        with self.assertRaisesRegex(ValueError, "matching coordinator proof"):
            self.collect()

    def test_cross_incarnation_edges_and_missing_kernel_drain_fail(self):
        events = self.events["owner_O0"]
        original = deepcopy(events[2])
        events[2]["after"][0]["incarnation"] = "6" * 32
        self.export()
        with self.assertRaisesRegex(ValueError, "same stream"):
            self.collect()
        events[2] = original
        events[2]["payload"]["export_pins"] = 1
        self.export()
        with self.assertRaisesRegex(ValueError, "drain evidence missing"):
            self.collect()

    def test_unbound_victim_needs_no_revoke_event(self):
        self.events["coordinator"].clear()
        self.events["owner_O0"].clear()
        self.events["owner_O1"].clear()
        self.build_chain(0, revoke=False)
        self.build_chain(1)
        self.export()
        self.assertEqual(len(self.collect()[1]), 2)
        self.events["owner_O0"][-2]["payload"]["tombstone_removed"] = 0
        self.export()
        with self.assertRaisesRegex(ValueError, "invalid R1 target tombstone"):
            self.collect()

    def test_no_gc_refuses_even_early_durable_retirement(self):
        self.scope["policy"] = "no_gc"
        del self.events["coordinator"][:]
        for role in ("owner_O0", "owner_O1"):
            del self.events[role][4:]
        self.export()
        self.assertEqual(self.collect()[1], set())
        self.append("owner_O0", "retirement_root_durable", (0, 2, 0, 4096, 91, 104),
                    {"seal_generation": 4, "root_generation": 5}, after=[3])
        self.export()
        with self.assertRaisesRegex(ValueError, "no-GC control retired files"):
            self.collect()

    def test_r1_streams_cannot_qualify_r2_evacuation(self):
        self.scope["policy"] = "evacuation"
        self.export()
        with self.assertRaisesRegex(ValueError, "R2 evacuation"):
            self.collect()
