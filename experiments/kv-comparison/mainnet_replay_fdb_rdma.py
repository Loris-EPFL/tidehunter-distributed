#!/usr/bin/env python3
"""Cached-server FDB chunked and paired-libvalkey TCP replay qualification.

Real RDMA needs an admitted device and server route; paired TCP is not RDMA evidence.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import industry_service
import local
import mainnet_replay_resp as resp

CASES = (("foundationdb", "qualification"), ("valkey_tcp", "qualification"),
         ("valkey_tcp", "distributed"))
TABLE_ROWS = [2, 2, 2, 2, 90, 90, 56, 90, 90, 90, 2, 1, 0, 1306, 90, 66, 3]


def check_result(output, backend):
    summary = json.loads((output / "summary.json").read_text())
    phases = summary["phase_counts"]
    profile = phases["profile"]
    verification = profile["final_verification"]
    if (summary["status"] != "completed" or phases["counts"] != resp.EXPECTED
            or phases["unknown_outcomes"] or phases["unresolved_workers"]
            or not profile["functional_qualification"]
            or not verification["full_values"]
            or not verification["fresh_empty_database_admitted"]
            or not verification["empty_marker_table_verified"]
            or verification["table_rows"] != TABLE_ROWS
            or verification["verified_rows"] != 1982):
        raise RuntimeError("replay counts or full final-row verification failed")
    extra = 4 if backend == "foundationdb" else 0
    if verification["enumerated_physical_rows"] != 1982 + extra:
        raise RuntimeError("unexpected final physical row population")
    before, after = (profile["physical"][name] for name in
                     ("before_measurement", "after_measurement"))
    if backend == "foundationdb":
        expected = {"physical_sets": 1418, "chunk_count": 4, "metadata_bytes": 26,
                    "physical_clear_operations": 2828, "transactions_acknowledged": 18}
        if profile["replay_contract"]["value_layout"] != "fdb_chunked_values_v1":
            raise RuntimeError("FDB chunked layout label missing")
    else:
        expected = {"sets_submitted": 1414, "sets_acknowledged": 1414}
        if summary["backend"]["topology"]["replay_transport"] != "libvalkey TCP":
            raise RuntimeError("paired client did not execute libvalkey TCP")
    for key, value in expected.items():
        if after[key] - before[key] != value:
            raise RuntimeError(f"physical counter mismatch: {key}")
    with (output / "samples.csv").open(newline="") as file:
        samples = list(csv.DictReader(file))
    if (len(samples) != 1032 or any(row["outcome"] != "success" for row in samples)
            or sum(row["operation"] == "get" for row in samples) != 1014
            or sum(row["operation"] == "commit" for row in samples) != 18):
        raise RuntimeError("raw sample population mismatch")
    return {"counts": phases["counts"], "samples": len(samples),
            "verification": verification, "physical": profile["physical"],
            "replay_contract": profile["replay_contract"],
            "measurement_seconds": profile["measurement_elapsed_seconds"]}


def options(state):
    if state["backend"] == "foundationdb":
        return {"cluster_file": str(Path(state["directory"]) / "fdb.cluster"),
                "required_replication_factor": 1, "transaction_timeout_ms": 5000}
    return dict(state["backend_options"], client="libvalkey")


def reset(state):
    local.inspect_owned(state)
    if state["backend"] == "foundationdb":
        local.execute(state, "fdb0", "/usr/bin/fdbcli", "-C", "/config/fdb.cluster",
                      "--timeout", "10", "--exec", 'writemode on; clearrange "" "\\xff"')
    else:
        for endpoint in state["backend_options"]["nodes"]:
            if industry_service.request(endpoint, "FLUSHALL") != "OK":
                raise RuntimeError("owned replay database reset failed")


def hardware_probe(argv):
    try:
        return {"output": local.command(argv, check=False), "available": True}
    except (OSError, subprocess.SubprocessError) as error:
        return {"available": False, "reason": str(error)}


def qualify(binary, output, test_binary=None, cases=CASES):
    output.mkdir(parents=True, exist_ok=False)
    local.dump(output / "host.json", {"cpu_count": os.cpu_count(),
               "meminfo": Path("/proc/meminfo").read_text(),
               "disk": local.command(["df", "-B1", str(output)]),
               "rdma_link": hardware_probe(["rdma", "link"]),
               "ibv_devices": hardware_probe(["ibv_devices"]),
               "infiniband_devices": sorted(p.name for p in Path("/sys/class/infiniband").glob("*")),
               "rdma_rxe_loaded": Path("/sys/module/rdma_rxe").exists(),
               "rdma_execution": "not_run; no approved device/server route selected",
               "physical_hosts": 1, "independent_disks": False,
               "functional_qualification_only": True})
    results = {}
    for backend, layout in cases:
        name = f"{backend}-{layout}"
        directory = output / name
        directory.mkdir()
        state_path = directory / "service"
        state = None
        started = time.monotonic()
        try:
            local.command(["docker", "image", "inspect", local.IMAGES[backend]])
            local.start(SimpleNamespace(backend=backend, layout=layout, state=state_path,
                                       timeout=180, ack_contract="synced", cached_images_only=True))
            state = local.load(state_path)
            reset(state)
            config = resp.configuration(backend, options(state), 1)
            if backend != "foundationdb":
                freshness = resp.check_nonempty_rejection(binary, directory, config)
            else:
                if test_binary is None:
                    raise RuntimeError("FDB requires --test-binary for real overwrite/strict-limit regressions")
                environment = dict(os.environ, KV_REPLAY_FDB_CLUSTER_FILE=options(state)["cluster_file"])
                with (directory / "overwrite-strict-limit.log").open("w") as log:
                    subprocess.run([str(test_binary), "backends::foundationdb::replay::tests::owned_server_overwrite_and_strict_rejection",
                                    "--exact", "--ignored", "--nocapture"], env=environment,
                                   stdout=log, stderr=subprocess.STDOUT, timeout=60, check=True)
                # Seed a real physical row. Admission must reject it without clearing it.
                local.execute(state, "fdb0", "/usr/bin/fdbcli", "-C", "/config/fdb.cluster", "--timeout", "10",
                              "--exec", 'writemode on; set sentinel existing')
                path = directory / "nonempty.json"
                local.dump(path, config)
                with (directory / "nonempty.log").open("w") as log:
                    rejected = subprocess.run([str(binary), "--config", str(path), "--output", str(directory / "nonempty")],
                                              stdout=log, stderr=subprocess.STDOUT, timeout=30)
                if (rejected.returncode == 0 or "fresh empty owned database" not in
                        (directory / "nonempty.log").read_text()):
                    raise RuntimeError("FDB admitted an existing database")
                existing = local.execute(state, "fdb0", "/usr/bin/fdbcli", "-C", "/config/fdb.cluster", "--timeout", "10",
                                         "--exec", 'get sentinel')
                if "existing" not in existing:
                    raise RuntimeError("FDB freshness rejection changed existing row")
                freshness = {"rejected_existing_row": True, "strict_limit_and_overwrite_test": "passed"}
            evidence = []
            for concurrency in (1, 4):
                reset(state)
                config = resp.configuration(backend, options(state), concurrency)
                path = directory / f"N{concurrency}.json"
                local.dump(path, config)
                run = directory / f"N{concurrency}"
                with (directory / f"N{concurrency}.log").open("w") as log:
                    subprocess.run([str(binary), "--config", str(path), "--output", str(run)],
                                   stdout=log, stderr=subprocess.STDOUT, timeout=75, check=True)
                evidence.append(check_result(run, backend))
            if evidence[0]["verification"] != evidence[1]["verification"]:
                raise RuntimeError("N=1 and N=4 final contents differ")
            results[name] = {"status": "completed", "runs": evidence, "freshness_regression": freshness,
                             "lifecycle_and_replay_seconds": time.monotonic() - started}
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as error:
            results[name] = {"status": "failed", "reason": str(error)}
        finally:
            if state is None and (state_path / "owner.json").exists():
                state = local.load(state_path)
            if state is not None:
                try:
                    local.export(state, directory / "service-evidence")
                finally:
                    local.stop(state)
            local.dump(output / "results.json", results)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--test-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = qualify(args.binary.resolve(strict=True), args.output.resolve(),
                      args.test_binary.resolve(strict=True))
    return 0 if all(case["status"] == "completed" for case in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
