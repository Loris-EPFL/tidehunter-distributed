#!/usr/bin/env python3
"""Sequential, cached-image-only two-repetition RESP mainnet qualification.

Local containers share one host/SSD; these runs are functional evidence, not
HA, crash-durability or sustained-performance qualification. No cloud actions.
"""
import argparse
import binascii
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import industry_service
import local

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "experiments/sui-mainnet/fixture/manifest.json"
MANIFEST_SHA = "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa"
CASES = (("valkey_tcp", "qualification"), ("valkey_tcp", "distributed"),
         ("garnet", "qualification"), ("kvrocks", "qualification"))
EXPECTED = {"store_gets": 1014, "overlay_demands": 232, "put_members": 1414,
            "write_calls": 18, "transactions": 90, "checkpoints": 2}


def configuration(backend, options, concurrency):
    if hashlib.sha256(MANIFEST.read_bytes()).hexdigest() != MANIFEST_SHA:
        raise RuntimeError("pinned replay manifest hash mismatch")
    return {"schema_version": 1, "case_id": f"mainnet-{backend}-N{concurrency}",
            "backend": {"kind": backend, "options": options},
            "workload": {"kind": "sui_mainnet_replay", "manifest_path": str(MANIFEST),
                         "manifest_sha256": MANIFEST_SHA, "repetitions": 2,
                         "minimum_measurement_seconds": 0},
            "execution": {"mode": "closed_loop", "concurrency": concurrency,
                          "client_shards": 1, "queue_capacity": 8,
                          "request_timeout_ms": 10000},
            "limits": {"max_keys": 10000, "max_bytes": 64 * 1024**2,
                       "max_operations": 10000, "deadline_seconds": 60,
                       "shutdown_grace_seconds": 5}}


def check_result(output):
    summary = json.loads((output / "summary.json").read_text())
    phases = summary["phase_counts"]
    if summary["status"] != "completed" or phases["counts"] != EXPECTED:
        raise RuntimeError(f"replay counts/status mismatch: {summary}")
    if phases["unknown_outcomes"] != 0 or phases["unresolved_workers"] != 0:
        raise RuntimeError("replay has an unresolved/unknown outcome")
    profile = phases["profile"]
    verification = profile["final_verification"]
    if (not profile["functional_qualification"] or not verification["full_values"]
            or not verification["empty_marker_table_verified"]
            or not verification["fresh_empty_database_admitted"]
            or verification["table_rows"][12] != 0
            or verification["enumerated_physical_rows"] != verification["verified_rows"]):
        raise RuntimeError("replay did not verify the complete table-aware final population")
    with (output / "samples.csv").open(newline="") as file:
        samples = list(csv.DictReader(file))
    if (len(samples) != 1032 or any(row["outcome"] != "success" for row in samples)
            or sum(row["operation"] == "get" for row in samples) != 1014
            or sum(row["operation"] == "commit" for row in samples) != 18):
        raise RuntimeError("raw replay sample population mismatch")
    return {"counts": phases["counts"], "samples": len(samples),
            "verification": verification, "replay_contract": profile["replay_contract"],
            "physical": profile["physical"], "placement": phases["resources"]["prepared_placement"],
            "measurement_seconds": profile["measurement_elapsed_seconds"]}


def freshness_probe_key(options):
    """Choose one sentinel on the first admitted primary, without redirect retries."""
    if not options["cluster"]:
        return "{}mainnet-freshness-probe"
    view = industry_service.request(options["nodes"][0], "CLUSTER", "NODES")
    columns = next(line.split() for line in view.splitlines()
                   if "myself" in line.split()[2].split(","))
    ranges = []
    for token in columns[8:]:
        start, _, end = token.partition("-")
        ranges.append((int(start), int(end or start)))
    for index in range(65536):
        key = f"{{}}mainnet-freshness-probe-{index}"
        slot = binascii.crc_hqx(key.encode(), 0) % 16384
        if any(start <= slot <= end for start, end in ranges):
            return key
    raise RuntimeError("no bounded freshness sentinel maps to the first primary")


def check_nonempty_rejection(binary, directory, config):
    options = config["backend"]["options"]
    endpoint = options["nodes"][0]
    key = freshness_probe_key(options)
    if industry_service.request(endpoint, "SET", key, "existing-row") != "OK":
        raise RuntimeError("freshness sentinel SET failed")
    path = directory / "nonempty.json"
    local.dump(path, config)
    run = directory / "nonempty"
    result = subprocess.run([str(binary), "--config", str(path), "--output", str(run)],
                            capture_output=True, text=True, timeout=75, check=False)
    (directory / "nonempty.log").write_text(result.stdout + result.stderr)
    if result.returncode == 0 or "requires fresh empty owned databases" not in result.stderr:
        raise RuntimeError("nonempty server was not rejected by replay admission")
    if industry_service.request(endpoint, "GET", key) != "existing-row":
        raise RuntimeError("rejected replay changed the existing sentinel")
    if industry_service.request(endpoint, "DEL", key) != 1:
        raise RuntimeError("freshness sentinel cleanup failed")
    return {"nonempty_rejected": True, "existing_row_unchanged": True,
            "exit_code": result.returncode, "key": key, "endpoint": endpoint}


def qualify(binary, output, cases=CASES):
    output.mkdir(parents=True, exist_ok=False)
    local.dump(output / "host.json", {"cpu_count": os.cpu_count(),
               "meminfo": Path("/proc/meminfo").read_text(),
               "disk": local.command(["df", "-B1", str(output)]),
               "docker_images": local.command(["docker", "images", "--digests", "--no-trunc"]),
               "physical_hosts": 1, "independent_disks": False,
               "functional_qualification_only": True})
    results = {}
    for backend, layout in cases:
        name = f"{backend}-{layout}"
        directory = output / name
        directory.mkdir()
        # Inspect the exact pinned image before lifecycle startup. Never pull.
        try:
            local.command(["docker", "image", "inspect", local.IMAGES[backend]])
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as error:
            results[name] = {"status": "not_run", "reason": f"cached pinned image missing: {error}"}
            local.dump(output / "results.json", results)
            continue
        state_path = directory / "service"
        state = None
        started = time.monotonic()
        try:
            local.start(SimpleNamespace(backend=backend, layout=layout, state=state_path,
                                       timeout=180, ack_contract="synced", cached_images_only=True))
            state = local.load(state_path)
            evidence = []
            local.inspect_owned(state)
            for endpoint in state["backend_options"]["nodes"]:
                if industry_service.request(endpoint, "FLUSHALL") != "OK":
                    raise RuntimeError("owned replay database reset failed")
            freshness = check_nonempty_rejection(
                binary, directory, configuration(backend, state["backend_options"], 1))
            for concurrency in (1, 4):
                # Only clear databases after exact-owned container admission.
                local.inspect_owned(state)
                for endpoint in state["backend_options"]["nodes"]:
                    if industry_service.request(endpoint, "FLUSHALL") != "OK":
                        raise RuntimeError("owned replay database reset failed")
                config = configuration(backend, state["backend_options"], concurrency)
                path = directory / f"N{concurrency}.json"
                local.dump(path, config)
                run = directory / f"N{concurrency}"
                with (directory / f"N{concurrency}.log").open("w") as log:
                    subprocess.run([str(binary), "--config", str(path), "--output", str(run)],
                                   stdout=log, stderr=subprocess.STDOUT, timeout=75, check=True)
                evidence.append(check_result(run))
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
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = qualify(args.binary.resolve(), args.output.resolve())
    print(json.dumps(results, indent=2))
    return 0 if all(case["status"] == "completed" for case in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
