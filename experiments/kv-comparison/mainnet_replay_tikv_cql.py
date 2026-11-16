#!/usr/bin/env python3
"""Cached-image-only TiKV RawKV and Cassandra LOGGED BATCH replay qualification.

Each N uses a fresh exact-owned lifecycle. No image pulls or cloud actions.
Local containers share one host and filesystem; this is functional evidence only.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import local
from mainnet_replay_resp import check_result, configuration

BACKENDS = ("tikv", "cassandra")


def options(state, directory):
    if state["backend"] == "tikv":
        return {"pd_endpoints": ["127.0.0.1:2379"],
                "pd_api_endpoints": ["http://127.0.0.1:2379"],
                "required_replication_factor": 1, "request_timeout_ms": 10000}
    proof = local.probe(state)
    nodes = [f"127.0.0.{11+i}:9042" for i in range(len(proof["nodes"]))]
    yamls = [node["effective_yaml"] for node in proof["nodes"]]
    if (any("batch_size_fail_threshold: 1MiB" not in text.splitlines() for text in yamls)
            or any("batch_size_warn_threshold: 5KiB" not in text.splitlines() for text in yamls)):
        raise RuntimeError("running Cassandra YAML does not contain admitted replay guardrails")
    effective_settings = []
    for index, endpoint in enumerate(nodes):
        address, port = endpoint.rsplit(":", 1)
        text = local.execute(state, f"cassandra{index}", "cqlsh", address, port, "-e",
                             "SELECT JSON name, value FROM system_views.settings;")
        rows = [json.loads(line.strip()) for line in text.splitlines() if line.strip().startswith("{")]
        settings = {row["name"]: row["value"] for row in rows}
        if (settings.get("batch_size_fail_threshold") != "1MiB"
                or settings.get("batch_size_warn_threshold") != "5KiB"
                or settings.get("commitlog_sync") != "periodic"
                or not isinstance(settings.get("commitlog_sync_period"), str)):
            raise RuntimeError(f"running Cassandra guardrail/commitlog settings mismatch on {endpoint}")
        effective_settings.append({"node": endpoint, "settings": settings})
    receipt = directory / "cassandra-server-config.json"
    local.dump(receipt, {"backend": "cassandra", "server_version": "5.0.9",
                        "owner": state["owner"], "nodes": nodes, "effective_yaml": yamls,
                        "effective_settings": effective_settings,
                        "batch_size_fail_threshold_bytes": 1048576,
                        "batch_size_warn_threshold_bytes": 5120,
                        "observed_at_unix": time.time()})
    return {"nodes": nodes, "keyspace": "kv_comparison", "required_replication_factor": len(nodes),
            "request_timeout_ms": 10000, "consistency": "quorum",
            "replay_server_config_receipt": str(receipt)}


def qualify(binary, output, backends=BACKENDS):
    output.mkdir(parents=True, exist_ok=False)
    local.dump(output / "host.json", {"cpu_count": os.cpu_count(),
               "meminfo": Path("/proc/meminfo").read_text(),
               "disk": local.command(["df", "-B1", str(output)]),
               "docker_images": local.command(["docker", "images", "--digests", "--no-trunc"]),
               "physical_hosts": 1, "independent_disks": False,
               "functional_qualification_only": True})
    results = {}
    for backend in backends:
        directory = output / backend
        directory.mkdir()
        evidence = []
        try:
            images = ("pd", "tikv") if backend == "tikv" else ("cassandra",)
            for image in images:
                local.command(["docker", "image", "inspect", local.IMAGES[image]])
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as error:
            results[backend] = {"status": "not_run", "reason": f"cached pinned image missing: {error}"}
            local.dump(output / "results.json", results)
            continue
        started = time.monotonic()
        try:
            for concurrency in (1, 4):
                run_dir = directory / f"N{concurrency}-lifecycle"
                run_dir.mkdir()
                state_path = run_dir / "service"
                state = None
                try:
                    local.start(SimpleNamespace(backend=backend, layout="qualification", state=state_path,
                                               timeout=180, ack_contract="synced", cached_images_only=True,
                                               mainnet_replay=True))
                    state = local.load(state_path)
                    local.inspect_owned(state)
                    config = configuration(backend, options(state, run_dir), concurrency)
                    path = directory / f"N{concurrency}.json"
                    local.dump(path, config)
                    run = directory / f"N{concurrency}"
                    with (directory / f"N{concurrency}.log").open("w") as log:
                        subprocess.run([str(binary), "--config", str(path), "--output", str(run)],
                                       stdout=log, stderr=subprocess.STDOUT, timeout=90, check=True)
                    result = check_result(run)
                    if result["verification"]["verified_rows"] != 1982:
                        raise RuntimeError("two-repetition final row count differs from pinned fixture")
                    expected_mapping = "raw_batch_put" if backend == "tikv" else "cql_logged_batch"
                    if result["replay_contract"]["commit_mapping"] != expected_mapping:
                        raise RuntimeError("replay capability mapping mismatch")
                    evidence.append(result)
                finally:
                    if state is None and (state_path / "owner.json").exists():
                        state = local.load(state_path)
                    if state is not None:
                        try:
                            local.export(state, run_dir / "service-evidence")
                        finally:
                            local.stop(state)
            if evidence[0]["verification"] != evidence[1]["verification"]:
                raise RuntimeError("N=1 and N=4 final contents differ")
            results[backend] = {"status": "completed", "runs": evidence,
                                "lifecycle_and_replay_seconds": time.monotonic() - started}
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as error:
            results[backend] = {"status": "failed", "reason": str(error), "completed_runs": evidence}
        local.dump(output / "results.json", results)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--backend", choices=BACKENDS, action="append")
    args = parser.parse_args()
    results = qualify(args.binary.resolve(), args.output.resolve(), args.backend or BACKENDS)
    print(json.dumps(results, indent=2))
    return 0 if all(case["status"] == "completed" for case in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
