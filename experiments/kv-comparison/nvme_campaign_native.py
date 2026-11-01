"""Prepare one persistent native dataset and construct immutable read runs.

The caller owns the output directory and must serialize access to its database.
Preparation is deliberately one-shot: a nonempty output directory is rejected.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

import analyze
import campaign


def _dump(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _digest(path, algorithm="sha256"):
    digest = hashlib.new(algorithm)
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _identity(config):
    namespace = config.get("key_namespace_hex")
    if not isinstance(namespace, str) or not re.fullmatch(r"[0-9a-f]{32}", namespace):
        raise ValueError("an explicit 32-digit lowercase key_namespace_hex is required")
    if config.get("backend", {}).get("kind") != "native":
        raise ValueError("native backend required")
    return {"key_namespace_hex": namespace, "seed": config["seed"],
            "dataset": copy.deepcopy(config["dataset"])}


def _run(argv, log, deadline, environment=None):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("native preparation deadline expired")
    with log.open("wb") as output, log.with_suffix(".stderr.log").open("wb") as errors:
        process = subprocess.Popen([str(x) for x in argv], stdout=output,
                                   stderr=errors, start_new_session=True, env=environment)
        try:
            code = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise TimeoutError("native preparation subprocess exceeded deadline") from exc
    if code:
        raise RuntimeError(f"native preparation subprocess exited {code}; see {log}")


def prepare_native(binary, config, output_dir, timeout_seconds, environment=None):
    """Preload a new native database; return its receipt only after raw admission.

    `config` is a complete kv-research config with an explicit key namespace.
    `output_dir` must be absolute and empty or absent; no existing data is reused.
    The one absolute deadline covers preflight and the entire preparation run.
    `environment`, when supplied, is passed unchanged to both subprocesses.
    """
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not 0 < timeout_seconds <= 86400:
        raise ValueError("timeout_seconds must be positive and at most 86400")
    deadline = time.monotonic() + timeout_seconds
    identity = _identity(config)
    binary = Path(binary).resolve(strict=True)
    root = Path(output_dir)
    if not root.is_absolute():
        raise ValueError("output_dir must be absolute")
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or any(root.iterdir()):
        raise ValueError("native preparation directory must be empty and not a symlink")
    root = root.resolve(strict=True)
    prepared = copy.deepcopy(config)
    prepared["backend"]["options"]["path"] = str(root / "database")
    prepared["phases"] = {"preload": True, "verify_existing_dataset": False,
                          "warmup_operations": 0, "aging_operations": 0,
                          "measurement_operations": 1}
    prepared["workload"]["mix"] = {"read": 100, "put": 0, "put_batch": 0,
                                   "delete": 0, "missing_read": 0, "get_many": 0}
    prepared["qualification_only"] = False
    prepared["measurement_gate"] = None
    prepared["measurement_completion_gate"] = None
    prepared["limits"]["max_operations"] = max(prepared["limits"]["max_operations"], identity["dataset"]["keys"] + 1)
    # The runner must not outlive the controller's remaining wall-clock budget.
    prepared["limits"]["deadline_seconds"] = min(prepared["limits"]["deadline_seconds"], max(1, int(timeout_seconds)))
    config_path = root / "config.json"
    _dump(config_path, prepared)
    receipt = {"schema_version": 1, "status": "failed", "identity": identity,
               "database_path": str(root / "database"), "config": prepared,
               "binary_sha256": _digest(binary), "binary_blake2b512": _digest(binary, "blake2b"),
               "raw_result": str(root / "result")}
    try:
        _run([binary, "--config", config_path, "--validate-config"], root / "admission.log", deadline, environment)
        envelope = json.loads((root / "admission.log").read_text())
        admitted = envelope["admitted_config"]
        analyze.verify_normalization(prepared, admitted)
        _dump(root / "normalized-config.json", admitted)
        _run([binary, "--config", config_path, "--output", root / "result"], root / "runner.log", deadline, environment)
        result = root / "result"
        campaign.verify_run_identity(result, receipt, admitted)
        manifest = json.loads((result / "manifest.json").read_text())
        summary = json.loads((result / "summary.json").read_text())
        if (manifest.get("schema_version") != 1 or manifest.get("raw_samples_complete") is not True
                or summary.get("status") != "completed" or summary.get("backend") != manifest.get("backend_info")):
            raise ValueError("native preparation terminal evidence is incomplete")
        receipt["timing_rows"] = campaign.verify_timing_result(result, admitted)
        if any(row["outcome"] != "success" for row in analyze.read_samples(result / "samples.csv", measurement_only=False)):
            raise ValueError("native preparation contains unsuccessful operations")
        receipt["artifacts_sha256"] = {p.name: _digest(p) for p in result.iterdir() if p.is_file()}
        receipt["status"] = "completed"
        return receipt
    except Exception as exc:
        receipt["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        _dump(root / "receipt.json", receipt)


def read_config(receipt, config):
    """Bind a read-only run to the prepared data, retaining its caller-owned gates."""
    if receipt.get("status") != "completed" or _identity(config) != receipt.get("identity"):
        raise ValueError("native dataset receipt is incomplete or dataset identity differs")
    result = copy.deepcopy(config)
    if result.get("qualification_only") or result["phases"].get("aging_operations", 0):
        raise ValueError("native dataset reads cannot qualify or age the dataset")
    mix = result["workload"]["mix"]
    if any(mix.get(name, 0) != 0 for name in ("put", "put_batch", "delete", "missing_read")):
        raise ValueError("native dataset measurement must be read-only")
    result["backend"]["options"] = copy.deepcopy(receipt["config"]["backend"]["options"])
    result["backend"]["options"]["path"] = receipt["database_path"]
    result["phases"]["preload"] = False
    result["phases"]["verify_existing_dataset"] = True
    return result
