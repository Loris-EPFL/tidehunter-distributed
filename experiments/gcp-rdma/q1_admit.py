"""Check an exact Q1 seal/package without initializing any cloud clients."""

import argparse
import json
import re
import sys
from pathlib import Path

from q1_capacity import read_r0, validate_admission
from q1_compare import validate_case, validate_point_report
from q1_index import validate_a1_selection, validate_stage
from q1_manifest import CAPACITY, COMPARISON, MANDATORY, validate_binding, validate_seal
from q1_package import artifact, hex_string, require, verify_package


def _sealed_json(root, spec, name, *, proof=None):
    sha = spec["inputs"].get(name)
    require(sha is not None and (proof is None or spec["proofs"][proof] == sha),
            f"{name}: producer evidence not bound to seal")
    return json.loads(artifact(root, name, sha).read_text())


def verify_image_recipe(receipt, image_id, recipes):
    require(type(receipt["schema_version"]) is int and receipt["schema_version"] == 1
            and receipt["image"]["id"] == str(image_id)
            and receipt["recipes"] == recipes, "qualification must match this image and the refreshed Q1 recipe")
    evidence = receipt["evidence"]
    require(hex_string(receipt["runtime_sha256"], 64) and len(evidence) == 2
            and all(set(row) == {"bucket", "object", "generation", "sha256"}
                    and row["bucket"] and row["object"] and not row["object"].startswith("/")
                    and re.fullmatch(r"[1-9][0-9]*", row["generation"]) and hex_string(row["sha256"], 64)
                    for row in evidence)
            and evidence[0]["bucket"] == evidence[1]["bucket"] and evidence[0]["object"] != evidence[1]["object"],
            "qualification lacks two distinct generation-pinned evidence objects")


def replay_memory_binding(root, spec, admit_memory):
    """Reuse HarnessFixes' exact workload and uncensored-peak validator."""
    jobs_name, policy_name = "q1-replay-jobs.json", "q1-replay-memory.json"
    includes = {jobs_name, policy_name} & spec["inputs"].keys()
    if not includes:
        return None
    require(len(includes) == 2 and any(cell["id"] == CAPACITY for cell in spec["cases"]),
            "replay inputs require the sealed capacity cell")
    jobs = _sealed_json(root, spec, jobs_name)
    policy = _sealed_json(root, spec, policy_name)
    fixture = "experiments/sui-mainnet/fixture/manifest.json"
    require(fixture in spec["inputs"], "replay fixture manifest not sealed")
    for entry in policy.get("workloads", {}).values():
        for binding in entry["evidence"]:
            require(spec["inputs"].get(binding["path"]) == binding["sha256"], "unsealed memory peak evidence")
    sibling = str(Path(__file__).resolve().parents[1] / "kv-comparison")
    if sibling not in sys.path:
        sys.path.insert(0, sibling)
    return admit_memory(policy, jobs, root, "z4m-highmem-16")


def _capacity_reports(root, spec, config):
    metadata = _sealed_json(root, spec, "q1-capacity-r0.json")
    require(set(metadata) == {"sui_mainnet_retention", "synthetic_churn"}, "R0 needs both workload classes")
    for workload, binding in metadata.items():
        require(set(binding) == {"path", "run_id", "input_digest"} and binding["path"] in spec["inputs"],
                "R0 artifact not bound to seal")
        if workload == "sui_mainnet_retention":
            require(binding["input_digest"] == config["retention_sha"], "R0 retention input differs")
        else:
            require(binding["input_digest"] is None or hex_string(binding["input_digest"], 64), "invalid churn input")
        path = artifact(root, binding["path"], spec["inputs"][binding["path"]])
        read_r0(path, run_id=binding["run_id"], workload=workload,
                input_digest=binding["input_digest"], min_rolls=config["cycles"])


def admit(spec, seal_sha, root):
    require(validate_seal(spec) == seal_sha, "seal digest differs from admission")
    verify_package(root, spec)
    from gcp_image import recipe_hashes
    from gcp_image_prepare import TRACE_BOOT_ARG
    from gcp_runtime import admit_replay_memory

    require(TRACE_BOOT_ARG == "trace_buf_size=4K", "Q1 requires the trace-capture image recipe")
    verify_image_recipe(_sealed_json(root, spec, "q1-image-qualification.json"), spec["image_id"], recipe_hashes())
    if any(cell["id"] in MANDATORY[2:] for cell in spec["cases"]):
        # index cases are sealed: the A1 decision is a pre-launch input
        validate_a1_selection(_sealed_json(root, spec, "q1-a0-v6.json", proof="a0"),
                              _sealed_json(root, spec, "q1-a1-run-manifest.json"),
                              _sealed_json(root, spec, "q1-a1-decision.json", proof="a1"),
                              spec["exact"])
    replay_memory_binding(root, spec, admit_replay_memory)
    for cell in spec["cases"]:
        _admit_case(root, spec, cell)


def _admit_case(root, spec, cell):
    """Bind one sealed case contract at pre-launch admission. Measured
    evidence (capacity reports, comparison point reports) is validated by the
    post-run collect steps, never here."""
    case = cell["id"]
    path = f"q1-{case}.json"
    require(spec["inputs"].get(path) == cell["inputs_sha"], f"{case}: case inputs not sealed")
    config = json.loads((root / path).read_text())
    if case == CAPACITY:
        validate_admission(config)
        require(config["deadline_seconds"] == cell["seconds"], "capacity time differs from seal")
        _capacity_reports(root, spec, config)
    elif case in MANDATORY[2:]:
        validate_stage(config)
        require(config["exact"] == spec["exact"] and case == f"index_{config['tier']}",
                "wrong case tier or Exact selection")
    elif case in COMPARISON:
        # pre-launch: bind the case contract only. The point report holds
        # measured results and is validated after the run by
        # admit_comparison_points (the collect step), never here.
        validate_case(config, case)
    else:
        require(config == {"tier": "native", "epoch_duration_ms": 30_000, "restart": "V3",
                           "retention": "L2-retain-all", "workload_sha": spec["inputs"]["q1-workload.json"],
                           "control": "tcp_mtls", "data": "nvme_rdma_kernel_buffered_per_owner"},
                "wrong golden case contract")


def admit_comparison_points(receipt, root, spec, case):
    """Post-run collect step for a comparison case: the measured point report
    arrives as a contributor artifact of the case's `collect` receipt and is
    bound by that receipt's own path/sha256/generation — never through the
    seal's frozen (pre-launch) inputs."""
    require(case in COMPARISON and any(cell["id"] == case for cell in spec["cases"]),
            "comparison case is not part of this seal")
    config = _sealed_json(root, spec, f"q1-{case}.json")
    validate_case(config, case)
    name = f"q1-{case}-points.json"
    matches = [item for item in receipt["artifacts"] if str(item["path"]).endswith(name)]
    require(len(matches) == 1, f"{case}: the collect receipt must bind exactly one points report")
    item = matches[0]
    path = artifact(root, item["path"], item["sha256"])
    report = json.loads(path.read_text())
    validate_point_report(report, case, config)
    return report

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package", type=Path)
    parser.add_argument("seal", type=Path)
    parser.add_argument("seal_sha")
    parser.add_argument("--placement", type=Path)
    parser.add_argument("--placement-sha")
    args = parser.parse_args()
    spec = json.loads(args.seal.read_text())
    admit(spec, args.seal_sha, args.package.absolute())
    if args.placement is not None or args.placement_sha is not None:
        require(args.placement is not None and args.placement_sha is not None, "incomplete placement arguments")
        binding = json.loads(args.placement.read_text())
        require(validate_binding(spec, args.seal_sha, binding) == args.placement_sha, "placement digest mismatch")
    print(json.dumps({"profile": spec["profile"], "seal_sha": args.seal_sha,
                      "placement_sha": args.placement_sha, "offline_validation": "passed"}))


if __name__ == "__main__":
    main()
