#!/usr/bin/env python3
"""Offline correctness checks. No profile proves EFA/RDMA hardware or performance."""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
TOOLCHAIN = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
PROFILES = ("format", "lint", "native", "distributed", "fabric", "software-fabric", "harness", "all")
SOFTWARE_STREAM_TESTS = (
    "software_provider_preserves_stream_bytes_across_reused_buffers",
    "software_provider_timeout_poison_and_teardown_preserve_buffer_lifetimes",
    "software_provider_pipelines_vectored_chunks_and_wraps_both_rings",
    "software_provider_nonblocking_multiple_sends_own_copies_and_wrap_rings",
    "software_provider_nonblocking_backpressure_resumes_without_replaying_unposted_bytes",
    "software_provider_nonblocking_close_keeps_pending_dma_slots_owned",
    "software_provider_explicit_sweeps_transfer_without_repolling",
    "software_provider_rma_reads_with_mixed_cq_and_retained_buffer_ownership",
    "software_provider_rma_group_preserves_order_with_retained_consumers",
)
SOFTWARE_ENGINE_TESTS = (
    "fabric_mtls_authentication_lost_response_and_recovery",
    "established_software_fabric_connection_survives_idle_timeout",
    "one_software_fabric_connection_completes_out_of_order",
    "fabric_control_violation_closes_a_live_session_without_waiting_for_rpc_deadline",
    "fabric_authenticated_ram_probe_boundaries_and_busy_shutdown",
    "software_fabric_cold_authority_preserves_hot_progress_and_rechecks_fencing",
    "native_software_provider_mixed_rpc_reads_complete_out_of_order",
    "fabric_tcp_native_rejects_invalid_reference_and_epoch_without_tls",
    "fabric_tcp_native_mode_mismatch_with_tls_fails_closed",
    "fabric_tcp_native_rejects_corrupt_member_reply_without_tls",
    "fabric_tcp_native_large_partial_frames_bypass_tls",
    "fabric_tcp_native_disconnect_retains_owned_reply_credits_until_retirement",
    "software_provider_read_io_monitor_reports_wal_and_real_rma_lane_activity",
    "software_provider_concurrent_cold_acquisition_with_one_handle_returns_both_values",
    "software_provider_durable_grant_reads_real_member_without_read_rpc",
    "software_provider_two_clients_submit_rma_before_either_completion_wait",
    "software_provider_idle_holder_cannot_acknowledge_fence_before_release",
    "software_provider_source_and_destination_limits_preserve_rpc_values",
    "software_provider_group_above_destination_slots_falls_back_without_reordering",
    "software_provider_corrupt_envelope_and_member_are_rejected",
    "software_provider_wrong_epoch_and_reference_never_return_stale_value",
    "software_provider_dropped_holder_quarantines_grant_and_blocks_fence",
    "software_provider_shutdown_releases_grants_before_clean_reopen",
    "software_provider_shutdown_drains_completed_read_while_result_handle_is_held",
    "software_provider_rma_off_history_retirement_drains_accepted_read",
    "software_provider_spontaneous_poison_waits_for_client_release_notification",
    "software_provider_release_immediately_after_rpc_reply_is_accepted",
    "software_provider_quarantine_keeps_original_wal_owner_after_node_drop",
    "software_provider_exact_drain_releases_original_wal_owner",
    "forced_server_grant_request_with_real_authority_uses_message_only_fallback",
    "expired_posted_rma_read_fail_stops_with_grant_memory_quarantined",
)


def run(command, env, capture=False):
    print("+ " + shlex.join(map(str, command)), flush=True)
    result = subprocess.run(command, cwd=ROOT, env=env, text=True,
                            stdout=subprocess.PIPE if capture else None,
                            stderr=subprocess.STDOUT if capture else None)
    if capture:
        print(result.stdout, end="", flush=True)
    result.check_returncode()
    return result.stdout if capture else ""


def cargo(env, subcommand, *args, capture=False):
    # fmt has no dependency resolution and does not accept --locked/--offline.
    flags = [] if subcommand == "fmt" else ["--locked", "--offline"]
    return run(["cargo", f"+{TOOLCHAIN}", subcommand, *flags, *args], env, capture)


def fabric_environment(env):
    env = env.copy()
    explicit = env.get("TIDEHUNTER_LIBFABRIC_PREFIX")
    cache = Path(env.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
    candidates = [Path(explicit).expanduser().resolve()] if explicit else [
        cache / "tidehunter/libfabric-2.6.0",
    ]
    # Ambient paths may name CUDA or another package, not libfabric. Preserve
    # compatible caller discovery; otherwise add the prepared prefix without
    # discarding the unrelated paths. Explicit prefixes still fail closed.
    caller_fabric = False
    if not explicit and any(env.get(key) for key in ("PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR", "LD_LIBRARY_PATH")):
        caller_fabric = subprocess.run(
            ["pkg-config", "--atleast-version=2.6.0", "libfabric"], env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ).returncode == 0
    if caller_fabric:
        print("Using compatible caller-provided libfabric discovery environment", flush=True)
    else:
        prefix = next((path for path in candidates if any(
            (path / lib / "pkgconfig/libfabric.pc").is_file() for lib in ("lib", "lib64"))), None)
        if prefix is None and explicit:
            raise RuntimeError(f"No libfabric.pc under requested prefix: {explicit}")
        if prefix:
            libraries = [prefix / lib for lib in ("lib", "lib64") if (prefix / lib).is_dir()]
            for key, paths in (("PKG_CONFIG_PATH", [p / "pkgconfig" for p in libraries]),
                               ("LD_LIBRARY_PATH", libraries)):
                existing = env.get(key)
                env[key] = os.pathsep.join([*(str(p) for p in paths), *([existing] if existing else [])])
            print(f"Using libfabric prefix: {prefix}", flush=True)
    run(["pkg-config", "--atleast-version=2.6.0", "libfabric"], env)
    run(["pkg-config", "--modversion", "libfabric"], env)
    return env


def selected_ignored(env, target, selector, expected):
    """List first, then run exact names; never sweep all ignored hardware tests."""
    output = cargo(env, "test", *target, selector, "--", "--ignored", "--list", capture=True)
    names = [line.removesuffix(": test") for line in output.splitlines() if line.endswith(": test")]
    short_names = [name.rsplit("::", 1)[-1] for name in names]
    if not names or len(names) != len(expected) or set(short_names) != set(expected):
        raise RuntimeError(f"Ignored-test selection {selector!r} changed: expected {expected}, found {names}")
    for name in names:
        output = cargo(env, "test", *target, name, "--", "--ignored", "--exact",
                       "--nocapture", "--test-threads=1", capture=True)
        if not re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output):
            raise RuntimeError(f"Selected test did not execute exactly once: {name}")


def check(profile, env):
    print(f"\n=== {profile}: correctness only; no hardware/performance claim ===", flush=True)
    if profile == "format":
        cargo(env, "fmt", "--all", "--", "--check")
    elif profile == "lint":
        for features in ((), ("--features", "experimental-distributed,test-utils")):
            cargo(env, "clippy", "-p", "tidehunter", "--all-targets", *features, "--", "-D", "warnings")
        cargo(env, "clippy", "-p", "tidehunter-fabric", "--all-targets", "--", "-D", "warnings")
    elif profile == "native":
        cargo(env, "test", "-p", "tidehunter")
    elif profile == "distributed":
        cargo(env, "test", "-p", "tidehunter", "--features", "experimental-distributed,test-utils")
    elif profile == "fabric":
        fabric = fabric_environment(env)
        for package, features in (("tidehunter-fabric", "native"),
                                  ("tidehunter", "experimental-fabric,test-utils")):
            cargo(fabric, "clippy", "-p", package, "--all-targets", "--features", features,
                  "--", "-D", "warnings")
            cargo(fabric, "test", "-p", package, "--lib", "--features", features)
        # Default integration tests include missing-provider rejection; their
        # software transfers and real-EFA discovery remain explicitly ignored.
        cargo(fabric, "test", "-p", "tidehunter-fabric", "--features", "native",
              "--test", "software_provider")
    elif profile == "software-fabric":
        fabric = fabric_environment(env)
        stream = ("-p", "tidehunter-fabric", "--features", "native", "--test", "software_provider")
        selected_ignored(fabric, stream, "software_provider_", SOFTWARE_STREAM_TESTS)
        direct = "software_provider_runs_direct_packet_adapter_on_large_partial_streams"
        selected_ignored(fabric, ("-p", "tidehunter-fabric", "--features", "native", "--lib"), direct, (direct,))
        strict_read = "strict_device_read_rejects_software_provider"
        selected_ignored(fabric, ("-p", "tidehunter-fabric", "--features", "native", "--lib"), strict_read, (strict_read,))
        engine = ("-p", "tidehunter", "--features", "experimental-fabric,test-utils", "--lib")
        for name in SOFTWARE_ENGINE_TESTS:
            selected_ignored(fabric, engine, name, (name,))
    elif profile == "harness":
        if env.get("TH_TEST_SYSTEMD") == "1":
            raise RuntimeError("harness is offline: unset TH_TEST_SYSTEMD; run real-systemd checks separately")
        print("Offline harnesses: optional MCP SDK tests may skip if absent; real-systemd exercise is excluded.", flush=True)
        for directory in ("experiments/aws-sharded-wal", "experiments/sui-checkpoint"):
            run([sys.executable, "-m", "unittest", "discover", "-s", directory,
                 "-p", "test_*.py", "-v"], env)


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=(
        "Prepare once: bash .github/scripts/setup.sh core|fabric. Checks never download dependencies; "
        "Cargo uses --locked --offline and rust-toolchain.toml. Set TIDEHUNTER_LIBFABRIC_PREFIX for a "
        "custom provider install; existing PKG_CONFIG_*/LD_LIBRARY_PATH and Cargo/Rust controls are retained. "
        "Tests default to two parallel cases; an explicit RUST_TEST_THREADS is retained. "
        "all includes software-fabric and fails if tcp;ofi_rxm cannot execute. EFA hardware tests stay ignored."))
    parser.add_argument("profile", choices=PROFILES)
    args = parser.parse_args()
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "always" if sys.stdout.isatty() else "never")
    # Keep concurrent five-node fixtures bounded on a single local SSD.
    env.setdefault("RUST_TEST_THREADS", "2")
    # Do not let rustup bootstrap a missing toolchain during ordinary checks.
    env["RUSTUP_AUTO_INSTALL"] = "0"
    try:
        for profile in PROFILES[:-1] if args.profile == "all" else (args.profile,):
            check(profile, env)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"checks: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
