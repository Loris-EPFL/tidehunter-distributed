#!/usr/bin/env python3
"""Offline correctness checks. No profile proves RDMA hardware or performance."""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))
from resident_config import software_fabric_environment
TOOLCHAIN = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
PROFILES = ("format", "lint", "native", "distributed", "fabric", "software-fabric", "harness",
            "file-length", "test-layout", "lint-backends", "all")
# Needs hosted-runner system packages; never part of `all` on a developer machine.
ALL_PROFILES = tuple(profile for profile in PROFILES[:-1] if profile != "lint-backends")
# Every feature-gated kv-research backend, grouped as a hosted runner can build it.
# The first set matches the replay-stores build plus the RESP clients; rocksdb compiles
# its own C++ tree and is linted alone so a failure names it. valkey-tcp alone compiles
# the `not(feature = "valkey-rdma")` arms that the full set turns off; the default
# features (the `lint` profile) compile the arms with both valkey features off.
BACKEND_FEATURE_SETS = (
    "tikv,cassandra,foundationdb,valkey-tcp,garnet,kvrocks,valkey-rdma",
    "valkey-tcp",
    "rocksdb",
)
SOFTWARE_STREAM_TESTS = (
    "software_provider_preserves_stream_bytes_across_reused_buffers",
    "software_provider_timeout_poison_and_teardown_preserve_buffer_lifetimes",
    "software_provider_pipelines_vectored_chunks_and_wraps_both_rings",
    "software_provider_nonblocking_multiple_sends_own_copies_and_wrap_rings",
    "software_provider_nonblocking_backpressure_resumes_without_replaying_unposted_bytes",
    "software_provider_nonblocking_close_keeps_pending_dma_slots_owned",
    "software_provider_explicit_sweeps_transfer_without_repolling",
    "software_provider_idle_wait_wakes_once_and_transfers_both_ways",
    "software_provider_pending_send_disables_idle_cq_wait",
    "software_provider_buffered_receive_disables_idle_cq_wait",
    "software_provider_records_effective_geometry_and_rxm_parameters",
    "software_provider_transfers_with_a_larger_slot_count",
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


FILE_LENGTH_LIMIT = 1500
RATCHET_ROOTS = (
    "tidehunter/src/distributed",
    "tidehunter/examples/support",
    "tidehunter/benches",
    "benchmarks/kv-research/src",
    "tidehunter-fabric/src",
)
FILE_LENGTH_BASELINE = ROOT / ".github/scripts/file-length-baseline.txt"
TEST_LAYOUT_BASELINE = ROOT / ".github/scripts/test-layout-baseline.txt"
TEST_LAYOUT_ATTRIBUTE = re.compile(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]")
# `#[test]`, `#[tokio::test]`, `#[tokio::test(flavor = "multi_thread")]` and any `#[a::b::test]`.
TEST_ATTRIBUTE = re.compile(r"#\s*\[\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*test\s*(?:\(|\])")
MODULE_DECLARATION = re.compile(
    r"(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+[A-Za-z_][A-Za-z0-9_]*\s*([{;])")


def read_baseline(path, fields):
    """Parse "<path> <n>..." lines; `fields` numbers follow each path."""
    baseline = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(maxsplit=fields)
        try:
            numbers = tuple(int(value) for value in parts[1:])
        except ValueError:
            numbers = ()
        if len(parts) != fields + 1 or len(numbers) != fields:
            raise RuntimeError(
                f"{path.name}: malformed baseline line {line!r}; "
                f"expected a path followed by {fields} whole number(s)")
        if parts[0] in baseline:
            raise RuntimeError(f"{path.name}: duplicate baseline entry for {parts[0]}")
        baseline[parts[0]] = numbers
    return baseline


def rust_files(root):
    """Yield (relative path, absolute path) of every Rust file under the ratchet roots."""
    for relative_root in RATCHET_ROOTS:
        directory = root / relative_root
        if not directory.is_dir():
            raise RuntimeError(f"ratchet root is missing: {relative_root}")
        for path in directory.rglob("*.rs"):
            yield path.relative_to(root), path


def check_file_length(root=ROOT, baseline_path=FILE_LENGTH_BASELINE):
    """Exact ratchet: a listed file equals its recorded length; others stay within the cap."""
    baseline = {path: values[0] for path, values in read_baseline(baseline_path, 1).items()}
    problems = []
    counts = {}
    for relative, path in sorted(rust_files(root)):
        relative = str(relative)
        counts[relative] = len(path.read_bytes().splitlines())
    for relative, lines in counts.items():
        recorded = baseline.get(relative)
        if recorded is None:
            if lines > FILE_LENGTH_LIMIT:
                problems.append(f"{relative}: {lines} lines, over the cap and not in the baseline")
        elif lines > recorded:
            problems.append(f"{relative}: {lines} lines, grew past baseline {recorded}")
        elif lines < recorded and lines > FILE_LENGTH_LIMIT:
            problems.append(f"{relative}: {lines} lines, below baseline {recorded}; "
                            f"change the baseline line to: {relative} {lines}")
    for relative in sorted(baseline):
        lines = counts.get(relative)
        if lines is None or lines <= FILE_LENGTH_LIMIT:
            problems.append(f"{relative}: stale baseline entry (file missing or within the cap); "
                            f"delete the line: {relative} {baseline[relative]}")
    if problems:
        raise RuntimeError(
            "file-length ratchet failed; the baseline may only shrink, in the same change "
            "that shrinks the file:\n  " + "\n  ".join(problems))


def without_comments_and_strings(source):
    """Remove comments and literals so attributes inside them are not counted."""
    out = []
    index = 0
    length = len(source)
    while index < length:
        char = source[index]
        if source.startswith("//", index):
            newline = source.find("\n", index)
            index = length if newline < 0 else newline
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            index = length if end < 0 else end + 2
        elif char == '"':
            index += 1
            while index < length and source[index] != '"':
                index += 2 if source[index] == "\\" else 1
            index += 1
        elif char == "r" and index + 1 < length and source[index + 1] in '#"':
            hashes = 0
            cursor = index + 1
            while cursor < length and source[cursor] == "#":
                hashes += 1
                cursor += 1
            if cursor < length and source[cursor] == '"':
                end = source.find('"' + "#" * hashes, cursor + 1)
                index = length if end < 0 else end + 1 + hashes
            else:
                out.append(char)
                index += 1
        elif char == "'":
            # Skip a character literal; a lifetime has no closing quote.
            cursor = index + 1
            escaped = cursor < length and source[cursor] == "\\"
            cursor += 2 if escaped else 1
            if cursor < length and source[cursor] == "'":
                index = cursor + 1
            else:
                out.append(char)
                index += 1
        else:
            out.append(char)
            index += 1
    return "".join(out)




def count_inline_tests(source):
    """Return (inline #[cfg(test)] module bodies, test functions) in one file."""
    stripped = without_comments_and_strings(source)
    modules = 0
    for attribute in TEST_LAYOUT_ATTRIBUTE.finditer(stripped):
        tail = stripped[attribute.end():].lstrip()
        while tail.startswith("#"):
            opening = re.match(r"#\s*\[", tail)
            if opening is None:
                break
            depth = 0
            cursor = opening.end() - 1
            while cursor < len(tail):
                if tail[cursor] == "[":
                    depth += 1
                elif tail[cursor] == "]":
                    depth -= 1
                    if depth == 0:
                        break
                cursor += 1
            tail = tail[cursor + 1:].lstrip()
        declaration = MODULE_DECLARATION.match(tail)
        if declaration is not None and declaration.group(1) == "{":
            modules += 1
    return modules, len(TEST_ATTRIBUTE.findall(stripped))


def is_test_only_file(relative):
    """Recognise separate test files by name only; no module-tree parsing."""
    return (relative.name == "tests.rs" or relative.name.endswith("_tests.rs")
            or any(part == "tests" or part.endswith("_tests") for part in relative.parts[:-1]))


def layout_counts(root):
    """Count inline tests per production file; separate test files are skipped."""
    return {str(relative): count_inline_tests(path.read_text(encoding="utf-8"))
            for relative, path in rust_files(root) if not is_test_only_file(relative)}


def check_test_layout(root=ROOT, baseline_path=TEST_LAYOUT_BASELINE):
    """Exact ratchet: inline test modules and test functions equal the baseline."""
    baseline = read_baseline(baseline_path, 2)
    counts = layout_counts(root)
    problems = []
    total = [0, 0, 0]
    for relative, (modules, tests) in sorted(counts.items()):
        if not modules and not tests:
            continue
        recorded = baseline.get(relative)
        if recorded is None:
            problems.append(
                f"{relative}: {modules} inline test modules and {tests} tests, not in the "
                "baseline; move them to a sibling test file")
            continue
        total = [total[0] + 1, total[1] + modules, total[2] + tests]
        if recorded != (modules, tests):
            direction = "grew past" if modules > recorded[0] or tests > recorded[1] else "fell below"
            problems.append(
                f"{relative}: {modules} modules and {tests} tests {direction} baseline "
                f"{recorded[0]}/{recorded[1]}; the baseline line must read: "
                f"{relative} {modules} {tests}")
    for relative in sorted(baseline):
        if counts.get(relative, (0, 0)) == (0, 0):
            problems.append(
                f"{relative}: stale baseline entry (file missing or no inline tests left); "
                f"delete the line: {relative} {baseline[relative][0]} {baseline[relative][1]}")
    print("test-layout debt (inline tests in production files): "
          f"{total[0]} files, {total[1]} modules, {total[2]} tests", flush=True)
    if problems:
        raise RuntimeError(
            "test-layout ratchet failed; the baseline may only shrink, in the same change "
            "that moves the tests:\n  " + "\n  ".join(problems))



def check(profile, env):
    print(f"\n=== {profile}: correctness only; no hardware/performance claim ===", flush=True)
    if profile == "format":
        cargo(env, "fmt", "--all", "--", "--check")
    elif profile == "file-length":
        check_file_length()
    elif profile == "test-layout":
        check_test_layout()
    elif profile == "lint":
        for features in ((), ("--features", "experimental-distributed,test-utils")):
            cargo(env, "clippy", "-p", "tidehunter", "--all-targets", *features, "--", "-D", "warnings")
        cargo(env, "clippy", "-p", "tidehunter-fabric", "--all-targets", "--", "-D", "warnings")
        cargo(env, "clippy", "-p", "kv-research", "--all-targets", "--", "-D", "warnings")
    elif profile == "lint-backends":
        if env.get("GITHUB_ACTIONS") != "true" or not env.get("VALKEY_SOURCE_DIR"):
            raise RuntimeError("lint-backends needs the hosted runner packages and a prepared "
                               "VALKEY_SOURCE_DIR; run it only from the kv-comparison workflow")
        for features in BACKEND_FEATURE_SETS:
            cargo(env, "clippy", "-p", "kv-research", "--all-targets", "--features", features,
                  "--", "-D", "warnings")
    elif profile == "native":
        cargo(env, "test", "-p", "tidehunter")
    elif profile == "distributed":
        # test_methods adds only test-facing items (observed relocation); it enables relocation_tests.
        cargo(env, "test", "-p", "tidehunter", "--features",
              "experimental-distributed,test-utils,test_methods")
        cargo(env, "test", "-p", "kv-research", "--features", "test-utils")
    elif profile == "fabric":
        fabric = fabric_environment(env)
        cargo(fabric, "clippy", "-p", "tidehunter", "--all-targets", "--features",
              "experimental-distributed,experimental-fabric", "--", "-D", "warnings")
        cargo(fabric, "clippy", "-p", "kv-research", "--all-targets", "--features",
              "fabric", "--", "-D", "warnings")
        for package, features in (("tidehunter-fabric", "native"),
                                  ("tidehunter", "experimental-distributed,experimental-fabric,test-utils"),
                                  ("kv-research", "fabric,test-utils")):
            cargo(fabric, "clippy", "-p", package, "--all-targets", "--features", features,
                  "--", "-D", "warnings")
            cargo(fabric, "test", "-p", package, "--features", features)
    elif profile == "software-fabric":
        fabric = software_fabric_environment(fabric_environment(env), software=True)
        stream = ("-p", "tidehunter-fabric", "--features", "native", "--test", "software_provider")
        selected_ignored(fabric, stream, "software_provider_", SOFTWARE_STREAM_TESTS)
    elif profile == "harness":
        for directory in ("experiments/kv-comparison", "experiments/kv-comparison/observability",
                          "experiments/gcp-rdma"):
            run([sys.executable, "-B", "-m", "unittest", "discover", "-s", directory,
                 "-p", "test_*.py", "-v"], env)


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=(
        "Prepare once: bash .github/scripts/setup.sh core|fabric. Checks never download dependencies; "
        "Cargo uses --locked --offline and rust-toolchain.toml. Set TIDEHUNTER_LIBFABRIC_PREFIX for a "
        "custom provider install; existing PKG_CONFIG_*/LD_LIBRARY_PATH and Cargo/Rust controls are retained. "
        "Tests default to two parallel cases; an explicit RUST_TEST_THREADS is retained. "
        "all includes software-fabric and fails if tcp;ofi_rxm cannot execute. "
        "lint-backends runs only on a GitHub-hosted runner (see kv-comparison.yaml) and is not part of all."))
    parser.add_argument("profile", choices=PROFILES)
    args = parser.parse_args()
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "always" if sys.stdout.isatty() else "never")
    # Keep concurrent five-node fixtures bounded on a single local SSD.
    env.setdefault("RUST_TEST_THREADS", "2")
    # Do not let rustup bootstrap a missing toolchain during ordinary checks.
    env["RUSTUP_AUTO_INSTALL"] = "0"
    try:
        for profile in ALL_PROFILES if args.profile == "all" else (args.profile,):
            check(profile, env)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"checks: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
