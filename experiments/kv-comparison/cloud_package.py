#!/usr/bin/env python3
"""Freeze dirty sources, build a portable comparison executable, and seal a runtime.

No AWS access. A receipt is written only after the corresponding operation succeeds.
All output directories must be new. See README.md for local/container commands.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[2]
FEATURES = ["tikv", "cassandra", "foundationdb", "valkey-tcp", "fabric"]
TARGET = "x86_64-unknown-linux-gnu"
MAX_GLIBC = (2, 39)  # Ubuntu 24.04, not the build host's version.
MAX_GLIBCXX = (3, 4, 33)  # Ubuntu 24.04 libstdc++6 (GCC 14).
MAX_SOURCE = 256 * 1024 * 1024
MAX_FILE = 16 * 1024 * 1024
MAX_BUNDLE = 512 * 1024 * 1024
MAX_FILES = 20000
FDB_IMAGE = "foundationdb/foundationdb:7.3.77@sha256:559b178a137bbe09272ad6e3752531aa04e2ddacab8ccfa4ffa86467db033bea"
# Independently observed from the native client in the pinned server image.
FDB_SHA256 = "617572467897d0360f76ffc33f24b58090ee691e3273750d2ff334cc71aae3fa"
FDB_BLAKE2B = "00724ddd562fe794722eefb0acd3fb8ccfcd001500a5c8912aa34ba1b0853350a9da34f47268d4bf514285fa21088c809aa8c2fcf965e253f39498b38deee638"
EFA = {"installer_version": "1.50.0", "installer_sha256": "fa6dff8593d866866c13cb4640d9059835cd4efa427971f100ab40c97bef2841",
       "libfabric_version": "2.6.0amzn1.0", "provider_version": "206.0", "rdm_protocol_version": 4, "direct_protocol_version": 1,
       "upstream_commit": "3cc1095272b07ebeb5cc8f44851af6aa3eb75e41",
       "runtime_library_dir": "/opt/amazon/efa/lib", "required_environment": {"FI_EFA_USE_DEVICE_RDMA": "1"},
       "hardware_admission": "provider efa, RDM fabrics efa (MSG|DIRECTED_RECV) and efa-direct (MSG); registered-RAM additionally requires efa-direct RMA|READ|REMOTE_READ; Rust enforces geometry and native device reads; no TCP fallback"}
NEW_ROOTS = ("benchmarks/kv-research/", "experiments/kv-comparison/", "experiments/portable_transport.py")
SUFFIXES = {".rs", ".toml", ".lock", ".py", ".json", ".yaml", ".yml", ".sh", ".md", ".c", ".h", ".cpp", ".proto", ".html", ".css", ".js", ".txt"}
EXCLUDED = {".git", "target", "checkpoint-work", "logs", "results", "artifacts", "data", "secrets", "credentials", ".state", "__pycache__", ".aws", ".ssh", "node_modules", ".venv"}
REQUIRED = {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/config.toml", "benchmarks/kv-research/Cargo.toml",
            "benchmarks/kv-research/src/main.rs", "tidehunter/Cargo.toml", "tidehunter-fabric/Cargo.toml",
            "experiments/portable_transport.py", "experiments/resident_config.py",
            "experiments/kv-comparison/cloud_package.py", "experiments/kv-comparison/cloud_controller.py",
            "experiments/kv-comparison/cloud_campaign.py", "experiments/kv-comparison/cloud_layout.py",
            "experiments/kv-comparison/cloud-campaign.json", "experiments/kv-comparison/local.py",
            "experiments/kv-comparison/resource_accounting.py",
            "experiments/kv-comparison/observability/campaign_monitor.py",
            "experiments/kv-comparison/observability/campaign_exporter.py",
            "experiments/kv-comparison/observability/exporter.py",
            "experiments/kv-comparison/observability/images.lock.json",
            "experiments/kv-comparison/observability/dashboards/campaign.json"}


def command(argv, *, cwd=None, env=None, timeout=60):
    return subprocess.check_output([str(x) for x in argv], cwd=cwd, env=env, text=True,
                                   stderr=subprocess.STDOUT, timeout=timeout).strip()


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def dump(path, value):
    path.write_bytes(encode(value))


def identity(path):
    regular(path)
    sha, blake = hashlib.sha256(), hashlib.blake2b()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(block)
            blake.update(block)
    return {"bytes": path.stat().st_size, "sha256": sha.hexdigest(), "blake2b512": blake.hexdigest()}


def regular(path):
    if not stat.S_ISREG(path.lstat().st_mode) or any(p.is_symlink() for p in path.parents):
        raise ValueError(f"not an ordinary non-symlink file: {path}")


def safe_name(name):
    path = PurePosixPath(name)
    if (not name or path.is_absolute() or ".." in path.parts or "\\" in name
            or path.as_posix() != name or any(ord(c) < 32 for c in name)):
        raise ValueError(f"unsafe archive path: {name!r}")
    return path


def allowed(name):
    path = safe_name(name)
    lowered = {x.lower() for x in path.parts}
    if path.parts[:2] == ("experiments", "valkey-infiniband"):
        return False
    if lowered & EXCLUDED or path.name.startswith(".env") or path.suffix.lower() in {".key", ".pem", ".p12", ".pfx"}:
        return False
    if any("credential" in x or "secret" in x for x in lowered):
        return False
    return path.suffix in SUFFIXES or path.name in {".gitignore", "Dockerfile", "LICENSE"} or path.name.startswith("Dockerfile.")


def source_paths(repo):
    tracked = command(["git", "ls-files", "-z"], cwd=repo).split("\0")
    new = command(["git", "ls-files", "--others", "--exclude-standard", "-z", "--", *NEW_ROOTS,
                   ".github/workflows/kv-comparison.yaml", ".github/scripts/kv-comparison*",
                   "docs/distributed/KV_COMPARISON_PROTOCOL.md"], cwd=repo).split("\0")
    result = []
    for name in sorted(set(tracked + new) - {""}):
        if not allowed(name):
            continue
        path = repo / name
        if not path.exists() or path.is_symlink() or any(p.is_symlink() for p in path.parents):
            continue  # Deleted tracked files and symlinks are not source bytes.
        regular(path)
        if path.stat().st_size > MAX_FILE:
            raise ValueError(f"source file exceeds {MAX_FILE} bytes: {name}")
        result.append(name)
    if len(result) > MAX_FILES or sum((repo / p).stat().st_size for p in result) > MAX_SOURCE:
        raise ValueError("source snapshot exceeds bounded inventory")
    missing = REQUIRED - set(result)
    if missing:
        raise ValueError(f"required source closure missing: {sorted(missing)}")
    return result


def inventory(repo):
    return {name: identity(repo / name)["sha256"] for name in source_paths(repo)}


def tree_sha(hashes):
    return hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def private_bytes(block):
    return bool(re.search(rb"-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----|(?:AKIA|ASIA)[A-Z0-9]{16}", block))


def snapshot(repo, output):
    repo = repo.resolve(strict=True)
    output.mkdir(parents=True, exist_ok=False)
    hashes = {}
    with zipfile.ZipFile(output / "source.zip", "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in source_paths(repo):
            path = repo / name
            # Read each bounded source file once; inventory names the bytes actually archived.
            data = path.read_bytes()
            if private_bytes(data):
                raise ValueError(f"possible private credential in source: {name}")
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (stat.S_IFREG | (0o755 if path.stat().st_mode & 0o111 else 0o644)) << 16
            archive.writestr(info, data)
            hashes[name] = hashlib.sha256(data).hexdigest()
    if inventory(repo) != hashes:
        raise ValueError("source changed while freezing; use a new output directory")
    manifest = {"schema_version": 1, "revision": command(["git", "rev-parse", "HEAD"], cwd=repo),
                "branch": command(["git", "branch", "--show-current"], cwd=repo),
                "dirty": bool(command(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=repo)),
                "sha256": hashes, "tree_sha256": tree_sha(hashes), "snapshot_sha256": identity(output / "source.zip")["sha256"]}
    dump(output / "source.json", manifest)
    return manifest


def admit_source(directory, extracted=None):
    regular(directory / "source.json")
    metadata = json.loads((directory / "source.json").read_text())
    hashes = metadata["sha256"]
    if metadata["schema_version"] != 1 or not hashes or len(hashes) > MAX_FILES or REQUIRED - hashes.keys():
        raise ValueError("source inventory schema/closure mismatch")
    if tree_sha(hashes) != metadata["tree_sha256"] or identity(directory / "source.zip")["sha256"] != metadata["snapshot_sha256"]:
        raise ValueError("source archive/inventory digest mismatch")
    with zipfile.ZipFile(directory / "source.zip") as archive:
        entries = archive.infolist()
        if len(entries) != len(hashes) or {x.filename for x in entries} != set(hashes):
            raise ValueError("source ZIP inventory/duplicate mismatch")
        if sum(x.file_size for x in entries) > MAX_SOURCE:
            raise ValueError("source ZIP exceeds expanded bound")
        for item in entries:
            safe_name(item.filename)
            if (not allowed(item.filename) or item.file_size > MAX_FILE
                    or not stat.S_ISREG(item.external_attr >> 16)):
                raise ValueError("source ZIP contains non-source, oversized or non-regular entry")
            with archive.open(item) as stream:
                data = stream.read(MAX_FILE + 1)
            if len(data) != item.file_size or hashlib.sha256(data).hexdigest() != hashes[item.filename] or private_bytes(data):
                raise ValueError(f"source ZIP byte admission failed: {item.filename}")
            if extracted is not None:
                path = extracted / item.filename
                if identity(path)["sha256"] != hashes[item.filename]:
                    raise ValueError(f"extracted source differs: {item.filename}")
    if extracted is not None:
        actual = {p.relative_to(extracted).as_posix() for p in extracted.rglob("*") if p.is_symlink() or not p.is_dir()}
        if actual != set(hashes):
            raise ValueError("extracted source contains extra or missing files")
    return metadata


def extract_source(directory, destination):
    admit_source(directory)
    destination.mkdir(parents=True, exist_ok=False)
    with zipfile.ZipFile(directory / "source.zip") as archive:
        for item in archive.infolist():
            path = destination / item.filename
            path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(item) as source, path.open("xb") as target:
                shutil.copyfileobj(source, target)
            path.chmod((item.external_attr >> 16) & 0o777)
    return admit_source(directory, destination)


def version_tuple(text):
    return tuple(int(x) for x in text.split("."))


def elf_identity(path):
    regular(path)
    header = command(["readelf", "-hW", path])
    if "Advanced Micro Devices X86-64" not in header or "ELF64" not in header:
        raise ValueError("runtime requires ELF64 x86_64")
    dynamic = command(["readelf", "-dW", path])
    versions = command(["readelf", "--version-info", "-W", path])
    needs = versions.split("Version needs section", 1)[-1] if "Version needs section" in versions else ""
    requirements = {}
    for family, maximum in (("GLIBC", MAX_GLIBC), ("GLIBCXX", MAX_GLIBCXX), ("CXXABI", (1, 3, 15)), ("OPENSSL", (3, 0, 0))):
        found = sorted(set(re.findall(r"\b" + family + r"_(\d+(?:\.\d+)+)", needs)), key=version_tuple)
        if found and version_tuple(found[-1]) > maximum:
            raise ValueError(f"{path.name} requires {family}_{found[-1]}, newer than Ubuntu24; use the container build")
        requirements[family] = found
    rpaths = re.findall(r"\((?:RPATH|RUNPATH)\).*\[(.*?)\]", dynamic)
    if any(x not in {"$ORIGIN/../lib", "$ORIGIN"} for group in rpaths for x in group.split(":")):
        raise ValueError("absolute/unadmitted ELF rpath is not portable")
    needed = re.findall(r"\(NEEDED\).*\[(.*?)\]", dynamic)
    supported = {"libc.so.6", "libm.so.6", "libpthread.so.0", "librt.so.1", "libdl.so.2", "ld-linux-x86-64.so.2",
                 "libgcc_s.so.1", "libstdc++.so.6", "liblzma.so.5", "libz.so.1", "libatomic.so.1",
                 "libssl.so.3", "libcrypto.so.3", "libfabric.so.1", "libfdb_c.so", "libnuma.so.1",
                 "libibverbs.so.1", "libefa.so.1", "libnl-3.so.200", "libnl-route-3.so.200", "libuuid.so.1"}
    if set(needed) - supported:
        raise ValueError(f"unadmitted Ubuntu24 runtime SONAMEs: {sorted(set(needed) - supported)}")
    return {"identity": identity(path), "needed": needed, "version_requirements": requirements, "rpaths": rpaths}


def clean_environment():
    environment = dict(os.environ)
    for key in list(environment):
        if (key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "LD_PRELOAD", "LD_AUDIT", "CFLAGS", "CXXFLAGS",
                    "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTC_BOOTSTRAP"}
                or key.endswith(("_RUSTFLAGS", "_CFLAGS", "_CXXFLAGS"))
                or key.startswith(("BINDGEN_EXTRA_CLANG_ARGS", "CFLAGS_", "CXXFLAGS_"))):
            environment.pop(key)
    environment.update({"CARGO_BUILD_JOBS": "2", "CARGO_INCREMENTAL": "0", "CFLAGS": "-march=x86-64 -mtune=generic",
                        "CXXFLAGS": "-march=x86-64 -mtune=generic", "PYTHONDONTWRITEBYTECODE": "1"})
    return environment
# FoundationDB's version getter is unsafe before selecting an API version.
# Keep that process-global initialization out of the controller/build process:
# each admission gets a fresh interpreter, no network setup, and a hard deadline.
_FDB_VERSION_PROBE = """
import ctypes
import json
import sys

client = ctypes.CDLL(sys.argv[1])
select_api = client.fdb_select_api_version_impl
select_api.argtypes = [ctypes.c_int, ctypes.c_int]
select_api.restype = ctypes.c_int
status = select_api(730, 730)
if status != 0:
    raise RuntimeError(f"FoundationDB API730 selection returned {status}")
get_version = client.fdb_get_client_version
get_version.argtypes = []
get_version.restype = ctypes.c_char_p
raw_version = get_version()
if raw_version is None:
    raise RuntimeError("FoundationDB native client returned a null version")
version = raw_version.decode("utf-8")
if not version.startswith("7.3.77,"):
    raise RuntimeError(f"FoundationDB native client reports unexpected version: {version!r}")
print(json.dumps({"api": 730, "client_version": version}))
"""


def fdb_client_version(library):
    """Admit the actual native API in an isolated, bounded, faulthandler child."""
    regular(library)
    library = library.resolve(strict=True)
    environment = clean_environment()
    environment["LD_LIBRARY_PATH"] = str(library.parent)
    try:
        result = subprocess.run(
            [sys.executable, "-I", "-B", "-X", "faulthandler", "-c", _FDB_VERSION_PROBE, str(library)],
            cwd="/", env=environment, stdin=subprocess.DEVNULL, capture_output=True,
            text=True, timeout=10, check=False)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"FoundationDB native version probe exceeded 10 seconds; stderr={error.stderr!r}") from error
    if result.returncode != 0:
        raise RuntimeError(f"FoundationDB native version probe exited {result.returncode}; stderr={result.stderr[-4096:]}")
    try:
        observed = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError("FoundationDB native version probe returned invalid JSON") from error
    if (not isinstance(observed, dict) or observed.get("api") != 730
            or not isinstance(observed.get("client_version"), str)
            or not observed["client_version"].startswith("7.3.77,")):
        raise ValueError("FoundationDB native version probe returned invalid API/version")
    return observed["client_version"]




def fdb_identity(prefix):
    metadata = json.loads((prefix / "client.json").read_text())
    library = prefix / "lib/libfdb_c.so"
    observed = identity(library)
    if (metadata.get("version") != "7.3.77" or metadata.get("api") != 730 or metadata.get("image") != FDB_IMAGE
            or metadata.get("sha256") != FDB_SHA256 or observed["sha256"] != FDB_SHA256
            or observed["blake2b512"] != FDB_BLAKE2B
            or metadata.get("blake2b512", FDB_BLAKE2B) != FDB_BLAKE2B):
        raise ValueError("FoundationDB native client differs from pinned image/version/API/hashes")
    return {"version": "7.3.77", "api": 730, "image": FDB_IMAGE, **observed,
            "elf": elf_identity(library), "client_version": fdb_client_version(library)}


def linked_libraries(path, environment):
    text = command(["ldd", path], env=environment)
    if "not found" in text:
        raise ValueError(f"unresolved runtime library: {text}")
    paths = sorted(set(re.findall(r"(?:=>\s+|^\s*)(/[^\s]+)", text, re.MULTILINE)))
    return {"ldd": text, "resolved": {str(Path(p).resolve(strict=True)): elf_identity(Path(p).resolve(strict=True)) for p in paths}}


def build(snapshot_dir, output, fdb_prefix, fabric_prefix, timeout=5400):
    if platform.machine() != "x86_64" or version_tuple(command(["getconf", "GNU_LIBC_VERSION"]).split()[-1]) > MAX_GLIBC:
        raise ValueError("build host must be x86_64 and glibc<=2.39; use Dockerfile.cloud-build")
    if not 1 <= timeout <= 7200:
        raise ValueError("build timeout must be 1..7200 seconds")
    snapshot_dir, fdb_prefix, fabric_prefix = (p.resolve(strict=True) for p in (snapshot_dir, fdb_prefix, fabric_prefix))
    output = output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    source = output / "source"
    metadata = extract_source(snapshot_dir, source)
    fdb = fdb_identity(fdb_prefix)
    environment = clean_environment()
    environment.update({"PKG_CONFIG_PATH": str(fabric_prefix / "lib/pkgconfig"),
                        "FDB_CLIENT_LIB_DIR": str(fdb_prefix / "lib"), "FDB_CLIENT_LIB_PATH": str(fdb_prefix / "lib"),
                        "LIBRARY_PATH": str(fdb_prefix / "lib"), "LD_LIBRARY_PATH": f"{fdb_prefix}/lib:{fabric_prefix}/lib",
                        "CARGO_TARGET_DIR": str(output / "target"), "SOURCE_DATE_EPOCH": "0"})
    fabric_version = command(["pkg-config", "--modversion", "libfabric"], env=environment)
    if fabric_version not in {"2.6.0", EFA["libfabric_version"]}:
        raise ValueError("build libfabric must match pinned 2.6.0 ABI, not a floating system provider")
    libdir = Path(command(["pkg-config", "--variable=libdir", "libfabric"], env=environment))
    if libdir.resolve() != (fabric_prefix / "lib").resolve():
        raise ValueError("pkg-config selected a different libfabric prefix")
    fabric_library = (libdir / "libfabric.so.1").resolve(strict=True)
    fabric = {"version": fabric_version, "path": str(fabric_library), **elf_identity(fabric_library)}
    # Fixed generic CPU, frozen lockfile/cache, explicit target; never inherit target-cpu=native.
    flags = ["-C", "target-cpu=x86-64", f"--remap-path-prefix={source}=/tidehunter-src"]
    argv = ["cargo", "--config", "build.rustflags=" + json.dumps(flags),
            "--config", f"target.{TARGET}.rustflags=" + json.dumps(flags), "build", "--release", "--frozen",
            "--target", TARGET, "-p", "kv-research", "--no-default-features", "--features", ",".join(FEATURES)]
    rustc = command(["rustc", "-vV"], cwd=source, env=environment)
    if not rustc.startswith("rustc 1.90.0 "):
        raise ValueError("the source-pinned Rust1.90.0 toolchain is required")
    if identity(Path(__file__).resolve())["sha256"] != metadata["sha256"]["experiments/kv-comparison/cloud_package.py"]:
        raise ValueError("executing packager differs from frozen source")
    with (output / "build.log").open("xb") as log:
        subprocess.run(argv, cwd=source, env=environment, stdout=log, stderr=subprocess.STDOUT,
                       timeout=timeout, check=True)
    admit_source(snapshot_dir, source)
    if fdb_identity(fdb_prefix) != fdb or elf_identity(fabric_library) != {k: v for k, v in fabric.items() if k not in {"version", "path"}}:
        raise ValueError("native build dependency changed during compilation")
    shutil.copyfile(output / "target" / TARGET / "release/kv-research", output / "kv-research")
    (output / "kv-research").chmod(0o755)
    binary = elf_identity(output / "kv-research")
    linkage = linked_libraries(output / "kv-research", environment)
    resolved = linkage["resolved"]
    if str((fdb_prefix / "lib/libfdb_c.so").resolve()) not in resolved or str(fabric_library) not in resolved:
        raise ValueError("executable did not link the admitted FDB and libfabric libraries")
    receipt = {"schema_version": 1, "source_sha256": metadata["snapshot_sha256"], "source_tree_sha256": metadata["tree_sha256"],
               "features": FEATURES, "target": TARGET, "cpu": "x86-64", "build_command": argv, "rustc": rustc,
               "cargo": command(["cargo", "--version"], cwd=source, env=environment), "glibc": command(["getconf", "GNU_LIBC_VERSION"]),
               "binary": binary, "fdb": fdb, "fabric_build": fabric, "linkage": linkage,
               "build_log": identity(output / "build.log"), "runtime_efa": EFA,
               "builder_image": os.environ.get("CLOUD_PACKAGE_BUILDER_IMAGE"),
               "builder_recipe_sha256": metadata["sha256"].get("experiments/kv-comparison/Dockerfile.cloud-build")}
    dump(output / "build.json", receipt)
    return receipt


def admit_build(snapshot_dir, build_dir, fdb_prefix):
    source = admit_source(snapshot_dir)
    receipt = json.loads((build_dir / "build.json").read_text())
    if (receipt.get("schema_version") != 1 or receipt.get("source_sha256") != source["snapshot_sha256"]
            or receipt.get("source_tree_sha256") != source["tree_sha256"] or receipt.get("features") != FEATURES
            or receipt.get("target") != TARGET or receipt.get("cpu") != "x86-64" or receipt.get("runtime_efa") != EFA
            or receipt.get("binary") != elf_identity(build_dir / "kv-research") or receipt.get("fdb") != fdb_identity(fdb_prefix)
            or receipt.get("build_log") != identity(build_dir / "build.log")):
        raise ValueError("source/build/native-client binding differs")
    admit_source(snapshot_dir, build_dir / "source")
    return source, receipt


def deterministic_tar(output, paths):
    with output.open("xb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|") as archive:
            total = 0
            for name, path in sorted(paths.items()):
                safe_name(name)
                regular(path)
                total += path.stat().st_size
                if total > MAX_BUNDLE:
                    raise ValueError("runtime bundle exceeds expanded bound")
                info = tarfile.TarInfo(name)
                info.size = path.stat().st_size
                info.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                info.mtime = info.uid = info.gid = 0
                with path.open("rb") as stream:
                    archive.addfile(info, stream)


def stage(repo, snapshot_dir, build_dir, fdb_prefix, output):
    source, receipt = admit_build(snapshot_dir, build_dir, fdb_prefix)
    if inventory(repo.resolve(strict=True)) != source["sha256"]:
        raise ValueError("checkout changed after source freeze; snapshot and rebuild latest sources")
    output.mkdir(parents=True, exist_ok=False)
    extract_source(snapshot_dir, output / "source")
    for name in ("source.zip", "source.json"):
        shutil.copyfile(snapshot_dir / name, output / name)
    for name in ("build.json", "build.log"):
        shutil.copyfile(build_dir / name, output / name)
    (output / "runtime/bin").mkdir(parents=True)
    (output / "runtime/lib").mkdir()
    shutil.copyfile(build_dir / "kv-research", output / "runtime/bin/kv-research")
    (output / "runtime/bin/kv-research").chmod(0o755)
    shutil.copyfile(fdb_prefix / "lib/libfdb_c.so", output / "runtime/lib/libfdb_c.so")
    dump(output / "runtime/fdb-client.json", receipt["fdb"])
    files = {p.relative_to(output).as_posix(): p for p in output.rglob("*") if p.is_file()}
    manifest = {"schema_version": 1, "revision": source["revision"], "branch": source["branch"], "dirty": source["dirty"],
                "source_sha256": source["snapshot_sha256"], "source_tree_sha256": source["tree_sha256"],
                "binary_sha256": receipt["binary"]["identity"]["sha256"],
                "binary_blake2b512": receipt["binary"]["identity"]["blake2b512"], "features": FEATURES,
                "paths": {"repository_root": "source", "binary": "runtime/bin/kv-research", "fdb_library_dir": "runtime/lib"},
                "runtime_efa": EFA, "files": {name: identity(path) for name, path in sorted(files.items())}}
    dump(output / "manifest.json", manifest)
    files["manifest.json"] = output / "manifest.json"
    deterministic_tar(output / "bundle.tar.gz", files)
    # Admit actual tar bytes, not just the directory used to assemble them.
    verify_archive(output / "bundle.tar.gz", identity(output / "bundle.tar.gz")["sha256"])
    if inventory(repo.resolve(strict=True)) != source["sha256"]:
        raise ValueError("checkout changed during staging; no deployable bundle receipt written")
    dump(output / "bundle.json", {"schema_version": 1, "archive": identity(output / "bundle.tar.gz"),
                                  "manifest": identity(output / "manifest.json")})
    return manifest


def verify_directory(directory, runtime=False):
    manifest = json.loads((directory / "manifest.json").read_text())
    files = manifest["files"]
    if (manifest.get("schema_version") != 1 or manifest.get("features") != FEATURES
            or manifest.get("runtime_efa") != EFA or len(files) > MAX_FILES):
        raise ValueError("runtime manifest schema/features/provider mismatch")
    if manifest.get("paths") != {"repository_root": "source", "binary": "runtime/bin/kv-research", "fdb_library_dir": "runtime/lib"}:
        raise ValueError("runtime manifest paths differ from package interface")
    required_files = {"source.zip", "source.json", "build.json", "build.log", "runtime/bin/kv-research",
                      "runtime/lib/libfdb_c.so", "runtime/fdb-client.json"}
    if required_files - files.keys():
        raise ValueError("runtime manifest omits mandatory artifact identities")
    if sum(x["bytes"] for x in files.values()) > MAX_BUNDLE:
        raise ValueError("runtime inventory exceeds expanded bound")
    for name, expected in files.items():
        safe_name(name)
        if identity(directory / name) != expected:
            raise ValueError(f"runtime file differs: {name}")
    actual = {p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_symlink() or not p.is_dir()}
    if actual - {"manifest.json", "bundle.tar.gz", "bundle.json"} != set(files):
        raise ValueError("runtime directory contains extra or missing files")
    source = admit_source(directory, directory / "source")
    if any(manifest.get(key) != source[key] for key in ("revision", "branch", "dirty")):
        raise ValueError("runtime source revision/branch/dirty metadata differs")
    build_receipt = json.loads((directory / "build.json").read_text())
    binary = directory / "runtime/bin/kv-research"
    library = directory / "runtime/lib/libfdb_c.so"
    native = json.loads((directory / "runtime/fdb-client.json").read_text())
    if (manifest["source_sha256"] != source["snapshot_sha256"] or manifest["source_tree_sha256"] != source["tree_sha256"]
            or build_receipt["source_sha256"] != manifest["source_sha256"] or build_receipt["features"] != FEATURES
            or build_receipt["source_tree_sha256"] != manifest["source_tree_sha256"]
            or build_receipt.get("target") != TARGET or build_receipt.get("cpu") != "x86-64"
            or build_receipt.get("runtime_efa") != EFA or build_receipt.get("schema_version") != 1
            or build_receipt["binary"]["identity"] != identity(binary) or native != build_receipt["fdb"]
            or manifest["binary_sha256"] != identity(binary)["sha256"]
            or manifest["binary_blake2b512"] != identity(binary)["blake2b512"]
            or identity(library)["sha256"] != FDB_SHA256 or identity(library)["blake2b512"] != FDB_BLAKE2B
            or native["sha256"] != FDB_SHA256 or native["blake2b512"] != FDB_BLAKE2B):
        raise ValueError("manifest source/binary/native-client binding mismatch")
    if not binary.stat().st_mode & 0o111:
        raise ValueError("comparison binary is not executable")
    elf_identity(binary)
    elf_identity(library)
    result = {"schema_version": 1, "manifest_sha256": identity(directory / "manifest.json")["sha256"],
              "source_sha256": manifest["source_sha256"], "binary_sha256": manifest["binary_sha256"],
              "binary_blake2b512": manifest["binary_blake2b512"], "runtime_checked": runtime}
    if runtime:
        environment = clean_environment()
        environment["LD_LIBRARY_PATH"] = f"{library.parent.resolve()}:{EFA['runtime_library_dir']}"
        result["linkage"] = linked_libraries(binary, environment)
        if str(library.resolve()) not in result["linkage"]["resolved"]:
            raise ValueError("runtime linker selected a different FDB client")
        fabric = Path(EFA["runtime_library_dir"]) / "libfabric.so.1"
        if str(fabric.resolve(strict=True)) not in result["linkage"]["resolved"]:
            raise ValueError("runtime linker selected a different EFA provider library")
        result["fabric_version"] = command(["/opt/amazon/efa/bin/fi_info", "--version"], env=environment)
        if EFA["libfabric_version"] not in result["fabric_version"]:
            raise ValueError("runtime libfabric differs from pinned AWS installer")
        # Selecting API730 before the getter is mandatory; never initialize FDB
        # process-global state in the long-lived controller itself.
        result["fdb_client_version"] = fdb_client_version(library)
        if result["fdb_client_version"] != native.get("client_version"):
            raise ValueError("loaded FoundationDB client version differs from local build admission")
    return result


def verify_archive(archive_path, expected_sha256, destination=None, runtime=False):
    if identity(archive_path)["sha256"] != expected_sha256:
        raise ValueError("outer archive checksum differs from caller-admitted digest")
    with tempfile.TemporaryDirectory(prefix="kv-package-") as temporary:
        target = Path(temporary) / "bundle" if destination is None else destination
        target.mkdir(parents=True, exist_ok=False)
        with tarfile.open(archive_path, "r:gz") as archive:
            names, size = set(), 0
            for member in archive:
                safe_name(member.name)
                size += member.size
                if not member.isfile() or member.name in names or size > MAX_BUNDLE or len(names) >= MAX_FILES:
                    raise ValueError("unsafe/duplicate/oversized runtime archive member")
                names.add(member.name)
                path = target / member.name
                path.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, path.open("xb") as output:
                    shutil.copyfileobj(source, output)
                path.chmod(0o755 if member.mode & 0o111 else 0o644)
        return verify_directory(target, runtime)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    freeze = sub.add_parser("snapshot")
    freeze.add_argument("--repo", type=Path, default=ROOT)
    freeze.add_argument("--output", type=Path, required=True)
    compile_parser = sub.add_parser("build")
    compile_parser.add_argument("--snapshot", type=Path, required=True)
    compile_parser.add_argument("--output", type=Path, required=True)
    compile_parser.add_argument("--fdb-prefix", type=Path, required=True)
    compile_parser.add_argument("--fabric-prefix", type=Path, required=True)
    compile_parser.add_argument("--timeout", type=int, default=5400)
    pack = sub.add_parser("stage")
    pack.add_argument("--repo", type=Path, default=ROOT)
    pack.add_argument("--snapshot", type=Path, required=True)
    pack.add_argument("--build", type=Path, required=True)
    pack.add_argument("--fdb-prefix", type=Path, required=True)
    pack.add_argument("--output", type=Path, required=True)
    verify = sub.add_parser("verify")
    choice = verify.add_mutually_exclusive_group(required=True)
    choice.add_argument("--directory", type=Path)
    choice.add_argument("--archive", type=Path)
    verify.add_argument("--sha256")
    verify.add_argument("--extract-to", type=Path)
    verify.add_argument("--runtime", action="store_true")
    args = parser.parse_args()
    if args.action == "snapshot":
        result = snapshot(args.repo, args.output)
        result = {k: v for k, v in result.items() if k != "sha256"}
    elif args.action == "build":
        result = build(args.snapshot, args.output, args.fdb_prefix, args.fabric_prefix, args.timeout)
    elif args.action == "stage":
        result = stage(args.repo, args.snapshot, args.build, args.fdb_prefix, args.output)
        result = {k: v for k, v in result.items() if k != "files"}
    else:
        if args.archive:
            if not args.sha256:
                parser.error("--archive requires externally admitted --sha256")
            result = verify_archive(args.archive, args.sha256, args.extract_to, args.runtime)
        else:
            if args.extract_to or args.sha256:
                parser.error("--extract-to/--sha256 require --archive")
            result = verify_directory(args.directory, args.runtime)
    print(encode(result).decode(), end="")


if __name__ == "__main__":
    main()
