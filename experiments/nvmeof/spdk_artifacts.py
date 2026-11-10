#!/usr/bin/env python3
"""Seal a locally built, pinned SPDK target; never package a cloud RDMA provider.

The source-bound binary supports separate sealed-read and live-WAL policies.
The guest uses its own libibverbs/librdmacm/provider stack for Falcon.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import shutil
import stat
import subprocess
# GitHub's abbreviated UI display is not the source identity.
COMMIT = "2ef883ef96e79c3cc16da02f667a7a58c2453f2f"
PATCH_SHA256 = "ba21692d7ac70190a505a7b94dc749c8f544d6eef8632a38980f4acf5e987373"
SUBMODULES = {
    "dpdk": "e01bfcd05fe39f628392c8b29b880f5692de2224",
    "isa-l": "6f420b14a1e3e091bc9d15f508a54f82c007483c",
    "isa-l-crypto": "07d0b253843ace996aa5a44f538c9b83b085aba6",
}
PRIVATE_LIBRARIES = {"libaio.so.1", "libuuid.so.1", "libnuma.so.1", "libssl.so.3", "libcrypto.so.3"}
CONFIGURE = ["--with-rdma=verbs", "--target-arch=nehalem", "--disable-tests",
             "--disable-unit-tests", "--disable-examples", "--without-vhost",
             "--without-virtio", "--without-vfio-user", "--without-fsdev",
             "--without-nvme-cuse", "--without-uring"]
MAX_TOTAL = 128 << 20


def command(argv):
    return subprocess.check_output([str(x) for x in argv], text=True,
                                   stderr=subprocess.STDOUT, timeout=30).strip()


def identity(path):
    path = Path(path)
    if not stat.S_ISREG(path.lstat().st_mode) or any(p.is_symlink() for p in path.parents):
        raise ValueError(f"SPDK artifact is not an ordinary file: {path}")
    if not 0 < path.stat().st_size <= MAX_TOTAL:
        raise ValueError(f"SPDK artifact size outside bounds: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return {"bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def elf(path):
    with Path(path).open("rb") as stream:
        header = stream.read(20)
    if (len(header) != 20 or header[:6] != b"\x7fELF\x02\x01"
            or int.from_bytes(header[18:20], "little") != 62):
        raise ValueError(f"SPDK requires little-endian ELF64 x86_64: {path}")


def admit_prefix(prefix):
    """Validate relocatable identities, provenance and an exact file inventory."""
    prefix = Path(prefix)
    metadata_path = prefix / "spdk.json"
    if identity(metadata_path)["bytes"] > 1 << 20:
        raise ValueError("SPDK manifest exceeds 1 MiB")
    metadata = json.loads(metadata_path.read_text())
    if (metadata.get("schema_version") != 1 or metadata.get("source_commit") != COMMIT
            or metadata.get("submodules") != SUBMODULES
            or metadata.get("configure_options") != CONFIGURE
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", metadata.get("builder_image", ""))
            or metadata.get("target_scope") not in ("sealed_read_only_file_aio_direct",
                                                     "file_aio_direct_live_and_sealed")):
        raise ValueError("unadmitted SPDK source, build or target scope")
    if (metadata["target_scope"] == "file_aio_direct_live_and_sealed"
            and metadata.get("source_patch_sha256") != PATCH_SHA256):
        raise ValueError("live SPDK artifact lacks the exact reviewed namespace-drain patch")
    files = metadata.get("files")
    if not isinstance(files, dict) or not 2 <= len(files) <= 16:
        raise ValueError("SPDK requires a bounded file identity map")
    if (metadata["target_scope"] == "file_aio_direct_live_and_sealed"
            and not {"build-proof.json", "build.log"} <= files.keys()):
        raise ValueError("live SPDK requires retained build provenance and log")
    allowed = {"bin/nvmf_tgt", "LICENSE.spdk", "build-proof.json", "build.log"} | {"lib/" + n for n in PRIVATE_LIBRARIES}
    allowed |= {"LICENSE." + name for name in SUBMODULES}
    if not {"bin/nvmf_tgt", "LICENSE.spdk"} <= files.keys() or not files.keys() <= allowed:
        raise ValueError("SPDK prefix contains an unadmitted artifact or provider library")
    observed = set()
    for path in prefix.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"SPDK prefix contains a symlink: {path}")
        if path.is_dir():
            continue
        observed.add(path.relative_to(prefix).as_posix())
    if observed != set(files) | {"spdk.json"}:
        raise ValueError("SPDK prefix inventory differs from its manifest")
    if sum(identity(prefix / name)["bytes"] for name in files) > MAX_TOTAL:
        raise ValueError("SPDK prefix exceeds 128 MiB")
    for name, expected in files.items():
        if identity(prefix / name) != expected:
            raise ValueError(f"SPDK artifact changed: {name}")
        if name.startswith(("bin/", "lib/")):
            elf(prefix / name)
    if metadata["target_scope"] == "file_aio_direct_live_and_sealed":
        proof = json.loads((prefix / "build-proof.json").read_text())
        binary = files["bin/nvmf_tgt"]
        if (proof.get("complete") is not True or proof.get("builder_image") != metadata["builder_image"]
                or proof.get("upstream_commit") != COMMIT or proof.get("source_patch_sha256") != PATCH_SHA256
                or proof.get("binary_sha256") != binary["sha256"] or proof.get("binary_bytes") != binary["bytes"]
                or proof.get("build_log_sha256") != files["build.log"]["sha256"]
                or proof.get("version") != metadata.get("version")
                or proof.get("commands") != [["make", "clean"], ["make", "-j4"]]
                or not re.fullmatch(r"[0-9a-f]{64}", proof.get("config_sha256", ""))
                or not re.fullmatch(r"[0-9a-f]{64}", proof.get("rpc_source_sha256", ""))):
            raise ValueError("live SPDK retained build evidence differs from binary or source")
    if not (prefix / "bin/nvmf_tgt").stat().st_mode & 0o111:
        raise ValueError("SPDK target is not executable")
    return metadata


def seal(source, prefix, builder_image, libraries, build_proof, build_log):
    """Seal the existing build, without installing drivers or building on EC2/GCP."""
    source = Path(source).resolve(strict=True)
    prefix = Path(prefix).absolute()
    if prefix.exists():
        raise ValueError("SPDK output must be new")
    if command(["git", "-C", source, "rev-parse", "HEAD"]) != COMMIT:
        raise ValueError("SPDK checkout is not the pinned v26.01 commit")
    diff = subprocess.check_output(["git", "-C", str(source), "diff", "--binary", "--no-ext-diff", "HEAD"],
                                   timeout=30)
    if hashlib.sha256(diff).hexdigest() != PATCH_SHA256:
        raise ValueError("SPDK tracked source differs from the exact reviewed namespace-drain patch")
    for name, commit in SUBMODULES.items():
        if (command(["git", "-C", source / name, "rev-parse", "HEAD"]) != commit
                or command(["git", "-C", source / name, "status", "--porcelain", "--untracked-files=no"])):
            raise ValueError(f"SPDK submodule is not the clean pinned source: {name}")
    config_text = (source / "mk/config.mk").read_text()
    configured = re.search(r"^# configure options: (.*)$", config_text, re.MULTILINE)
    options = shlex.split(configured.group(1)) if configured else None
    if options != CONFIGURE:
        raise ValueError("SPDK actual configure arguments differ from the admitted build")
    config = dict(re.findall(r"^(CONFIG_[A-Z0-9_]+)\?=(.*)$", config_text, re.MULTILINE))
    if any(config.get(k) != v for k, v in {
            "CONFIG_ARCH": "nehalem", "CONFIG_RDMA": "y", "CONFIG_RDMA_PROV": "verbs",
            "CONFIG_URING": "n", "CONFIG_FSDEV": "n", "CONFIG_DEBUG": "n",
            "CONFIG_VHOST": "n", "CONFIG_VIRTIO": "n", "CONFIG_VFIO_USER": "n",
            "CONFIG_NVME_CUSE": "n", "CONFIG_TESTS": "n", "CONFIG_UNIT_TESTS": "n",
            "CONFIG_EXAMPLES": "n"}.items()):
        raise ValueError("SPDK configured features do not match the admitted build")
    binary = source / "build/bin/nvmf_tgt"
    version = command([binary, "--version"])
    if "SPDK v26.01 git sha1 2ef883e" not in version:
        raise ValueError("SPDK binary version does not match the admitted source")
    build_proof, build_log = Path(build_proof), Path(build_log)
    proof = json.loads(build_proof.read_text())
    binary_identity = identity(binary)
    observed = {"builder_image": builder_image, "upstream_commit": COMMIT,
                "source_patch_sha256": PATCH_SHA256, "complete": True,
                "rpc_source_sha256": identity(source / "lib/nvmf/nvmf_rpc.c")["sha256"],
                "config_sha256": identity(source / "mk/config.mk")["sha256"],
                "binary_sha256": binary_identity["sha256"], "binary_bytes": binary_identity["bytes"],
                "build_log_sha256": identity(build_log)["sha256"], "version": version,
                "commands": [["make", "clean"], ["make", "-j4"]]}
    if any(proof.get(key) != value for key, value in observed.items()):
        raise ValueError("SPDK clean build provenance does not bind the admitted source and binary")
    copies = {"bin/nvmf_tgt": binary, "LICENSE.spdk": source / "LICENSE",
              "build-proof.json": build_proof, "build.log": build_log}
    for entry in libraries:
        original = Path(entry)
        if original.name not in PRIVATE_LIBRARIES or "lib/" + original.name in copies:
            raise ValueError(f"unadmitted or duplicate SPDK private library: {entry}")
        copies["lib/" + original.name] = original.resolve(strict=True)
    for name in SUBMODULES:
        candidates = [source / name / "LICENSE", source / name / "license/bsd-3-clause.txt"]
        found = [p for p in candidates if p.is_file()]
        if not found:
            raise ValueError(f"missing bundled submodule license: {name}")
        copies["LICENSE." + name] = found[0]
    prefix.mkdir(parents=True)
    for name, original in copies.items():
        identity(original)
        target = prefix / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
        target.chmod(0o755 if name.startswith("bin/") else 0o644)
    metadata = {"schema_version": 1, "source_commit": COMMIT, "submodules": SUBMODULES,
                "builder_image": builder_image, "configure_options": options,
                "target_scope": "file_aio_direct_live_and_sealed", "source_patch_sha256": PATCH_SHA256,
                "version": version,
                "files": {name: identity(prefix / name) for name in sorted(copies)}}
    (prefix / "spdk.json").write_text(json.dumps(metadata, sort_keys=True, indent=2) + "\n")
    return admit_prefix(prefix)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    build = commands.add_parser("seal")
    build.add_argument("--source", type=Path, required=True)
    build.add_argument("--prefix", type=Path, required=True)
    build.add_argument("--builder-image", required=True)
    build.add_argument("--library", action="append", default=[])
    build.add_argument("--build-proof", type=Path, required=True)
    build.add_argument("--build-log", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("--prefix", type=Path, required=True)
    args = parser.parse_args()
    result = (seal(args.source, args.prefix, args.builder_image, args.library, args.build_proof, args.build_log)
              if args.action == "seal" else admit_prefix(args.prefix))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
