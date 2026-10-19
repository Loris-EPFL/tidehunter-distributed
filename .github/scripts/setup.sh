#!/usr/bin/env bash
# Prepare correctness dependencies only. Never install an AWS/EFA driver.
set -euo pipefail

usage() {
    cat <<'HELP'
Usage: bash .github/scripts/setup.sh core|fabric
  core    Ubuntu build dependencies, project Rust + rustfmt/clippy, locked Cargo fetch.
  fabric  core plus pinned libfabric 2.6.0 tcp/rxm in a user-owned prefix.

TIDEHUNTER_LIBFABRIC_PREFIX overrides the install location (default:
${XDG_CACHE_HOME:-$HOME/.cache}/tidehunter/libfabric-2.6.0).
An existing compatible prefix is reused without replacing its providers.
TIDEHUNTER_SETUP_JOBS sets build concurrency (default: 2, maximum: 32).
TIDEHUNTER_SETUP_SKIP_APT=1 skips apt when dependencies are already installed.
Run checks.py separately; setup does not run tests or claim hardware coverage.
HELP
}

if [[ $# != 1 ]]; then usage >&2; exit 2; fi
case "$1" in
    -h|--help) usage; exit 0 ;;
    core|fabric) mode=$1 ;;
    *) usage >&2; exit 2 ;;
esac
root=$(realpath "$(dirname "${BASH_SOURCE[0]}")/../..")
cache=${XDG_CACHE_HOME:-$HOME/.cache}/tidehunter
jobs=${TIDEHUNTER_SETUP_JOBS:-2}
if [[ ! $jobs =~ ^([1-9]|[12][0-9]|3[0-2])$ ]]; then
    echo 'TIDEHUNTER_SETUP_JOBS must be an integer from 1 to 32' >&2
    exit 2
fi

if [[ ${TIDEHUNTER_SETUP_SKIP_APT:-0} != 1 ]]; then
    if ! command -v apt-get >/dev/null; then
        echo 'Install the listed Ubuntu dependencies, then set TIDEHUNTER_SETUP_SKIP_APT=1.' >&2
        exit 1
    fi
    privileged=()
    if [[ $(id -u) != 0 ]]; then privileged=(sudo -n); fi
    timeout 300 "${privileged[@]}" apt-get -o Acquire::Retries=2 -o Acquire::http::Timeout=30 update
    timeout 600 "${privileged[@]}" env DEBIAN_FRONTEND=noninteractive apt-get \
        -o Acquire::Retries=2 -o Acquire::http::Timeout=30 install -y --no-install-recommends \
        build-essential clang libclang-dev pkg-config libfontconfig1-dev libssl-dev \
        cmake python3 python3-pip python3-yaml python3-boto3 openssl ca-certificates curl bzip2
fi
for tool in python3 timeout curl make cc pkg-config; do
    command -v "$tool" >/dev/null || { echo "Missing setup dependency: $tool" >&2; exit 1; }
done
# Keep installations user-local and never change the caller's default toolchain.
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
mkdir -p "$cache"
if ! command -v rustup >/dev/null; then
    installer=$(mktemp "$cache/rustup-init.XXXXXXXX")
    trap 'rm -f "$installer"' EXIT
    curl --fail --location --proto '=https' --tlsv1.2 --connect-timeout 30 --max-time 120 \
        --retry 2 https://sh.rustup.rs --output "$installer"
    timeout 600 sh "$installer" -y --profile minimal --default-toolchain none --no-modify-path
    rm -f "$installer"
    trap - EXIT
fi
# apt modules may not be visible to a hosted toolcache Python. Prepare the exact
# interpreter used by checks.py; never change global Python packages or shell files.
python3 -c 'import tomllib' || { echo 'Python 3.11 or newer is required' >&2; exit 1; }
if ! python3 -c 'import yaml, boto3' >/dev/null 2>&1; then
    pip_scope=()
    if python3 -c 'import sys; sys.exit(sys.prefix != sys.base_prefix)'; then
        pip_scope=(--user)
    fi
    timeout 300 python3 -m pip install "${pip_scope[@]}" --disable-pip-version-check \
        --retries 2 --timeout 30 'PyYAML==6.0.2' 'boto3==1.35.99'
fi
python3 -c 'import tomllib, yaml, boto3'
# The Python runner reads this same project toolchain file.
toolchain=$(python3 -c 'import pathlib,sys,tomllib; print(tomllib.loads(pathlib.Path(sys.argv[1]).read_text())["toolchain"]["channel"])' "$root/rust-toolchain.toml")
timeout 900 rustup toolchain install "$toolchain" --profile minimal --component rustfmt --component clippy
(cd "$root" && timeout 900 cargo "+$toolchain" fetch --locked)

if [[ $mode == core ]]; then
    echo 'Core dependencies prepared. Run: python3 .github/scripts/checks.py native'
    exit 0
fi

prefix=$(realpath -m "${TIDEHUNTER_LIBFABRIC_PREFIX:-$cache/libfabric-2.6.0}")
# Never turn a root-run Cloud setup into a system library replacement.
case "$prefix" in
    /|/usr|/usr/*|/lib|/lib/*|/lib64|/lib64/*|/bin|/bin/*|/sbin|/sbin/*|/etc|/etc/*|/opt|/opt/*)
        echo "Refusing system/shared libfabric prefix: $prefix" >&2; exit 1 ;;
esac
parent=$prefix
while [[ ! -e $parent ]]; do parent=$(dirname "$parent"); done
if [[ ! -O $parent || ! -w $parent ]]; then
    echo "Prefix must be in a writable directory owned by the current user: $prefix" >&2
    exit 1
fi
pcpath="$prefix/lib/pkgconfig:$prefix/lib64/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
existing=0
if [[ -f $prefix/lib/pkgconfig/libfabric.pc || -f $prefix/lib64/pkgconfig/libfabric.pc ]]; then
    existing=1
    PKG_CONFIG_PATH="$pcpath" pkg-config --atleast-version=2.6.0 libfabric
    echo "Reusing existing libfabric prefix, without changing providers: $prefix"
elif [[ -e $prefix ]]; then
    echo "Refusing to overwrite an existing incomplete prefix: $prefix" >&2
    exit 1
fi
if [[ $existing == 0 ]]; then
    # Official release checksum: https://github.com/ofiwg/libfabric/releases/tag/v2.6.0
    # Release tag commit: 5de2d6a2df83595bcc38531cb91bf59495559495.
    url=https://github.com/ofiwg/libfabric/releases/download/v2.6.0/libfabric-2.6.0.tar.bz2
    sha512=4aa70dfad7d38c142c6758ce00a23fc0f2f919cb3a4acf9ea294c6bc1db182fc402e4e6578148b34a37b9bd0dad09045cf0284d118c6a5e47eea8b766c051c37
    archive=$cache/libfabric-2.6.0.tar.bz2
    if [[ ! -f $archive ]]; then
        partial=$(mktemp "$cache/libfabric-download.XXXXXXXX")
        trap 'rm -f "$partial"' EXIT
        curl --fail --location --proto '=https' --tlsv1.2 --connect-timeout 30 --max-time 300 \
            --retry 2 "$url" --output "$partial"
        printf '%s  %s\n' "$sha512" "$partial" | sha512sum --check --status
        mv "$partial" "$archive"
        trap - EXIT
    fi
    printf '%s  %s\n' "$sha512" "$archive" | sha512sum --check --status
    work=$(mktemp -d "$cache/libfabric-build.XXXXXXXX")
    trap 'rm -rf "$work"' EXIT
    tar -xjf "$archive" -C "$work"
    (
        cd "$work/libfabric-2.6.0"
        timeout 300 ./configure --prefix="$prefix" --libdir="$prefix/lib" \
            --enable-only --enable-tcp --enable-rxm --disable-static
        timeout 900 make -j "$jobs"
        timeout 300 make install
    )
    PKG_CONFIG_PATH="$pcpath" pkg-config --exact-version=2.6.0 libfabric
    rm -rf "$work"
    trap - EXIT
fi
printf 'Fabric dependencies prepared (software correctness only).\nRun: TIDEHUNTER_LIBFABRIC_PREFIX=%q python3 %q software-fabric\n' \
    "$prefix" "$root/.github/scripts/checks.py"
