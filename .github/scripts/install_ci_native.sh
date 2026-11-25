#!/usr/bin/env bash
# Install the hosted-runner packages that the local KV workflow needs.
#
# Every network call is bounded. A stalled mirror costs at most one retry on
# the upstream archive mirror plus one install attempt, never a step budget.
#   install_ci_native.sh resolve
#       Refresh the index and write the package-version cache key.
#   install_ci_native.sh install <receipt-dir> [rdma]
#       Install only the packages the image lacks and record the receipts.
set -euo pipefail

update_budget=120
install_budget=180
# The install path is bounded at 180 s, then one mirror switch, one 120 s index
# attempt and a second 180 s attempt: 480 s inside the 9-minute step cap. The
# resolve path is bounded at 120 s per index attempt, so 240 s inside 6 minutes.

apt_update() {
  sudo env DEBIAN_FRONTEND=noninteractive timeout "$update_budget" apt-get \
    -o Acquire::Retries=2 -o Acquire::Retries::Delay=2 \
    -o Acquire::http::Timeout=15 -o Acquire::https::Timeout=15 update
}

use_archive_mirror() {
  # A stalled region mirror is the observed failure mode (run 44536336265).
  # The upstream archive is the fallback; the second attempt shares the bound.
  local source
  for source in /etc/apt/sources.list /etc/apt/sources.list.d/*; do
    [[ -f $source ]] || continue
    sudo sed -i -e 's|//azure\.archive\.ubuntu\.com|//archive.ubuntu.com|g' "$source"
  done
}

resolve() {
  apt_update || { use_archive_mirror; apt_update; }
  # The key hashes package names and resolved versions only: `apt-cache policy`
  # also prints mirror URLs, which differ per runner region.
  apt-cache policy build-essential pkg-config clang libclang-dev cmake protobuf-compiler \
    libssl-dev libfontconfig1-dev ca-certificates openssl bzip2 python3-venv \
    libibverbs-dev librdmacm-dev \
    | awk '/^[^ ]/ {package=$1} /^ +(Installed|Candidate):/ {print package, $1, $2}' \
    > "${RUNNER_TEMP:?}/native-package-candidates.txt"
  key="$(sha256sum < "$RUNNER_TEMP/native-package-candidates.txt" | cut -c1-16)"
  echo "key=$key" >> "${GITHUB_OUTPUT:?}"
}

install() {
  local receipt="${1:?receipt directory}" variant="${2:-}"
  # Script scope: the EXIT trap runs after install returns.
  archives="$RUNNER_TEMP/apt-archives"
  mkdir -p "$receipt" "$archives/partial"
  # Root writes the archives dir; hand it back even when apt fails so the cache
  # tarball never reads a root-owned partial/ or lock.
  cleanup() {
    sudo rm -f "$archives/lock" 2>/dev/null || true
    sudo rm -rf "$archives/partial" 2>/dev/null || true
    sudo chown -R "$(id -u):$(id -g)" "$archives" 2>/dev/null || true
  }
  trap cleanup EXIT
  {
    uname -a
    lscpu
    cat /proc/meminfo
    df -B1 "$RUNNER_TEMP" "$GITHUB_WORKSPACE"
    findmnt -T "$GITHUB_WORKSPACE"
    cat /proc/self/status
    for limit in /sys/fs/cgroup/{cpu.max,memory.max,memory.swap.max}; do
      if [ -f "$limit" ]; then printf '%s: ' "$limit"; cat "$limit"; fi
    done
  } > "$receipt/host-resources.txt"
  local packages=(build-essential pkg-config libssl-dev libfontconfig1-dev ca-certificates
                  openssl bzip2 python3-venv protobuf-compiler)
  if [[ $variant == rdma ]]; then packages+=(libibverbs-dev librdmacm-dev); fi
  if ! command -v clang >/dev/null; then packages+=(clang); fi
  if ! command -v cmake >/dev/null; then packages+=(cmake); fi
  # Bindgen loads the C shared library plus the Clang resource headers; both
  # must be present before libclang-dev can be skipped.
  local library header libclang_found=0
  for library in "${LIBCLANG_PATH:-/usr/lib}"/libclang.so* \
                 "${LIBCLANG_PATH:-/usr/lib}"/libclang-[0-9]*.so* \
                 /usr/lib/llvm-*/lib/libclang.so* \
                 /usr/lib/llvm-*/lib/libclang-[0-9]*.so* \
                 /usr/lib/*-linux-gnu/libclang.so* \
                 /usr/lib/*-linux-gnu/libclang-[0-9]*.so*; do
    [[ -f $library && -r $library ]] || continue
    # Globs do not expand inside [[ ]]; iterate explicitly.
    for header in "$(dirname "$library")"/clang/*/include/stddef.h; do
      if [[ -f $header ]]; then
        libclang_found=1
        echo "LIBCLANG_PATH=$(dirname "$library")" >> "${GITHUB_ENV:?}"
        break 2
      fi
    done
  done
  if [[ $libclang_found == 0 ]]; then packages+=(libclang-dev); fi
  local package
  local missing=()
  for package in "${packages[@]}"; do
    if ! dpkg-query -W -f='${db:Status-Abbrev}' "$package" 2>/dev/null | grep -q '^ii'; then
      missing+=("$package")
    fi
  done
  if [ "${#missing[@]}" -ne 0 ]; then
    if ! sudo env DEBIAN_FRONTEND=noninteractive timeout "$install_budget" apt-get \
        -o Acquire::Retries=2 -o Acquire::Retries::Delay=2 \
        -o Acquire::http::Timeout=20 -o Acquire::https::Timeout=20 \
        -o Dir::Cache::archives="$archives" \
        -o APT::Keep-Downloaded-Packages=1 \
        install -y --no-install-recommends "${missing[@]}"; then
      use_archive_mirror
      apt_update
      sudo env DEBIAN_FRONTEND=noninteractive timeout "$install_budget" apt-get \
        -o Acquire::Retries=2 -o Acquire::Retries::Delay=2 \
        -o Acquire::http::Timeout=20 -o Acquire::https::Timeout=20 \
        -o Dir::Cache::archives="$archives" \
        -o APT::Keep-Downloaded-Packages=1 \
        install -y --no-install-recommends "${missing[@]}"
    fi
  else
    echo "Native prerequisites already installed on the runner image."
  fi
  { clang --version; cmake --version | head -n 1; protoc --version; } \
    | tee "$receipt/native-tools.txt"
  local listed=(build-essential pkg-config cmake protobuf-compiler libssl-dev
                libfontconfig1-dev python3-venv clang libclang-dev)
  if [[ $variant == rdma ]]; then listed+=(libibverbs-dev librdmacm-dev); fi
  dpkg-query -W -f='${Package} ${db:Status-Abbrev} ${Version}\n' "${listed[@]}" \
    > "$receipt/native-packages.txt" 2>/dev/null || true
}

case "${1:-}" in
  resolve) resolve ;;
  install) shift; install "$@" ;;
  *) echo "usage: $0 resolve | install <receipt-dir> [rdma]" >&2; exit 2 ;;
esac
