#!/usr/bin/env bash
# Runs only inside the Rocky build container, with host paths mounted unchanged.
set -euo pipefail
# The daemon-owned container must stop even if its Docker client disappears.
if [[ ${TIDEHUNTER_GCP_BUILD_CHILD:-0} != 1 ]]; then
    remaining=$(( ${TIDEHUNTER_GCP_BUILD_DEADLINE_EPOCH:?} - $(date +%s) ))
    (( remaining > 0 )) || exit 124
    export TIDEHUNTER_GCP_BUILD_CHILD=1
    exec timeout --signal=TERM --kill-after=5 "$remaining" "$BASH" "$0" "$@"
fi
engine=$1
output=$2
build_timeout=$3
with_valkey=$4
export HOME="$output/container-home"
export CARGO_HOME="$HOME/cargo" RUSTUP_HOME="$HOME/rustup"
export PATH="$CARGO_HOME/bin:$PATH"
export TIDEHUNTER_SETUP_SKIP_APT=1 TIDEHUNTER_SETUP_SKIP_AWS_SDK=1
export TIDEHUNTER_LIBFABRIC_PREFIX="$output/fabric"
mkdir -p "$HOME"
bash "$engine/.github/scripts/setup.sh" fabric
args=()
if [[ $with_valkey == 1 ]]; then
    python3 "$engine/experiments/kv-comparison/prepare_valkey_rdma.py" --output "$output/valkey-source"
    args+=(--valkey-source "$output/valkey-source")
fi
exec python3 "$engine/experiments/kv-comparison/cloud_package.py" build \
    --snapshot "$output/snapshot" --output "$output/build" \
    --fdb-prefix "$output/fdb-client" --fabric-prefix "$output/fabric" \
    --timeout "$build_timeout" "${args[@]}"
