# Sui mainnet workload sampling

The archive uses zstd-compressed `sui.rpc.v2.Checkpoint` protobuf messages, not recent `.chk` BCS blobs. The decoder reads selected protobuf fields and uses the local `sui-sdk-types` BCS implementation for transactions, effects, events, summaries and objects. It recomputes every transaction digest and stops on a mismatch. The protobuf field numbers are from `sui-rpc/src/proto/generated/sui.rpc.v2.rs` in the Sui Rust SDK checkout. SDK types are preferable to a handwritten Sui BCS parser because they cover current transaction and effects variants and provide the digest implementation.

This experiment uses the SDK checkout at the path in `Cargo.toml`. Edit that path if the checkout moves. The build uses its own target directory and does not use the Sui node target directory. It needs Rust, the offline Cargo cache, Python with NumPy, and `zstd`.

From this directory, for the pilot:

```sh
DATA=/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/sui-workload-20261107
cargo build --offline --release --target-dir "$DATA/target"
cargo test --offline --target-dir "$DATA/target"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python download.py "$DATA"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python analyze.py "$DATA"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python stats.py "$DATA"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python -m unittest discover -s tests -v
```

For the primary sample, after the build above:

```sh
export TH_PRIMARY=1
PRIMARY="$DATA/primary"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python download.py "$PRIMARY"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python analyze.py "$PRIMARY"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python validate_api.py "$PRIMARY"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python stream_stats.py "$PRIMARY"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python extended.py "$PRIMARY"
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python report.py "$PRIMARY"
```

The downloader uses at most eight connections, retries transient errors with exponential backoff, and stops on 429, repeated 5xx or the ten-billion-byte cap. The primary budget includes retained pilot files and a reserve for probe traffic. Existing files are skipped. The fixed windows and sizes are in `download.py`. Run only when the archive still retains those checkpoints. GraphQL cross-checks are in `graphql-validation.json` and `api_validation.csv` in each data directory.

`workload-model.json` includes definitions for its empirical quantiles, rank buckets, object changes, and censored ages. The model is a calibration summary, not a generator or a record of node KV requests.

`input_objects.csv` is a reconstruction from effects object changes, read-only consensus roots, programmable transaction object references and gas references. It is not a node storage trace: checkpoint-level object sets are deduplicated and do not specify a complete per-transaction object-read list. In particular, package and other execution reads can be absent. The age columns only cover versions written earlier in the same window. Unknown versions are left blank and are not zero. Output bytes are serialized BCS object sizes, not database records or physical I/O.
