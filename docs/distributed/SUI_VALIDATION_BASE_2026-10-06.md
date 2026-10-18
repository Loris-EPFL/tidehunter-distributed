# Sui validation base and existing changes

Date: 6 October 2026. Source inspection only; this document does not claim that
the new partition backend serves Sui. The existing Sui checkout remains unchanged.

## Decision

Use the existing `refactor/distributed-nvme` Sui branch at
`c79040af6c06417245e24574246ada38a99d8b30` for the first matched local comparison of
the previous middleware and new native distributed backend. Keep its previous
three commits and record the exact revision in every result. Once M3–M5 supply
the real native API, create a separate validation branch/worktree from that SHA
for adapter changes. Do not replace the existing benchmark checkout or describe
the new log component as an already integrated backend.

Current MystenLabs `main` is a suitable **second compatibility target**, not a
drop-in improvement to this comparison. Fetching it revealed 117 upstream commits
after the existing baseline, with changes to gas, workload execution, pruning,
JSON-RPC and native Tidehunter caches. Those changes confound a direct comparison
with previous results. The examined named Tidehunter branch is older than the
current validation base and pins an older engine.

## Verified Git identities

| Ref or repository | Verified value |
| --- | --- |
| Sui origin | `git@github.com:Loris-EPFL/sui-tidehunter-distributed.git` |
| Sui upstream | `https://github.com/MystenLabs/sui.git` |
| Local validation branch | `refactor/distributed-nvme` |
| Local HEAD | `c79040af6c06417245e24574246ada38a99d8b30` |
| Original local upstream baseline | `f772ed35aed9adb68e9b90a36ce6d231b9baaa2c` |
| MystenLabs `main`, fetched 6 October | `4630995ca0ca9fd32ebc8ef027c4e68ed0f8c677`, commit dated 5 October |
| MystenLabs `andll/tidehunter-8630bf6-1.76` | `9ce1f83a574d20a2fb0db96bffde93bd4f0fadc2`, commit dated 22 July |
| Tidehunter dependency on both local baseline and current upstream main | `f8bc93cf6a973d31fbbe6fa7c2b643ddd8e83ddf` |
| Tidehunter dependency on the examined 1.76 branch | `8630bf6f2e59d94177557cfffb4e972c63155629` |

The source checkout was clean before and after inspection. Only the two named
upstream tracking refs were fetched; no branch switch, merge, reset or Sui source
edit was performed. The fork contains many historical tracking refs: their names
alone are not evidence that a branch is newer or better suited to validation.

## What the existing Sui changes do

The three local commits change 16 files, with 1,426 additions and 155 removals.
They include integration hooks **and changes to Sui execution/cache behavior**;
calling them only a dependency patch would be inaccurate.

| Commit | Changes relevant to validation | Treatment for the new backend |
| --- | --- | --- |
| `60dc10875a`, Add gated distributed Tidehunter integration | `typed-store` bulk get/exists hooks, test-only acknowledgement configuration, bounded-replay session handshake; `sui-node` local state-audit example and node-role gating | Preserve for the old comparator. New backend selection must have an explicit identity and cannot silently run the old facade. Reuse the audit contract after native APIs are implemented. |
| `033f1738de`, Improve distributed storage durability and checkpoint handling | Checkpoint watermark/summary cache and serialized publication, pruning/cache invalidation coordination, cache-read health validation, fallible shared-version initialization, corrected checkpoint pipeline stage timers | Preserve for matched comparisons. These materially affect dependent operations. The new native backend needs equivalent health checking; otherwise a Sui cache hit could bypass an unresolved authority failure. |
| `c79040af6c`, Propagate shared version read errors and preserve pruning updates | Propagates quarantine storage read failures rather than panicking/continuing and preserves pruning epoch/cache behavior | Retain correctness changes. Any upstream rebase needs deliberate reapplication and regression tests, not blind cherry-picking of old signatures. |

Important source boundaries:

- `crates/typed-store/build.rs`: `USE_TIDEHUNTER` enables `cfg(tidehunter)`.
- `crates/typed-store/Cargo.toml`: Tidehunter dependency plus
  `distributed-tidehunter-{bulk,durability,session}` features.
- `crates/typed-store/src/rocks/mod.rs`: native/facade API calls; bulk paths and
  `validate_cached_read()` reach APIs absent from unmodified native Tidehunter.
- `crates/typed-store/src/tidehunter_util.rs`: native configuration, schema and
  acknowledgement knobs; the new fork does not inherit the old fork's added
  `sync_writes` and `coalesce_wal_directory_sync` fields automatically.
- `crates/sui-core/src/checkpoints/{mod,control_cache}.rs`: cache mutation ordering
  and health checks are part of the application behavior used by old results.
- `crates/sui-core/src/authority/{shared_object_version_manager,consensus_quarantine}.rs`:
  errors must propagate before initializing object versions.
- `crates/sui-core/src/authority/authority_store_pruner.rs`: pruning must update
  durable data and cached summaries consistently.
- `crates/sui-node/examples/distributed_storage_local_audit.rs`: real table audit
  tooling. An adapter must supply its expected session/health interfaces before
  this can honestly validate a new serving engine.

Merely changing the Cargo patch from `middleware/crates/tidehunter-facade` to this
fork is insufficient. The current native `Db` path still has its original local
WAL and there is no new `Db::open` backend selection yet. The M1/M2 log substrate
must not be counted as Sui integration.

## What current upstream changes would introduce

`git diff f772ed35ae..upstream/main -- crates/typed-store` is empty. The new main
does not supply a newer typed-store adapter that removes our integration work.
It does introduce these relevant differences:

- `83f11dc85d`: doubles Tidehunter object/transaction caches. Memory and hit rates
  would differ from the existing local/cloud comparisons.
- `ffda814a72`: changes executed-transaction digest pruning across node modes.
- `8b88240781`: changes per-epoch store ownership/lifetime.
- `5ca40428ee` and later protocol changes: execution work per transaction can
  differ even with nominally identical storage configuration.
- `e4eac81f31`: removes fullnode JSON-RPC, requiring review of the old harness.

Current upstream `assign_versions_from_effects` still discards the fallible
`get_or_init_versions` result, and its quarantine lookup still uses
`multi_get(...).expect("db error")`. Our propagated-error changes are therefore
not made redundant merely by choosing current upstream main.

This is a source-level comparison, not a new Sui build or runtime compatibility
claim. The appropriate later sequence is:

1. Finish native M3/M4 publication/recovery and M5 authenticated transport gates.
2. Add the smallest explicit typed-store selection/adapter on the matched branch.
3. Run native versus new distributed engine with identical Sui binary family,
   schema, cache limits, durability mode, workload and complete audit.
4. Separately port to the then-current MystenLabs main. Re-evaluate each custom
   change against upstream; run its own correctness suite and repeat both sides
   before comparing speeds.

## Reproduce the inspection

Run inside the existing `sui` repository:

```sh
git status --short
git remote -v
git log -4 --oneline
git ls-remote upstream refs/heads/main 'refs/heads/*tidehunter*'
git fetch --no-tags upstream \
  refs/heads/main:refs/remotes/upstream/main \
  refs/heads/andll/tidehunter-8630bf6-1.76:refs/remotes/upstream/andll/tidehunter-8630bf6-1.76
git rev-list --left-right --count f772ed35ae...upstream/main
git diff --stat f772ed35ae..HEAD
git diff f772ed35ae..upstream/main -- crates/typed-store
git show upstream/main:crates/typed-store/Cargo.toml
git log --oneline f772ed35ae..upstream/main -- crates/sui-core
```

Pinned primary sources for later review:
[current upstream Sui](https://github.com/MystenLabs/sui/tree/4630995ca0ca9fd32ebc8ef027c4e68ed0f8c677),
[typed-store dependency](https://github.com/MystenLabs/sui/blob/4630995ca0ca9fd32ebc8ef027c4e68ed0f8c677/crates/typed-store/Cargo.toml),
[cache change](https://github.com/MystenLabs/sui/commit/83f11dc85d),
[pruning change](https://github.com/MystenLabs/sui/commit/ffda814a72).

The local shared SSD supports correctness and matched local mechanism checks.
It cannot validate independent-drive throughput, RDMA, or a cluster speedup.
