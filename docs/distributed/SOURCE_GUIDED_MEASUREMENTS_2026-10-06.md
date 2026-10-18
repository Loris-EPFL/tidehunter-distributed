# Source-guided local measurements: complete comparison

Date: 6 October 2026. Companion to [source-to-code mapping](SOURCE_GUIDED_PIPELINES_2026-10-06.md). All results below are from the original completed 252-case matrix; no outliers are removed. Three repetitions per exact shape; each displayed value is the median of the three per-run statistics. The new combined variant uses two preparation workers and two-page scan lookahead. All TCP value caches are disabled. The native application cache remains disabled. These are five processes on one shared SSD with warm workload ordering, not cold-device or cluster measurements.

## All measured API phases, 8 KiB / batch 32 / window 32

The unit is **microseconds per measured API call or complete write window**. A write window here contains up to 32 batches, each up to 32 operations. Multi-get is a driver call with up to 32 keys; the native driver calls its scalar API internally. Open/reopen/scan/maintenance are whole operations. Different rows are not additive stages. Means and percentiles are different statistics.

| Phase | Previous TCP mean µs | New TCP mean µs | Native mean µs | New/previous | New/native |
| --- | ---: | ---: | ---: | ---: | ---: |
| `open` | 62,144.722 | 62,582.344 | 2,796.657 | 1.007× | 22.378× |
| `load_write_window` | 23,338.129 | 20,975.450 | 9,974.791 | 0.899× | 2.103× |
| `point_get` | 33.418 | 35.529 | 1.940 | 1.063× | 18.315× |
| `point_get_many` | 329.541 | 353.438 | 56.311 | 1.073× | 6.277× |
| `point_get_missing` | 0.322 | 0.357 | 0.220 | 1.108× | 1.619× |
| `exists` | 0.444 | 0.327 | 0.144 | 0.737× | 2.272× |
| `exists_missing` | 0.281 | 0.211 | 0.136 | 0.750× | 1.546× |
| `mixed_write_window` | 22,070.909 | 20,566.058 | 10,576.748 | 0.932× | 1.944× |
| `mixed_get` | 34.710 | 25.379 | 1.816 | 0.731× | 13.973× |
| `checkpoint_capture` | 0.977 | 0.850 | 88.121 | 0.870× | 0.010× |
| `checkpoint_intervening_write` | 771.132 | 859.938 | 603.924 | 1.115× | 1.424× |
| `checkpoint_get` | 64.804 | 53.020 | 5.169 | 0.818× | 10.257× |
| `delete_window` | 2,900.298 | 2,732.189 | 1,673.735 | 0.942× | 1.632× |
| `full_scan` | 22,714.206 | 15,366.860 | 7,037.210 | 0.677× | 2.184× |
| `maintenance_flush_index` | 4,569.324 | 4,441.743 | 6,531.372 | 0.972× | 0.680× |
| `post_flush_get` | 20.445 | 25.722 | 2.251 | 1.258× | 11.427× |
| `shutdown` | 18,812.253 | 20,299.016 | 1,370.995 | 1.079× | 14.806× |
| `reopen` | 243,350.426 | 260,724.575 | 1,775.336 | 1.071× | 146.859× |
| `reopen_get_audit` | 18.497 | 23.646 | 2.174 | 1.278× | 10.877× |

Harness-only process startup/shutdown phases have no native equivalent and are present in the raw CSV. Every tail statistic, full phase wall time, min/max repetition range, stage counter and ablation is retained in [medians](../../logs/source-guided-2026-10-06/analysis/medians.csv), [comparisons](../../logs/source-guided-2026-10-06/analysis/comparison.csv), [stage counters](../../logs/source-guided-2026-10-06/analysis/stages.csv) and [nested read profiles](../../logs/source-guided-2026-10-06/analysis/profiles.csv).

## Every workload shape, original full matrix

Percentages are new combined versus previous TCP; negative is faster. Write and scan compare **full phase wall time**. Reads compare the **per-run p50**. These percentages are descriptive measurements, not causal proof or significance estimates. In particular, batch-1 scans have only 128 live keys and no second-page overlap opportunity.

| Value KiB | Batch | Window | Load change | Point p50 change | Multi-get p50 change | Scan change | Reopen change | Load/native |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | +12.8% | -44.7% | +15.7% | -28.2% | -15.6% | 1.70× |
| 1 | 1 | 8 | +1.0% | -5.7% | -22.8% | -12.3% | +3.5% | 3.07× |
| 1 | 1 | 32 | -1.5% | -33.2% | -21.0% | -11.5% | +5.1% | 2.38× |
| 1 | 32 | 1 | -10.7% | -12.5% | -7.4% | -37.3% | -7.4% | 1.66× |
| 1 | 32 | 8 | -13.9% | +3.2% | -2.6% | -35.7% | -8.5% | 1.69× |
| 1 | 32 | 32 | +6.9% | +5.9% | -1.9% | -52.2% | -11.6% | 2.44× |
| 8 | 1 | 1 | -9.3% | +67.0% | +9.2% | -16.9% | +12.0% | 1.74× |
| 8 | 1 | 8 | +13.3% | +24.6% | +175.7% | +133.2% | +10.8% | 3.12× |
| 8 | 1 | 32 | -8.9% | +12.7% | +26.9% | +25.8% | +5.7% | 3.34× |
| 8 | 32 | 1 | +93.7% | +28.3% | +12.8% | -32.6% | +5.1% | 4.11× |
| 8 | 32 | 8 | +1.0% | +0.9% | -2.7% | -11.6% | +0.3% | 3.01× |
| 8 | 32 | 32 | -10.1% | -2.3% | +3.8% | -32.3% | +7.1% | 2.10× |

## Original small-window anomaly

The 8 KiB/batch32/window1 combined load took **596.030, 279.962 and 1,316.446 ms** across the three runs, versus a previous-TCP median of **307.630 ms**. The original median therefore regressed **93.7%** and remains in the table. The same new executable with two preparation workers and one-page scans took **279.275–314.055 ms** for this shape. All combined runs used 128 direct appends, 128 file barriers, five directory barriers and **zero** parallel preparation calls. Most excess time was in storage wait; validation and publication also varied.

Scan lookahead executes after this load and cannot directly explain that earlier write timing. The evidence does not identify the external or internal cause; it is not silently discarded as noise. A separate nine-case confirmation, using the same frozen executables and no code changes, recorded medians of **327.455 ms previous**, **322.962 ms new two-worker/one-page**, and **304.682 ms combined**. Combined ranged **294.103–325.143 ms**, while previous ranged **282.589–1,283.900 ms**. The severe new-binary regression was not reproduced; an outlier also appeared in the old binary. The [confirmation audit](../../logs/source-guided-2026-10-06/confirmation-analysis/audit.json) verifies every value and reopen. This follow-up does not replace the original matrix or establish tail stability.

## Interpretation boundaries

- Three samples per shape do not establish tail stability or statistical significance.
- New one-worker window preparation collects the whole window before first dispatch; the old scalar submission loop overlapped preparation and persistence. Same-binary parallel speedup alone is not the total improvement against the old architecture.
- Directory syncing is reduced; file persistence barriers remain. Storage-wait timers also include scheduling/protocol delay.
- Read profiling fields are nested; client exchange is not a measurement of pure wire time.
- A disabled application cache does not evict the OS page cache. No isolated physical NVMe I/O counters were collected by this runner.
- Native controls and the original anomalous repetitions remain in the evidence; no universal native parity claim is made.
