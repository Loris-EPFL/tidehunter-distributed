# Superseded in part

The brief `docs/research/supervisor-brief-2026-11-07/main.pdf` replaces the decision in section "Next work: fast remote WAL reads" of `main.tex`.

| Statement in this brief | Status | Reason |
|---|---|---|
| "Keep kernel NVMe/RDMA as the current disk-read baseline. Do not claim the present SPDK target is faster." | Withdrawn | The kernel target served the sealed first-touch reads from its page cache (0.03 device reads per call at 1 KiB). The SPDK target bypasses the page cache and read the SSD on every call (1.0 device reads per call). The two arms did not read the same medium, so the p50 gap (40.4/44.4/63.8 µs kernel against 63.0/106.7/137.2 µs SPDK at 1/8/64 KiB) is a cache gap, not a transport gap. Where both read the SSD (cold 64 KiB, c16), SPDK p50 is 9% lower and p99 is 2.4 times lower (one repetition). |
| Latency numbers of the sealed first-touch cases | Valid as measured | They describe a cache-resident kernel target against an SSD-reading SPDK target. Do not use them to rank the transports. |
| Experiment E0 (QD=1 floor with `fio`, `bdevperf` and the project client) | Still open | Add a direct-I/O control for the kernel target so both arms read the SSD. |

Evidence: block-device counters (`/proc/diskstats`) at the measurement gates of every case, in `docs/research/supervisor-brief-2026-11-07/device-reads.csv`.
