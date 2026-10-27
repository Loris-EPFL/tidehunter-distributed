# Development repetition and variability assessment

## Decision

Future development campaigns use one independent repetition of every full-matrix case. Existing repetitions are retained. Do not buy additional development repetitions when their observed spread cannot change the report's stated material conclusion. Publication/reproducibility studies still need multiple independent repetitions under an explicit protocol.

This assessment uses only already-recorded repetitions 0 and 1. It launches no experiment and does not replace or recompute any original case percentile.

## Frozen inputs and thresholds

[variability-summary.json](variability-summary.json) records both input CSV hashes and resource profiles. The previous and current histories are analyzed **separately**. The older 4-CPU/8-GiB and current 8-CPU/16-GiB settings, changed scheduling and stronger read validation prevent treating their difference as a causal architecture regression.

[variability-policy.json](variability-policy.json) freezes the engineering thresholds before any follow-up measurement. Its SHA-256 is `14b68bfe6be6ada22395cf0fa703af77fce3495953f3eb9efcb5b9e5d73221eb`.

| Decision metric | Material effect threshold |
|---|---|
| API p50 or p99 latency | At least 10% of baseline **and** at least 1 µs |
| Achieved successful calls/s | At least 10% of baseline |
| Rejection fraction | At least one percentage point |

These thresholds define development decisions, not statistical confidence, equivalence or significance. A different question may need a different threshold; declare it before that question's rerun. Do not retroactively adjust the threshold to favor one backend.

## Method

For each history, keep cohort, workload, value size, operation/outcome and metric separate. Retain each backend's r0/r1 values and sample counts. Compare all four combinations of the two baseline values and two candidate values against the declared threshold.

If all four combinations give the same material conclusion, observed spread does not change **that descriptive conclusion on this history**. No extra development repetition is recommended just to confirm it, including an unfavorable result. If combinations cross the decision boundary, the configuration is eligible for targeted review only when that question matters to the report. This does not automatically schedule a rerun.

The range of two observations is not a confidence interval. A within-threshold result is not proof of equivalence or a statistically insignificant difference. API tail values retain their original sample-count warnings. Offered-load conditional latency must be considered with goodput and rejection counts; rejecting more arrivals cannot establish superiority by making the surviving calls fast.

## Coverage

| History | Unchanged decision across observed spread | Decision crosses material boundary |
|---|---:|---:|
| Current `th-e4a783f03101a780` | 3,624 | 280 |
| Previous `th-7dea3c080cb44d48` | 3,502 | 374 |

These are **comparison/metric/operation decision cells**, not distinct benchmark cases or requests for thousands of reruns. No extra repetitions have been scheduled.

- [Every pairwise decision](variability-comparisons.csv): 7,780 rows.
- [Every within-history/backend observed spread](variability-run-spreads.csv): 2,456 rows.

## Concrete conclusions

- **8-KiB concurrent reads, native versus EFA-native:** every observed combination keeps the distributed API materially slower. Repeating merely to obtain a missing third observation cannot change the coarse conclusion supported by these ranges. Do not run until the unfavorable result disappears.
- **256-B coordinator-cache API, TCP versus EFA-native:** every combination remains inside the declared material band. More development repetitions are not needed to rank a sub-microsecond difference that the chosen decision threshold does not treat as material. This is not a claim of true equivalence or of a measured transport RTT.
- **1-KiB concurrent writes, native versus EFA-native:** the API-p50 difference crosses the 10% boundary across observed combinations. A targeted follow-up is eligible **only if that boundary is a necessary report claim**. Freeze the experiment and run both involved backends, not EFA-native alone. If the claim compares all eight systems, include all eight.

## Required follow-up record

Before a selective rerun, record the precise question, material threshold/direction, uncertain or regressed configuration, all compared backends, source/binary identities, server version/image pins, hardware/resource allocation, seed and workload, concurrency/load, cache/cohort settings, phases, operation/sample counts, guarantee/security contract and requested repetition count. Use the same frozen plan across every involved backend.

A changed-setting observation is confounded, not a reproducible regression. Do not narrow the default full 228-case matrix or launch another full matrix merely to fill a missing third repetition. Targeted follow-ups require their own explicit reviewed selection. Final publication and explicit reproducibility checks keep multiple independent repetitions; they do not inherit a confidence claim from this development triage.
