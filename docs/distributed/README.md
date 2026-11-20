# Distributed Tidehunter documentation

Navigation only. Removed designs are archived at tag
`archive/multi-design-*`; their documents are not recreated here.

- [Current architecture](ARCHITECTURE.md) — the leased NVMe-oF design: one
  current description of roles, write, read, recovery, control, lifecycle and
  capacity.
- [Durability tiers](DURABILITY_TIERS.md) — `native` and `synchronous`
  acknowledgement, version B durable-prefix recovery.
- [Model](MODEL.md) — the bounded executable protocol model.
- [Cloud and CI testing](CLOUD_AND_CI_TESTING.md) — local, kernel NVMe/TCP
  loopback CI and GCP validation surfaces.
- [KV comparison protocol](KV_COMPARISON_PROTOCOL.md) — comparison research
  rules for external KV adapters.
- [GCP RDMA minimum access](GCP_RDMA_MINIMUM_ACCESS.md) — the least-privilege
  GCP service account for cloud qualification.
- [Refactor findings](REFACTOR-FINDINGS.md) — concurrency findings that need
  a main decision.
- [Live WAL notes](../../experiments/nvmeof/LIVE_WAL.md) — leased-extent
  commands and durability limits.
- [Sui checkpoint experiments](../../experiments/sui-checkpoint/README.md) —
  historical Sui replay campaigns and evidence rules.
