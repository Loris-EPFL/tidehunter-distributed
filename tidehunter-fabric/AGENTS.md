# Fabric correctness and hardware evidence

Apply the root `AGENTS.md` as well as these transport-specific rules. Read
`DESIGN.md`, `../docs/distributed/IMPLEMENTATION_PLAN_CHECKPOINT_AND_FABRIC_2026-10-18.md`
and `../docs/distributed/CLOUD_AND_CI_TESTING.md` before changing ownership or test
classification. The implementation plan is a proposal, not completed behavior.

## Evidence classes

1. Deterministic CQ/slot fixtures can run without a NIC. They establish only the
   exercised ordering, validation, backpressure and lifetime behavior.
2. `tcp;ofi_rxm` exercises real libfabric calls on loopback. It is not verbs
   hardware, one-sided RDMA or NVMe-oF.
3. Real verbs requires the correct device, provider and registered memory on the
   actual hosts. An open-only check does not establish data-path activity, remote
   durability or performance. Keep the existing hardware tests explicit.

Use the shared runner's `fabric` and `software-fabric` profiles for Cloud/local
correctness. The latter must report failure when the explicitly requested provider
cannot execute. Never convert unavailable hardware into successful fallback or
claim hardware coverage from skipped tests.

## Ownership and correctness review

- Preserve exclusive endpoint/CQ ownership and the fixed registered-slot bounds.
- TX bytes, contexts and reply leases remain owned until exact CQ retirement;
  cancellation is not completion. Failed close must not free live DMA memory.
- EAGAIN consumes no unposted bytes, credit, ordinal or send fence.
- Distinguish local CQ completion, peer ciphertext consumption and the durable
  storage receipt. Only the storage receipt acknowledges durable writes.
- Keep the verbs provider/MR-mode/geometry checks explicit. Do not weaken
  hardware guards to make a software provider look like verbs.
- Do not remove inherited locks merely to call the system lock-free. A new unsafe
  block needs its lifetime/ownership invariant and the smallest necessary scope.

The native Tidehunter paper's WAL-as-values and asynchronous durability design
remain the storage reference; this crate changes message delivery, not recovery
or fsync semantics. eRPC's bounded owner-loop ideas motivate scheduling, not an
assumption that its hardware latency transfers to verbs/TLS.

When tests change, retain consumer-visible byte correctness, fragmentation,
out-of-order completion, exhausted credits, cancellation and deadline cases.
Use temporary diagnostic scenarios for call-count/performance hypotheses rather
than permanent mock-forwarding or source-wording tests. Do not launch clusters,
install a NIC driver or report Cloud-container speed as independent-NVMe speed.
