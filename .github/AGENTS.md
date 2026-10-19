# GitHub Actions and Codex Cloud

This file adds automation-specific rules. It does not replace the repository's
root `AGENTS.md` or the containing project's working rules.

## Entry points

- Shared checks: `python3 .github/scripts/checks.py PROFILE`, from any directory.
- Environment preparation: `bash .github/scripts/setup.sh core` or `fabric`.
- Use the repository Rust toolchain and committed lockfiles. Ordinary checks must
  not install packages, fetch a new provider, rewrite lockfiles or launch AWS.
- Read `docs/distributed/CLOUD_AND_CI_TESTING.md` for profile coverage, setup,
  unavailable-environment handling and the separate AWS campaign path.

## Review rules

### Truthful transport coverage

- Keep deterministic protocol tests, loopback software-provider tests and real
  EFA hardware tests distinct. A software result is not RDMA latency evidence.
- Never run all ignored tests in a Cloud/PR job: the existing integration target
  also contains an explicitly ignored real-EFA test.
- A requested suite must fail if its provider/dependencies or selected tests are
  unavailable. Do not treat zero matched tests as a pass or silently fall back
  from EFA/direct to TCP.
- Do not label a libfabric compile or endpoint-open check as an end-to-end hardware
  validation. That requires the existing multi-host activity and state proofs.

### Trust and cost

- PR correctness jobs use least privilege and no AWS credentials. Do not use
  privileged `pull_request_target` to check out and execute untrusted PR code.
- Paid benchmarks run only from explicitly trusted manual campaigns or the
  protected `bench/aws-efa` push workflow after account-side activation. Keep its
  non-cancelling concurrency, independent expiry, owned result export and cleanup
  separate from cheap cancellable CI. Compiler jobs use `rdma-bench-inputs`;
  only orchestration/accounting jobs may use the `rdma-bench` fleet identity.
- A GitHub environment name alone does not configure required reviewers. Document
  the account-side approval setting; never imply that committing YAML enables it.
- Do not add API-key review bots for subscription-backed Codex review. The native
  GitHub integration is configured in Codex settings, outside workflow YAML.
- Pin external actions/dependency sources. Scope caches to their actual toolchain,
  lockfile and provider inputs; a cache hit is not proof of current binary identity.
- Use the connected AWS MCP for interactive AWS operations. If it disconnects,
  pause AWS work and notify the user so they can reconnect it; do not switch to
  local AWS CLI/SDK fallback. Retry through MCP after reconnection. This does not
  replace the authorized campaign's own SDK calls inside GitHub/EC2.

### Preserve existing campaigns

- Keep local and AWS runner APIs, source/config manifests, exact binary hashes,
  protocol/ALPN checks, per-host provider proofs and exact-owned-resource cleanup.
- Never start a paid cluster without explicit authorization. Workflow cancellation
  is not a substitute for the cluster's independent deadline/termination logic.
- Export and verify results before deleting their storage. Do not keep paid hosts
  alive for a slow local download or offline analysis.
- Do not modify historical hash-bound reports or `checkpoint-work` source copies
  to make new checks pass. Change the maintained code and record new evidence.
