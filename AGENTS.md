# Working Rules

Follow these rules for all tasks unless the current user message gives a more specific instruction. Skills hold the detailed reference documentation; the [Skills](#skills) section maps each work area to its skill.

## Priorities

- Optimize for correctness, then performance, then developer experience, in that order.
- Treat readability as a tool for correctness and maintenance, not as an end by itself.
- Prefer simple designs that satisfy all three priorities.
- Spend design effort before implementation when it can prevent complexity or production cost.
- Do not accept known technical debt in changed code.
- Do not ship undefined behavior, unsound `unsafe` code, data races, or avoidable exponential work.

## Communication

- Use concise ASD-STE100 Simplified Technical English.
- Use direct statements.
- Prefer short, common words when they are precise.
- Remove every word that does not add useful information.
- Prefer active voice.
- Avoid metaphors, idioms, figures of speech, and unnecessary jargon.
- Use technical terms when they are more precise than common words.
- Do not add unnecessary explanation.
- Do not mention yourself in Git commit messages.
- Do not suggest, create, or request smoke tests.

## Task Quality

- Complete the full requested task.
- Do not stop mid-task to report progress. Continue until the task is complete or blocked on input only the user can provide.
- Do not defer a sub-item silently. List anything not done, with the reason.
- Before reporting completion, verify every sub-item against the files, builds, and tests — not against your recollection.
- Surface every defect you find. Never leave a known defect unreported.
- When the user asks a question, answer it. Do not change code unless asked.
- Verify a checkable fact (git state, API signature, numbers, environment) before you state it.
- Check related code and behavior when needed to make the requested change correct.
- Fix root causes. Do not hide defects with workarounds. A symptom that recurs after your fix means the fix was wrong: trace to the origin.
- If a correct solution requires a large or unclear workaround because of a bug in a tool, crate, or dependency that we control, stop and ask before you add the workaround.
- Do not overengineer.
- Prefer the smallest correct change.
- Minimize added code, complexity, maintenance cost, latency, allocations, and unnecessary work.
- Preserve existing behavior unless the task requires a behavior change.

## Code Changes

- Change only the files and code that are necessary for the task.
- Follow YAGNI principles, and prefer one-liner solutions.
- Prefer existing abstractions when they are suitable.
- Add an abstraction only when it makes the domain clearer or removes real duplication.
- Do not add abstractions for hypothetical future use.
- Do not add defensive code for conditions that cannot occur under the existing contract.
- Keep performance-sensitive paths efficient.
- Do not trade correctness or readability for meaningless code-size reduction.
- Remove dead code that becomes unnecessary because of the requested change when removal is safe and directly related to the task.
- Justify every `unsafe` block with a comment that states the invariant the caller must uphold and why the block preserves it.
- Prefer safe abstractions over `unsafe` code. Add `unsafe` only when a safe alternative cannot meet the performance requirement, and confine it to the smallest possible scope.

## Correctness

- Use simple, explicit control flow.
- Give every loop and queue a fixed upper bound.
- Assert the intended non-termination of loops that are designed to run forever (e.g. the request-accept loop).
- Handle every operating error. Do not use `unwrap` or `expect` on a path that can fail in production; reserve them for conditions the type system or an earlier check has already ruled out.
- Treat assertion failures and panics in server code as programmer errors and corrupt on-disk state as fatal.
- Treat all `clippy` lints at the strictest supported setting (`-D warnings` with `clippy::pedantic` reviewed) as errors, unless a lint is explicitly and individually allowed with a comment stating why.
- Preserve crash consistency: any code touching the write-ahead log, memtable flush, or compaction path must leave on-disk state recoverable after a crash at any point.

## Performance

- Consider performance during design, not only after implementation.
- Estimate network, disk, memory, and CPU costs when they can affect the design.
- Consider both bandwidth and latency, and consider tail latency (p99/p999) separately from throughput.
- Optimize the slowest or most frequently used resources first.
- Prefer flat data and predictable access over unnecessary pointer chasing.
- Amortize allocation cost. Avoid allocating on the hot read/write path when a reusable buffer or pool can serve instead.
- Grow dynamic containers geometrically instead of one element at a time.
- Prefer zero-copy parsing and serialization on the wire-protocol and storage paths when it does not compromise correctness or readability.
- For contended shared state, prefer lock-free structures or sharded locks. Use the actor/message-passing pattern with bounded channels for backpressure. Details: `kvstore-concurrency`.

## Tooling

- Prefer existing project tools before adding new tools.
- Use `cargo` for build, test, format (`cargo fmt`), lint (`cargo clippy`), and the project's own benchmark harness (`cargo bench` / `criterion`) when they fit the task.
- Do not add a tool when the existing project tool can solve the problem well.

## Dependencies

- Keep runtime dependencies minimal.
- Prefer the standard library and already-vetted crates over new ones.
- Any new crate needs a stated reason: no in-tree or standard-library equivalent meets the requirement.
- Treat every new dependency as a supply-chain, versioning, build, and maintenance cost. Check license, maintenance activity, and `unsafe` usage before adding one.
- Pin dependency versions in `Cargo.lock` and commit it for binaries; review `cargo update` diffs before accepting them.

## Tool, Compiler, and Dependency Defects

If natural and correct code fails because of a defect in a tool, the Rust compiler, or a dependency that we control:

1. Treat that defect as part of the task.
2. Fix the root cause when the fix is clear and reasonably scoped.
3. Keep the natural code instead of adding a workaround.
4. If the root-cause fix is unclear, unsafe, or too large, stop and ask before you add a workaround.

## Git

- Never run `git commit` unless the current user message explicitly asks for a commit.
- Never run `git push` unless the current user message explicitly asks for a push.
- Run `git status` before any `checkout`, `restore`, `reset`, or `clean`. Never revert paths you did not create.
- After any git action, verify with `git status` or `git log` that it happened before reporting it.
- Permission to commit or push applies only to the current user message. It does not carry to later messages.
- If asked to commit, create exactly one commit unless the user asks for more.
- Never use `git commit --no-verify`, `git commit -n`, or any method that disables or skips hooks.
- If a Git hook fails, fix the cause. Do not bypass the hook.

### Commit Messages

- Use only a short subject line unless the user explicitly asks for a body.
- Do not use `--` punctuation or em dashes.
- Do not mention session-local identifiers such as phase numbers, bug numbers, or increment numbers unless they are real project identifiers required by the user.
- Do not mention tests, checks, benchmarks, or validation results.
- Do not mention the assistant or AI.
- Make the subject describe the change clearly enough to remain useful in `git blame`.

## Web Search

Use web search when the task needs current external facts, a referenced page or paper,
precise quotations, external recommendations, or information that is uncertain or
likely to change. Do not use web search for repository facts that source inspection can
answer.

For technical claims, use primary sources such as official documentation, RFCs, crate
documentation, or research papers. Compare publication or update dates when recency
matters. Cite every web-derived claim with a direct Markdown link. State when the
conclusion is an inference.

Search may support design work for storage engines, replication protocols,
concurrency strategies, and comparable systems. It does not replace local benchmarks,
source review, tests, or the crash-consistency and durability checks. Do not add
external dependencies based only on a search result; review license, maintenance, and
supply-chain impact first.

## Skills

Use a skill whenever one covers the area. Before working in that area, read the matching skill. The skills are authoritative for their areas; this file keeps only the working rules no skill covers.

| Area | Skill |
|------|-------|
| Concurrent code: async runtime, locking, lock-free structures, sharding, backpressure, remote WAL reads, benchmark reporting | `kvstore-concurrency` |

Only the skills in this table exist. Do not reference or search for other `kvstore-*` skills. Add a row here when a new skill is created.

Two rules bind every task:

1. Every storage-engine or WAL change must preserve crash consistency (see Correctness).
2. If a change makes a skill stale, tell the user which skill and why.

## Reference Paper

- The main agent always reads the full Tidehunter paper at the start of a task and keeps it in context until the task ends: A. Chursin et al., "Tidehunter: Large-Value Storage With Minimal Data Relocation", arXiv:2602.01873v2.
- Read the whole paper. Do not use an abstract, search snippets, or excerpts.
- Load it with the qmd MCP. If qmd does not have it, load it with the arXiv MCP. Use the full-document read function of the tool.
- If neither tool is available, or the paper loads incomplete, stop and tell the user. Do not work from memory.
- If code or a design choice contradicts the paper, report it. Name the section.
- Sub-agents, including `rust-task-implementer`, do not read the paper. The main agent puts the parts they need into the assignment.

## Agents and Skill Use

- Use the `rust-task-implementer` agent for Rust implementation tasks whenever it fits. Give it the assignment, the acceptance criteria, the approved design, and the files it owns.
- Keep design, architecture, and API decisions in the main session. The agent escalates these decisions. Decide them and send the answer back. Do not let the agent choose them.
- Use a `kvstore-*` skill whenever one applies. Do not work from memory when a skill covers the area.
- Read the matching skill before you start or delegate. In the assignment, name each skill the agent must read, with its required references.
- Always use `kvstore-concurrency` for locking, sharding, lock-free code, queues, channels, async tasks, and backpressure. Pass its design order to the agent: share nothing, actor with bounded channels, lock-free structure, sharded locks, single lock.
- Do not delegate questions, Git actions, or one-file edits that cost less to do than to explain.
- Verify the agent's report against the files, builds, and tests before you report completion. Task Quality rules apply to delegated work. The agent does not commit or push.
