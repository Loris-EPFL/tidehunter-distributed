# Rust guidelines

Binding rules for Rust work in this repository. Scope: `tidehunter/src/distributed/`,
`benchmarks/kv-research/src/`, `tidehunter-fabric/src/`, and every new Rust file. The
native engine under `tidehunter/src/` outside `distributed/` is excluded except for
defect fixes, so it stays rebasable on upstream MystenLabs.

Precedence: `AGENTS.md`, then the `kvstore-concurrency` skill (an installed agent skill, not a file in this repository), then this document. If a
rule here conflicts with `AGENTS.md` or the skill, the higher document wins and this
document is wrong. Toolchain: Rust 1.90, edition 2024. A named lint is enforced only
where DEC-5 lists it as landed (stage 1 via R6, stage 2 via R7); a rule with no lint, or
with a lint not yet landed, is enforced in review.

Rule IDs are stable. Reviews cite the ID, for example `OWN-3`. Each rule gives one
sentence, an example only where it removes ambiguity, the enforcing Clippy lint when one
exists, and a source link.

## Ownership and borrowing

- **OWN-1** Take `&T`, `&[T]`, or `&str` when the callee does not keep the value.
  `fn hash(key: &[u8])`, not `fn hash(key: Vec<u8>)`. Lint: `clippy::needless_pass_by_value`.
  [Book 4.2](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html)
- **OWN-2** Move a value into the callee when ownership transfers; do not clone to satisfy the borrow checker.
  [Effective Rust, references](https://www.lurklurk.org/effective-rust/references.html)
- **OWN-3** Return a borrow (`&str`, `&[u8]`, `impl Iterator + '_`) or `Cow` when the caller only reads; return owned data when the caller outlives the source.
  [C-CALLER-CONTROL](https://rust-lang.github.io/api-guidelines/flexibility.html#c-caller-control)
- **OWN-4** Respect exclusive-or-shared borrowing; do not add `RefCell` or a lock to bypass a real aliasing conflict.
  [Book 15.5](https://doc.rust-lang.org/book/ch15-05-interior-mutability.html)
- **OWN-5** Name a lifetime only when elision cannot express the relation, and name it for the data it ties.
  [Book 10.3](https://doc.rust-lang.org/book/ch10-03-lifetime-syntax.html)
- **OWN-6** Take `&mut T` only when mutation is part of the contract.
  Lint: `clippy::needless_pass_by_ref_mut`.
  [Book 4.2](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html)
- **OWN-7** Never return a reference into data owned by a lock guard or temporary; return owned data or a documented guard.
  [std::sync::Mutex](https://doc.rust-lang.org/std/sync/struct.Mutex.html)
- **OWN-8** Use `std::mem::take` or `std::mem::replace` to move out of `&mut T` instead of cloning.
  [std::mem::take](https://doc.rust-lang.org/std/mem/fn.take.html)
- **OWN-9** Use slices in signatures: `&[u8]`, not `&Vec<u8>`; `&str`, not `&String`.
  Lint: `clippy::ptr_arg`.
  [Book 4.3](https://doc.rust-lang.org/book/ch04-03-slices.html)
- **OWN-10** Do not hold a borrow across a call that can mutate the owner; restructure the code or pass an ID.
  [Book 4.2](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html)

## Cloning and allocation

- **CLN-1** Every `.clone()` on the put, get, replay, or publication path needs a stated ownership reason; clone an `Arc` explicitly as `Arc::clone(&x)`.
  Lint: `clippy::clone_on_ref_ptr`.
  [Clippy clone_on_ref_ptr](https://rust-lang.github.io/rust-clippy/master/index.html#clone_on_ref_ptr)
- **CLN-2** Remove a clone whose value is dropped without further use.
  Lint: `clippy::redundant_clone` (nursery).
  [Clippy redundant_clone](https://rust-lang.github.io/rust-clippy/master/index.html#redundant_clone)
- **CLN-3** Move `Bytes` or a slice through the write and read paths; do not copy a payload into a new `Vec`.
  [Perf Book, heap allocations](https://nnethercote.github.io/perf-book/heap-allocations.html)
- **CLN-4** Preallocate with `Vec::with_capacity` or `reserve` when the size is known; grow containers geometrically.
  [Perf Book, heap allocations](https://nnethercote.github.io/perf-book/heap-allocations.html)
- **CLN-5** Reuse a buffer or pool on the hot path; no per-request allocation when a reusable buffer exists. A caller-owned `&mut Vec<u8>` or `&mut [u8]` that the callee fills is the accepted form and wins over API-10; say so in a comment at the function.
  [AGENTS.md, Performance](../AGENTS.md)
- **CLN-6** Keep small, short-lived values on the stack; use `Box` only for indirection or a large enum variant.
  Lint: `clippy::large_enum_variant`.
  [Perf Book, type sizes](https://nnethercote.github.io/perf-book/type-sizes.html)
- **CLN-7** Use `Cow<'_, T>` when a value is sometimes borrowed and sometimes owned; do not allocate to normalize it.
  [std::borrow::Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)
- **CLN-8** Do not `collect()` into a `Vec` that is only iterated again; return `impl Iterator`.
  Lint: `clippy::needless_collect`.
  [Perf Book, iterators](https://nnethercote.github.io/perf-book/iterators.html)
- **CLN-9** Avoid `format!` and temporary `String`s on the hot path; write into a reused buffer.
  [Perf Book, heap allocations](https://nnethercote.github.io/perf-book/heap-allocations.html)
- **CLN-10** Use `Box<dyn Trait>` only at a boundary, never per message or per request.
  Lint: `clippy::borrowed_box`.
  [Perf Book, inlining](https://nnethercote.github.io/perf-book/inlining.html)
- **CLN-11** Use `dst.clone_from(&src)` instead of `dst = src.clone()` when `dst` already owns a buffer that can be reused.
  Lint: `clippy::assigning_clones`.
  [Perf Book, heap allocations](https://nnethercote.github.io/perf-book/heap-allocations.html)
- **CLN-12** Use `Arc::make_mut` or `Rc::make_mut` for clone-on-write of shared state; it clones only when the reference count is above one.
  [Perf Book, standard library types](https://nnethercote.github.io/perf-book/standard-library-types.html)

## Error handling and Result/Option

- **ERR-1** Return `Result<T, E>` for recoverable failure; reserve panic for a programmer error or corrupt on-disk state.
  [Book 9.2](https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html)
- **ERR-2** Never call `unwrap` or `expect` on a fallible production path; propagate with `?` or return a typed error.
  Lints: `clippy::unwrap_used`, `clippy::expect_used`.
  [Clippy unwrap_used](https://rust-lang.github.io/rust-clippy/master/index.html#unwrap_used)
- **ERR-3** Use `expect` only when an earlier check proves the invariant, and annotate it with `#[allow(clippy::expect_used, reason = "...")]` on the smallest item or statement that holds it (not on a whole function with other code). Prefer `let ... else` or `if let` (ERR-12) when the check and the use are in the same function.
  [Clippy expect_used](https://rust-lang.github.io/rust-clippy/master/index.html#expect_used)
- **ERR-4** Each module defines a typed error enum; do not return `String` or `&'static str` as an error (DEC-2 exempts `benchmarks/kv-research`).
  [C-GOOD-ERR](https://rust-lang.github.io/api-guidelines/interoperability.html#c-good-err)
- **ERR-5** Implement `Display` and `std::error::Error`; expose a wrapped cause through `source()`, not twice in `Display`.
  [std::error::Error](https://doc.rust-lang.org/std/error/trait.Error.html)
- **ERR-6** Convert with `?` and `From`; do not hand-write a `match` that only returns `Err(e)`.
  Lint: `clippy::question_mark`.
  [Book 9.2](https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html)
- **ERR-7** Do not ignore a `Result`; bind it, add context, or return it.
  Lints: `unused_must_use` (rustc), `clippy::let_underscore_must_use`.
  [std::result](https://doc.rust-lang.org/std/result/enum.Result.html)
- **ERR-8** Mark an API `#[must_use]` when dropping its value is a bug.
  Lint: `clippy::must_use_candidate`.
  [must_use attribute](https://doc.rust-lang.org/reference/attributes/diagnostics.html#the-must_use-attribute)
- **ERR-9** Use `Option` for absence and `Result` for failure with a reason; do not use a sentinel value.
  [C-CUSTOM-TYPE](https://rust-lang.github.io/api-guidelines/type-safety.html#c-custom-type)
- **ERR-10** Do not use `Box<dyn Error>` in library code; it erases the typed error and allocates. Binaries, tests, and the benchmark-driver crate `benchmarks/kv-research` may use it (DEC-2).
  [Effective Rust, errors](https://www.lurklurk.org/effective-rust/errors.html)
- **ERR-11** A panic message names the broken invariant; never panic on user input. A helper that wraps `expect` or `unwrap` for many callers carries `#[track_caller]` so the panic names the call site.
  [Book 9.3](https://doc.rust-lang.org/book/ch09-03-to-panic-or-not-to-panic.html),
  [track_caller](https://doc.rust-lang.org/reference/attributes/codegen.html#the-track_caller-attribute)
- **ERR-12** Use `let ... else` for the early-return shape instead of a nested `match`.
  Lint: `clippy::manual_let_else`.
  [Book 6.1](https://doc.rust-lang.org/book/ch06-01-defining-an-enum.html)
- **ERR-13** Build an expensive error value lazily: use `ok_or_else` and `map_or_else`, not `ok_or` or `map_or` with a function call.
  Lint: `clippy::or_fun_call`.
  [Perf Book, standard library types](https://nnethercote.github.io/perf-book/standard-library-types.html)

## Types, newtypes, and enums

- **TYP-1** Newtype every identifier whose bare integer can be confused with another: positions, generations, batch IDs, partitions, epochs.
  [C-NEWTYPE](https://rust-lang.github.io/api-guidelines/type-safety.html#c-newtype),
  [Effective Rust, newtype](https://www.lurklurk.org/effective-rust/newtype.html)
- **TYP-2** Use `NonZero*` when zero is invalid.
  [std::num::NonZero](https://doc.rust-lang.org/std/num/struct.NonZero.html)
- **TYP-3** Use an enum instead of a `bool` flag, a magic integer, or a string for a mode or outcome.
  [C-CUSTOM-TYPE](https://rust-lang.github.io/api-guidelines/type-safety.html#c-custom-type)
- **TYP-4** Derive `Debug`, `Clone`, `Copy` (only when valid), `Eq`, `PartialEq`, `Hash`, and `Ord` when the semantics allow.
  [C-COMMON-TRAITS](https://rust-lang.github.io/api-guidelines/interoperability.html#c-common-traits)
- **TYP-5** Keep struct fields private and construct through a checked constructor or builder.
  [C-STRUCT-PRIVATE](https://rust-lang.github.io/api-guidelines/future-proofing.html#c-struct-private)
- **TYP-6** Validate arguments at the boundary and return a typed error; do not accept a value the type should exclude.
  [C-VALIDATE](https://rust-lang.github.io/api-guidelines/dependability.html#c-validate)
- **TYP-7** Choose `u32`/`u64` deliberately for disk and wire widths; use checked conversions, never a truncating `as`.
  Lint: `clippy::cast_possible_truncation`.
  [Rust Reference, numeric types](https://doc.rust-lang.org/reference/types/numeric.html)
- **TYP-8** Model states with an enum so an illegal state cannot be constructed.
  [Effective Rust, use types](https://www.lurklurk.org/effective-rust/use-types.html)
- **TYP-9** Do not derive `Default` when the default value is invalid; absence is `Option`.
  [C-COMMON-TRAITS](https://rust-lang.github.io/api-guidelines/interoperability.html#c-common-traits)
- **TYP-10** Mark a public enum `#[non_exhaustive]` when a new variant must not break downstream matches.
  [non_exhaustive attribute](https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute)
- **TYP-11** Put `#[repr(transparent)]` on a newtype whose inner layout must stay the same, for example a newtype that crosses an FFI or on-disk layout boundary.
  [Effective Rust, Item 6](https://effective-rust.com/newtype.html)

## Traits and generics

- **TRT-1** Default to static dispatch (`impl Trait` or a generic) on hot paths; use `dyn Trait` only at a boundary or for a heterogeneous collection.
  [C-GENERIC](https://rust-lang.github.io/api-guidelines/flexibility.html#c-generic),
  [Effective Rust, generics](https://www.lurklurk.org/effective-rust/generics.html)
- **TRT-2** Do not put `Box<dyn Trait>` or `&dyn Trait` in a per-message or per-request structure on the put, get, replay, or publication path.
  [C-OBJECT](https://rust-lang.github.io/api-guidelines/flexibility.html#c-object)
- **TRT-3** Implement `From`, `TryFrom`, or `AsRef` instead of an ad-hoc conversion method when the standard trait fits.
  [C-CONV-TRAITS](https://rust-lang.github.io/api-guidelines/interoperability.html#c-conv-traits)
- **TRT-4** Do not add a blanket impl that can overlap a downstream impl; prefer a concrete impl.
  [C-SEALED](https://rust-lang.github.io/api-guidelines/future-proofing.html#c-sealed)
- **TRT-5** Seal a trait that external crates must not implement.
  [C-SEALED](https://rust-lang.github.io/api-guidelines/future-proofing.html#c-sealed)
- **TRT-6** Put a trait bound on the function or impl that needs it, not on the struct.
  [C-STRUCT-BOUNDS](https://rust-lang.github.io/api-guidelines/future-proofing.html#c-struct-bounds)
- **TRT-7** A trait method that can fail returns `Result` and documents the error; it does not panic.
  [C-FAILURE](https://rust-lang.github.io/api-guidelines/documentation.html#c-failure)
- **TRT-8** Use `impl Trait` in argument position when the concrete type is not otherwise named.
  [C-GENERIC](https://rust-lang.github.io/api-guidelines/flexibility.html#c-generic)
- **TRT-9** Do not erase a statically known type behind `dyn Trait`; it costs a vtable and blocks inlining.
  [Perf Book, inlining](https://nnethercote.github.io/perf-book/inlining.html)

## API design and visibility

- **API-1** Default to the narrowest visibility: private, then `pub(crate)`, then `pub`; widen only for a real caller.
  [C-STRUCT-PRIVATE](https://rust-lang.github.io/api-guidelines/future-proofing.html#c-struct-private)
- **API-2** Do not expose an internal type in a public signature; wrap it or re-export it deliberately.
  [C-NEWTYPE-HIDE](https://rust-lang.github.io/api-guidelines/future-proofing.html#c-newtype-hide)
- **API-3** Follow `as_` (cheap borrow), `to_` (copy/format), and `into_` (consume) conversion naming.
  [C-CONV](https://rust-lang.github.io/api-guidelines/naming.html#c-conv)
- **API-4** Name a getter after the field, with no `get_` prefix.
  [C-GETTER](https://rust-lang.github.io/api-guidelines/naming.html#c-getter)
- **API-5** Name iterator-producing methods `iter`, `iter_mut`, and `into_iter`.
  [C-ITER](https://rust-lang.github.io/api-guidelines/naming.html#c-iter)
- **API-6** Document `# Errors`, `# Panics`, and `# Safety` on every public item where they apply.
  Lints: `clippy::missing_errors_doc`, `clippy::missing_panics_doc`, `clippy::missing_safety_doc`.
  [C-FAILURE](https://rust-lang.github.io/api-guidelines/documentation.html#c-failure)
- **API-7** Every public item has a rustdoc example where practical.
  [C-EXAMPLE](https://rust-lang.github.io/api-guidelines/documentation.html#c-example)
- **API-8** Doc examples use `?`, not `unwrap`.
  [C-QUESTION-MARK](https://rust-lang.github.io/api-guidelines/documentation.html#c-question-mark)
- **API-9** A function with a clear receiver is a method, not a free function.
  [C-METHOD](https://rust-lang.github.io/api-guidelines/predictability.html#c-method)
- **API-10** Do not use out-parameters; return a tuple or a named struct. Exception: a caller-owned reusable buffer on a hot path (CLN-5).
  [C-NO-OUT](https://rust-lang.github.io/api-guidelines/predictability.html#c-no-out)
- **API-11** Constructors are associated functions named `new` or `open`, returning `Result` when they can fail.
  [C-CTOR](https://rust-lang.github.io/api-guidelines/predictability.html#c-ctor)
- **API-12** Implement `Deref` only for a smart pointer.
  [C-DEREF](https://rust-lang.github.io/api-guidelines/predictability.html#c-deref)
- **API-13** Feature names describe a capability, with no placeholder word.
  [C-FEATURE](https://rust-lang.github.io/api-guidelines/naming.html#c-feature)

## Iterators and loops

- **ITR-1** Prefer an iterator chain when the loop only maps, filters, or collects.
  [Effective Rust, iterators](https://www.lurklurk.org/effective-rust/iterators.html)
- **ITR-2** Keep an index loop when the body needs the index, several collections, early exit with cleanup, or `?`.
  [Book 13.2](https://doc.rust-lang.org/book/ch13-02-iterators.html)
- **ITR-3** Do not collect only to iterate again.
  [Perf Book, iterators](https://nnethercote.github.io/perf-book/iterators.html)
- **ITR-4** An index loop on a hot path must not repeat a bounds check; iterate, or take the slice once before the loop.
  [Perf Book, bounds checks](https://nnethercote.github.io/perf-book/bounds-checks.html)
- **ITR-5** Use `chunks_exact` when the chunk size divides the slice, and handle the remainder explicitly.
  [Perf Book, iterators](https://nnethercote.github.io/perf-book/iterators.html)
- **ITR-6** Use `iter().copied()` for small `Copy` items when a measurement shows it helps.
  [Perf Book, iterators](https://nnethercote.github.io/perf-book/iterators.html)
- **ITR-7** Avoid `chain` and `filter().map()` on a hot iterator; use `filter_map` or one loop.
  [Perf Book, iterators](https://nnethercote.github.io/perf-book/iterators.html)
- **ITR-8** Return `impl Iterator<Item = T> + '_` instead of a `Vec` when the caller only consumes it.
  [Perf Book, iterators](https://nnethercote.github.io/perf-book/iterators.html)
- **ITR-9** Give every loop a fixed upper bound, or document and assert its non-termination contract.
  [AGENTS.md, Correctness](../AGENTS.md)
- **ITR-10** Give a custom `Iterator` that is collected on a hot path an accurate `size_hint` (and `ExactSizeIterator::len` when exact), so `collect` allocates once.
  [Perf Book, iterators](https://nnethercote.github.io/perf-book/iterators.html)

## Threads and standard concurrency

- **CON-1** Follow the `kvstore-concurrency` design order: share nothing, actor with a bounded channel, lock-free, sharded locks, single lock. This order is binding.
  Source: `kvstore-concurrency` skill (outside this repository).
- **CON-2** Never put one global `Mutex` or `RwLock` on a hot path; no I/O, allocation, callback, or `.await` inside a lock.
  [Book 16.3](https://doc.rust-lang.org/book/ch16-03-shared-state.html)
- **CON-3** Every channel is bounded; document its owner, capacity, full-queue policy, and shutdown.
  Source: `kvstore-concurrency` skill (outside this repository).
- **CON-4** Write the lock order at the type, and lock shards in ascending index order.
  Source: `kvstore-concurrency` skill (outside this repository).
- **CON-5** Use `std::thread::scope` when threads borrow stack data; join every handle.
  [std::thread::scope](https://doc.rust-lang.org/std/thread/fn.scope.html)
- **CON-6** State the `Ordering` and the reason for every atomic; `Acquire`/`Release` for handoff, `Relaxed` for independent counters, `SeqCst` only with a stated reason.
  [std Ordering](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html)
- **CON-7** Use `Mutex<X>` only when an atomic cannot do the job.
  Lint: `clippy::mutex_atomic`.
  [Clippy mutex_atomic](https://rust-lang.github.io/rust-clippy/master/index.html#mutex_atomic)
- **CON-8** Never add `unsafe impl Send` or `Sync`; make the type `Send + Sync`, or keep it thread-local.
  Lint: `clippy::arc_with_non_send_sync`.
  [Book 16.4](https://doc.rust-lang.org/book/ch16-04-extensible-concurrency-sync-and-send.html)
- **CON-9** Treat lock poisoning as fatal in server code; do not silently recover.
  [std::sync::Mutex](https://doc.rust-lang.org/std/sync/struct.Mutex.html)
- **CON-10** Give every spawned thread or task a shutdown path and join it; no detached work.
  Source: `kvstore-concurrency` skill (outside this repository).
- **CON-11** Report p50, p99, and p999 latency and per-stage queue wait; no speedup claim without benchmark numbers.
  Source: `kvstore-concurrency` skill (outside this repository).
- **CON-12** Bound task creation with a fixed worker count or a semaphore; never spawn one task per request without a limit.
  Source: `kvstore-concurrency` skill (outside this repository).

## Async and tokio

- **ASY-1** Async is for I/O concurrency; move blocking I/O and long CPU work to `spawn_blocking` or a dedicated thread.
  [tokio::task::spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)
- **ASY-2** Do not hold a `std` mutex guard or a `RefCell` reference across `.await`.
  Lints: `clippy::await_holding_lock`, `clippy::await_holding_refcell_ref`.
  [Clippy await_holding_lock](https://rust-lang.github.io/rust-clippy/master/index.html#await_holding_lock)
- **ASY-3** Prefer an actor with a bounded `mpsc` channel over `Arc<Mutex<State>>` shared across tasks.
  [tokio shared state](https://tokio.rs/tokio/tutorial/shared-state)
- **ASY-4** A `select!` branch must be cancellation safe; if it is not, pin the future outside the loop or restructure.
  [tokio::select! cancellation safety](https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety)
- **ASY-5** Bound every async queue; either `send().await` with a safe caller, or `try_send` with an explicit `Overloaded` error.
  [tokio mpsc](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html)
- **ASY-6** Bound every remote call with a timeout and a deadline that passes down the call chain.
  Source: `kvstore-concurrency` skill (outside this repository).
- **ASY-7** Await or join every `JoinHandle`; handle a `JoinError` instead of discarding it.
  [tokio::task::spawn](https://docs.rs/tokio/latest/tokio/task/fn.spawn.html)
- **ASY-8** Do not mark a function `async` when it never awaits.
  Lint: `clippy::unused_async`.
  [Book 17.1](https://doc.rust-lang.org/book/ch17-01-futures-and-syntax.html)
- **ASY-9** An `async fn` in a trait needs a deliberate choice between native AFIT and a boxed future; do not box per call on a hot path.
  [Async Book](https://rust-lang.github.io/async-book/)
- **ASY-10** Make shutdown explicit: stop accepting, close senders, drain, join; every `select!` loop ends on a shutdown signal.
  [tokio graceful shutdown](https://tokio.rs/tokio/topics/shutdown)

## Unsafe

- **UNS-1** Prefer safe code; add `unsafe` only when a safe alternative cannot meet a measured requirement.
  [Nomicon](https://doc.rust-lang.org/nomicon/meet-safe-and-unsafe.html)
- **UNS-2** Every `unsafe` block has a `// SAFETY:` comment that states the invariant and why the block preserves it.
  Lint: `clippy::undocumented_unsafe_blocks`.
  [Clippy undocumented_unsafe_blocks](https://rust-lang.github.io/rust-clippy/master/index.html#undocumented_unsafe_blocks)
- **UNS-3** Put one unsafe operation in each block so each is independently justified.
  Lint: `clippy::multiple_unsafe_ops_per_block`.
  [Clippy multiple_unsafe_ops_per_block](https://rust-lang.github.io/rust-clippy/master/index.html#multiple_unsafe_ops_per_block)
- **UNS-4** Confine `unsafe` to the smallest scope and put a safe abstraction with a documented contract around it.
  [Nomicon, working with unsafe](https://doc.rust-lang.org/nomicon/working-with-unsafe.html)
- **UNS-5** Do not add a new unsafe abstraction, `unsafe impl`, or unsafe dependency without owner approval.
  Source: `kvstore-concurrency` skill (outside this repository).
- **UNS-6** Produce no undefined behavior: no data race, dangling pointer, aliasing violation, or invalid value.
  [Reference: behavior considered undefined](https://doc.rust-lang.org/reference/behavior-considered-undefined.html)
- **UNS-7** Use an explicit `unsafe {}` block inside an `unsafe fn` body.
  Lint: `unsafe_op_in_unsafe_fn` (rustc).
  [Rust Reference: unsafe](https://doc.rust-lang.org/std/keyword.unsafe.html)
- **UNS-8** Run Miri on new unsafe code when the crate supports it; state why when it cannot.
  [Miri](https://github.com/rust-lang/miri)
- **UNS-9** Do not remove a bounds check with `get_unchecked` unless a benchmark shows the check is hot.
  [Perf Book, bounds checks](https://nnethercote.github.io/perf-book/bounds-checks.html)
- **UNS-10** Do not use `transmute` or a raw-pointer dereference to extend a lifetime; bound the lifetime at the function boundary instead.
  Lint: `clippy::transmute_ptr_to_ref`.
  [Rustonomicon, unbounded lifetimes](https://doc.rust-lang.org/nomicon/unbounded-lifetimes.html)

## Documentation

- **DOC-1** Every module and public item has rustdoc; explain the contract and the reason, not the syntax.
  [C-CRATE-DOC](https://rust-lang.github.io/api-guidelines/documentation.html#c-crate-doc)
- **DOC-2** Document panics, errors, safety, and blocking behavior.
  [C-FAILURE](https://rust-lang.github.io/api-guidelines/documentation.html#c-failure)
- **DOC-3** A comment states an invariant or a reason; it does not restate the code.
  [AGENTS.md, Communication](../AGENTS.md)
- **DOC-4** Link the design document or source for a non-obvious protocol, format, or recovery decision.
  [C-LINK](https://rust-lang.github.io/api-guidelines/documentation.html#c-link)
- **DOC-5** Update the document when behavior changes, and delete the document for a removed design.
  [AGENTS.md, Code Changes](../AGENTS.md)
- **DOC-6** Use ASD-STE100 Simplified Technical English: short, direct, active sentences.
  [AGENTS.md, Communication](../AGENTS.md)
- **DOC-7** Every `#[allow(...)]` carries `reason = "..."`.
  Lint: `clippy::allow_attributes_without_reason`.
  [Clippy allow_attributes_without_reason](https://rust-lang.github.io/rust-clippy/master/index.html#allow_attributes_without_reason)

## Testing

- **TST-1** Every bug fix lands with a regression test that fails before the fix and passes after.
  [AGENTS.md, Task Quality](../AGENTS.md)
- **TST-2** A crash-consistency change tests every crash boundary it adds or moves.
  [AGENTS.md, Correctness](../AGENTS.md)
- **TST-3** Keep test bodies out of production files: end the file with only `#[cfg(test)] mod tests;` and put the tests in `foo/tests.rs`, or in `foo_tests.rs` declared with `#[path = "foo_tests.rs"]`. No inline `mod tests { ... }` body and no `#[test]` function in a production file.
  [Book 11.3](https://doc.rust-lang.org/book/ch11-03-test-organization.html), [Effective Rust, Item 30](https://effective-rust.com/testing.html)
- **TST-4** Test code may use `unwrap` and `expect`; set `allow-unwrap-in-tests` and `allow-expect-in-tests` in `clippy.toml`.
  [Clippy lint configuration](https://doc.rust-lang.org/clippy/lint_configuration.html#allow-unwrap-in-tests)
- **TST-5** A test name states the behavior and the condition it exercises.
  [Book 11.1](https://doc.rust-lang.org/book/ch11-01-writing-tests.html)
- **TST-6** Use seeded, deterministic inputs and print the seed on failure.
  Source: `kvstore-concurrency` skill (outside this repository).
- **TST-7** Do not weaken an assertion to make a flaky test pass; fix the cause.
  [AGENTS.md, Task Quality](../AGENTS.md)
- **TST-8** Keep a test file at or below 1,500 lines, the same cap as a production file.
  [Book 11.1](https://doc.rust-lang.org/book/ch11-01-writing-tests.html)
- **TST-9** A wait in a test follows observed progress (a counter, a handshake, a joined thread) and is capped by one named constant per test module with a single documented reason; a fixed sleep is allowed only as a floor after a progress signal. Do not relax a production validator or limit so a test can pass a larger cap.
  Source: extends TST-7 (fix the cause, do not weaken the check); added in revision r2 from the batch-2 review of the time-bound test patches.

## Performance verification

- **PRF-1** Measure before optimizing; profile to find the hot path before changing it.
  [Perf Book, profiling](https://nnethercote.github.io/perf-book/profiling.html)
- **PRF-2** A hot-path change (put, get, replay, publication) needs before/after numbers from the local live bench at c=1, 4, and 16, with 5 interleaved repeats.
  [Perf Book, benchmarking](https://nnethercote.github.io/perf-book/benchmarking.html)
- **PRF-3** Do not regress p50 or p99 by more than 3%; a larger change needs a written justification and owner approval.
  Source: project rule, owner decision of 2026-11-19 (NVMe-oF parity plan, not in this repository).
- **PRF-4** Report exact nearest-rank quantiles from raw samples, not log-bucket bounds.
  Source: project rule, owner decision of 2026-11-19 (NVMe-oF parity plan, not in this repository).
- **PRF-5** A latency row must not contain waiting: pass the c=1 pilot, keep p50 within the stated factor, keep throughput about linear, and scheduling about zero.
  Source: project rule, owner decision of 2026-11-19 (NVMe-oF parity plan, not in this repository).
- **PRF-6** Benchmark a release build with the project harness; never compare a dev build.
  [Perf Book, benchmarking](https://nnethercote.github.io/perf-book/benchmarking.html)
- **PRF-7** An allocation or cloning change on the hot path is a performance change and needs PRF-2 numbers.
  [Perf Book, heap allocations](https://nnethercote.github.io/perf-book/heap-allocations.html)
- **PRF-8** Do not add a benchmark tool when the project harness or criterion fits.
  [AGENTS.md, Tooling](../AGENTS.md)
- **PRF-9** When a micro-optimization is claimed but the benchmark is flat, inspect the machine code before keeping it.
  [Perf Book, machine code](https://nnethercote.github.io/perf-book/machine-code.html)
- **PRF-10** Move a rarely taken branch into a separate function marked `#[cold]` so the compiler favors the hot path; measure the effect.
  [Perf Book, inlining](https://nnethercote.github.io/perf-book/inlining.html)

## Rules from The Rust Programming Language, chapter by chapter

Rules derived from the book's chapters and appendices. The record of how the book was read is in the RustRules handoff report, not in this document.
A rule here adds to the topic sections above; where a book rule repeats one above, only
the topic rule is kept. Lint names marked "not enabled" exist but are not on the deny list.

### Ch 1 Getting Started

- **BK1-1** Commit `Cargo.lock`, use `cargo check` in the edit loop, and measure only `--release` artifacts.
  [Ch 1.3](https://doc.rust-lang.org/book/ch01-03-hello-cargo.html)
### Ch 2 Programming a Guessing Game

- **BK2-1** Pin dependency versions with SemVer specifiers and treat every `cargo update` as a reviewed change.
  [Ch 2](https://doc.rust-lang.org/book/ch02-00-guessing-game-tutorial.html)
### Ch 3 Common Programming Concepts

- **BK3-1** Default to immutable bindings and name shared literals as `const`.
  [Ch 3.1](https://doc.rust-lang.org/book/ch03-01-variables-and-mutability.html)
- **BK3-2** Release builds wrap integer overflow silently; use `checked_*`, `saturating_*`, or `wrapping_*` explicitly for offsets, sizes, and counters.
  Lint (not enabled): `clippy::arithmetic_side_effects`.
  [Ch 3.2](https://doc.rust-lang.org/book/ch03-02-data-types.html)
- **BK3-3** `a[i]` panics when out of range; use `get()` or an iterator for on-disk, network, or trace-derived indices.
  Lint (not enabled): `clippy::indexing_slicing`.
  [Ch 3.2](https://doc.rust-lang.org/book/ch03-02-data-types.html)
- **BK3-4** Prefer `for` over an index-driven `while` loop.
  [Ch 3.5](https://doc.rust-lang.org/book/ch03-05-control-flow.html)
### Ch 4 Understanding Ownership

- **BK4-1** Derive `Copy` only for small plain-data types; a heap-owning value moves or clones explicitly.
  [Ch 4.1](https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html)
- **BK4-2** Return a slice instead of a byte index, so the borrow checker ties the result to its buffer.
  [Ch 4.3](https://doc.rust-lang.org/book/ch04-03-slices.html)
### Ch 5 Using Structs

- **BK5-1** Use named fields or a newtype tuple struct instead of a bare tuple, and remember `..other` moves non-`Copy` fields out of the source.
  [Ch 5.1](https://doc.rust-lang.org/book/ch05-01-defining-structs.html)
- **BK5-2** Choose `&self`, `&mut self`, or `self` deliberately: the receiver states whether the method reads, mutates, or consumes.
  [Ch 5.3](https://doc.rust-lang.org/book/ch05-03-method-syntax.html)
### Ch 6 Enums and Pattern Matching

- **BK6-1** Do not use a `_` catch-all arm on an internal enum, so a new variant forces every handler to change.
  Lints (not enabled): `clippy::wildcard_enum_match_arm`, `clippy::match_wildcard_for_single_variants`.
  [Ch 6.2](https://doc.rust-lang.org/book/ch06-02-match.html)
### Ch 7 Packages, Crates, and Modules

- **BK7-1** Keep logic in `lib.rs` and keep `main.rs` thin; use `crate::` paths and `pub use` so the public shape is independent of the internal layout.
  [Ch 7.3](https://doc.rust-lang.org/book/ch07-03-paths-for-referring-to-an-item-in-the-module-tree.html)
- **BK7-2** Import a function through its parent module and a type by its full path; avoid glob imports outside tests and preludes.
  Lint: `clippy::wildcard_imports`.
  [Ch 7.4](https://doc.rust-lang.org/book/ch07-04-bringing-paths-into-scope-with-the-use-keyword.html)
### Ch 8 Common Collections

- **BK8-1** Treat on-disk keys and values as `&[u8]` or `Bytes`, never `String`; slicing a `String` off a char boundary panics.
  [Ch 8.2](https://doc.rust-lang.org/book/ch08-02-strings.html)
- **BK8-2** Use the `entry` API for insert-or-update; keep the default SipHash until a profile justifies another `BuildHasher`.
  Lint: `clippy::map_entry`.
  [Ch 8.3](https://doc.rust-lang.org/book/ch08-03-hash-maps.html)
### Ch 9 Error Handling

- **BK9-1** With `panic = "abort"` nothing unwinds, so no crash-consistency step may depend on `Drop` running after a panic.
  [Ch 9.1](https://doc.rust-lang.org/book/ch09-01-unrecoverable-errors-with-panic.html)
### Ch 10 Generics, Traits, and Lifetimes

- **BK10-1** `impl Trait` in return position names exactly one concrete type; use `Box<dyn Trait>` only when the type varies at run time.
  [Ch 10.2](https://doc.rust-lang.org/book/ch10-02-traits.html)
- **BK10-2** Never add `'static` only to silence a lifetime error; the error marks a real dangling or mismatched borrow.
  [Ch 10.3](https://doc.rust-lang.org/book/ch10-03-lifetime-syntax.html)
- **BK10-3** Generics are monomorphized at no run-time cost; watch binary size and compile time when a generic is instantiated many times.
  [Ch 10.1](https://doc.rust-lang.org/book/ch10-01-syntax.html)
### Ch 11 Automated Tests

- **BK11-1** Unit tests live in a sibling test module (`foo/tests.rs` or `foo_tests.rs`, declared with `#[cfg(test)] mod tests;`), never in an inline body; tests of the public API live in `tests/` with helpers in `tests/common/mod.rs`.
  [Ch 11.3](https://doc.rust-lang.org/book/ch11-03-test-organization.html)
- **BK11-2** Tests run in parallel; give each a unique temp path, and use `--test-threads=1` only for tests that share global state.
  [Ch 11.2](https://doc.rust-lang.org/book/ch11-02-running-tests.html)
- **BK11-3** Mark slow crash and recovery tests `#[ignore]` and run them explicitly; check a specific failure with `#[should_panic(expected = "...")]` or `assert!(result.is_err())`.
  [Ch 11.2](https://doc.rust-lang.org/book/ch11-02-running-tests.html)
- **BK11-4** Give each assertion a message that shows the values involved.
  [Ch 11.1](https://doc.rust-lang.org/book/ch11-01-writing-tests.html)
### Ch 12 An I/O Project

- **BK12-1** `main` parses arguments, builds configuration, calls `run`, and maps the error to a non-zero exit code; diagnostics go to stderr.
  [Ch 12.3](https://doc.rust-lang.org/book/ch12-03-improving-error-handling-and-modularity.html),
  [Ch 12.6](https://doc.rust-lang.org/book/ch12-06-writing-to-stderr-instead-of-stdout.html)
- **BK12-2** Use a `build() -> Result` constructor, not a `new` that can panic; `env::args()` panics on invalid Unicode, so use `args_os` for paths, and read environment variables once at startup.
  [Ch 12.1](https://doc.rust-lang.org/book/ch12-01-accepting-command-line-arguments.html)
### Ch 13 Closures and Iterators

- **BK13-1** Give a closure parameter the weakest bound that works (`FnOnce`, then `FnMut`, then `Fn`), and use `move` closures for data handed to spawned threads.
  [Ch 13.1](https://doc.rust-lang.org/book/ch13-01-closures.html)
- **BK13-2** Iterator adapters are lazy; always consume them, and take `impl Iterator<Item = String>` and move its items instead of indexing a slice and cloning.
  [Ch 13.2](https://doc.rust-lang.org/book/ch13-02-iterators.html),
  [Ch 13.3](https://doc.rust-lang.org/book/ch13-03-improving-our-io-project.html)
### Ch 14 More About Cargo and Crates.io

- **BK14-1** Set `opt-level`, `lto`, `codegen-units`, and `panic` in an explicit `[profile.release]` at the workspace root; Cargo ignores profile settings in member crates.
  [Ch 14.1](https://doc.rust-lang.org/book/ch14-01-release-profiles.html),
  [Perf Book, build configuration](https://nnethercote.github.io/perf-book/build-configuration.html)
- **BK14-2** Keep rustdoc examples compiling, because `cargo test` runs them.
  [Ch 14.2](https://doc.rust-lang.org/book/ch14-02-publishing-to-crates-io.html)
- **BK14-3** Split crates inside one workspace (one `Cargo.lock`, one `target/`) and declare each inter-crate dependency explicitly; helper `cargo-*` tools are separate binaries, never runtime dependencies.
  [Ch 14.3](https://doc.rust-lang.org/book/ch14-03-cargo-workspaces.html),
  [Ch 14.4](https://doc.rust-lang.org/book/ch14-04-installing-binaries.html)
- **BK14-4** Make Cargo features additive: enabling a feature must only add items, never remove or change behavior, and no feature may exclude another.
  [Effective Rust, Item 26](https://effective-rust.com/features.html)
### Ch 15 Smart Pointers

- **BK15-1** Use `Box<T>` to move a large value cheaply or to build a recursive type, not as a default wrapper.
  [Ch 15.1](https://doc.rust-lang.org/book/ch15-01-box.html)
- **BK15-2** Put cleanup in `Drop`, release early with `std::mem::drop(x)`, and never call `x.drop()`; a `Drop` impl must not fail or block unbounded.
  [Ch 15.3](https://doc.rust-lang.org/book/ch15-03-drop.html),
  [C-DTOR-FAIL](https://rust-lang.github.io/api-guidelines/dependability.html#c-dtor-fail)
- **BK15-3** `Rc` and `RefCell` are single-threaded; across threads use `Arc` and a lock or atomic, and keep `RefCell` off hot and concurrent paths because it adds run-time borrow tracking and panics on violation.
  [Ch 15.4](https://doc.rust-lang.org/book/ch15-04-rc.html),
  [Ch 15.5](https://doc.rust-lang.org/book/ch15-05-interior-mutability.html)
- **BK15-4** Make parent and owner back-references `Weak<T>`; a cycle of `Rc` or `Arc` leaks and the compiler does not report it.
  [Ch 15.6](https://doc.rust-lang.org/book/ch15-06-reference-cycles.html)
### Ch 16 Fearless Concurrency

- **BK16-1** Ownership moves through `mpsc` because `send` consumes the value; handle the `Result` of `send` and `recv` instead of calling `unwrap` on a production path.
  [Ch 16.2](https://doc.rust-lang.org/book/ch16-02-message-passing.html)
### Ch 17 Async and Await

- **BK17-1** Use threads for CPU-bound parallel work (compaction, checksums) and async for I/O-bound concurrency (network, many in-flight requests).
  [Ch 17.6](https://doc.rust-lang.org/book/ch17-06-futures-tasks-threads.html)
- **BK17-2** `.await` points are the only yield points; keep blocking and long CPU work out of async blocks, and call `yield_now` only when a measurement justifies it.
  [Ch 17.3](https://doc.rust-lang.org/book/ch17-03-more-futures.html)
- **BK17-3** A future is a lazy state machine; give every future an owner and an `.await`, and build a runtime in `main` instead of an `async fn main`.
  [Ch 17.1](https://doc.rust-lang.org/book/ch17-01-futures-and-syntax.html)
- **BK17-4** Close a channel by dropping every sender (use `async move`) so receive loops end.
  [Ch 17.2](https://doc.rust-lang.org/book/ch17-02-concurrency-with-async.html)
- **BK17-5** Build a timeout by racing the work against a timer with `select`; `select` polls its arguments in order, so state the fairness you rely on.
  [Ch 17.3](https://doc.rust-lang.org/book/ch17-03-more-futures.html)
- **BK17-6** Model a sequence of async events with `Stream` and `StreamExt`, not a hand-written polling loop.
  [Ch 17.4](https://doc.rust-lang.org/book/ch17-04-streams.html)
- **BK17-7** Futures of different types in one `Vec` need `Pin<Box<dyn Future>>` or `pin!`, because async state machines can be self-referential.
  [Ch 17.5](https://doc.rust-lang.org/book/ch17-05-traits-for-async.html)
### Ch 18 Object-Oriented Programming Features

- **BK18-1** Keep fields private and expose invariant-preserving methods, so cached or derived fields (counters, checksums, lengths) cannot go out of sync.
  [Ch 18.1](https://doc.rust-lang.org/book/ch18-01-what-is-oo.html)
- **BK18-2** Encode a lifecycle as distinct types whose transitions consume `self`; do not use the `Option<Box<dyn State>>` take-and-replace pattern on a hot path.
  [Ch 18.3](https://doc.rust-lang.org/book/ch18-03-oo-design-patterns.html)
### Ch 19 Patterns and Matching

- **BK19-1** Use `let ... else` or `if let` for refutable patterns, and plain `let` only for irrefutable ones.
  [Ch 19.2](https://doc.rust-lang.org/book/ch19-02-refutability.html)
- **BK19-2** A match guard defeats the exhaustiveness check; avoid guards on state and opcode enums.
  [Ch 19.3](https://doc.rust-lang.org/book/ch19-03-pattern-syntax.html)
- **BK19-3** Destructure with `..` and bind with `@` instead of re-reading fields; use `_` rather than `_x` when the value must not move.
  [Ch 19.3](https://doc.rust-lang.org/book/ch19-03-pattern-syntax.html)
### Ch 20 Advanced Features

- **BK20-1** Reach a shared mutable static only through `&raw const` or `&raw mut` inside `unsafe`, and prefer an atomic, `Mutex`, or `OnceLock`; `static mut` is banned by `kvstore-concurrency`.
  [Ch 20.1](https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html)
- **BK20-2** Build a slice only from an allocation with known provenance, length, and alignment, never from an integer-cast pointer.
  [Ch 20.1](https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html)
- **BK20-3** Use an associated type, not a generic parameter, when a type has one natural implementation; use fully qualified syntax (`<T as Trait>::f`) when method names collide.
  [Ch 20.2](https://doc.rust-lang.org/book/ch20-02-advanced-traits.html)
- **BK20-4** Wrap a foreign type in a newtype to implement a foreign trait; a `type` alias gives no type distinction.
  [Ch 20.2](https://doc.rust-lang.org/book/ch20-02-advanced-traits.html),
  [Ch 20.3](https://doc.rust-lang.org/book/ch20-03-advanced-types.html)
- **BK20-5** Put `T: ?Sized` on a generic that takes `&T` when `str`, slices, or `dyn Trait` must be accepted.
  [Ch 20.3](https://doc.rust-lang.org/book/ch20-03-advanced-types.html)
- **BK20-6** Take `impl Fn*` or a generic `F: Fn*` for callbacks so they inline; use `fn` pointers only at an FFI boundary, and `Box<dyn Fn>` only when different closure types share one collection.
  [Ch 20.4](https://doc.rust-lang.org/book/ch20-04-advanced-functions-and-closures.html)
- **BK20-7** Prefer a function or generic to a macro; use `macro_rules!` only for token-level repetition, and give a proc-macro `expect` a message that names the failure.
  [Ch 20.5](https://doc.rust-lang.org/book/ch20-05-macros.html)
### Ch 21 Multithreaded Web Server

- **BK21-1** The book's server unwraps every I/O result; a storage server propagates `io::Error` for every accept, read, and write, and wraps sockets in `BufReader`/`BufWriter` to batch syscalls.
  [Ch 21.1](https://doc.rust-lang.org/book/ch21-01-single-threaded.html)
- **BK21-2** Do not build a job queue from `Arc<Mutex<Receiver>>` plus `Box<dyn FnOnce>` on a hot path: it costs one lock and one allocation per job.
  [Ch 21.2](https://doc.rust-lang.org/book/ch21-02-multithreaded.html)
- **BK21-3** Never hold a `MutexGuard` temporary across blocking work; bind the received job with `let` so the guard drops first (the `while let Ok(job) = rx.lock()...` trap).
  Lint (not enabled): `clippy::significant_drop_in_scrutinee`.
  [Ch 21.2](https://doc.rust-lang.org/book/ch21-02-multithreaded.html)
- **BK21-4** Spawn with `thread::Builder::spawn` and handle its `Result`.
  [Ch 21.2](https://doc.rust-lang.org/book/ch21-02-multithreaded.html)
- **BK21-5** Shut down by dropping the sender, then joining every worker, in `Drop`; never `unwrap()` a `join` inside `Drop`, because a second panic aborts the process.
  [Ch 21.3](https://doc.rust-lang.org/book/ch21-03-graceful-shutdown-and-cleanup.html)
### Appendices A to G

- **BK-A** Do not use reserved or edition-dependent words (`try`, `gen`, `async`, `dyn`) as names; use `r#` only for older-edition interop.
  [Appendix A](https://doc.rust-lang.org/book/appendix-01-keywords.html)
- **BK-B** Implement an operator trait (`Add`, `Index`, `Deref`) only with the expected semantics, and never let an operator hide I/O or allocation.
  [Appendix B](https://doc.rust-lang.org/book/appendix-02-operators.html)
- **BK-C** Derive `Copy` only for small plain-bits types, check that derived `Ord` follows field order and field order is the intended sort order for on-disk keys, and do not derive `Default` or `Clone` on a handle or buffer that must not be duplicated.
  [Appendix C](https://doc.rust-lang.org/book/appendix-03-derivable-traits.html)
- **BK-D** CI gates on `cargo fmt --check` and `cargo clippy -- -D warnings`.
  [Appendix D](https://doc.rust-lang.org/book/appendix-04-useful-development-tools.html)
- **BK-E** Set `edition = "2024"` explicitly in every crate manifest; a missing key means edition 2015.
  [Appendix E](https://doc.rust-lang.org/book/appendix-05-editions.html)
- **BK-G** Pin the stable toolchain in `rust-toolchain.toml`, test beta in CI, and confine nightly to named tools such as Miri.
  [Appendix G](https://doc.rust-lang.org/book/appendix-07-nightly-rust.html)

## Project-specific decisions

These decisions are binding for this repository. They add to the rules above; they do
not replace `AGENTS.md` or the `kvstore-concurrency` skill.

- **DEC-1** Each module defines its own typed error enum and implements `Display` and
  `std::error::Error` by hand. Do not add `thiserror` or `anyhow`; the current error
  enums (`GroupError`, `CodecError`, `StorageError`, `FrontierError`) are the model.
- **DEC-2** No `Box<dyn Error>` and no `anyhow::Error` in `tidehunter` (the `distributed/`
  tree) or `tidehunter-fabric`. One exception: a `source` slot holding
  `Arc<dyn Error + Send + Sync>` in an error type that wraps components with no common
  error type (today `LocalDbError` and `TcpError`); `Error::source` itself returns
  `dyn Error`, and the `Arc` keeps the error `Clone`. Document the slot at the type.
  `benchmarks/kv-research` is benchmark-driver class: its `BackendResult`
  (`Box<dyn Error + Send + Sync>`) and string errors stay, and R7 does not replace them
  with an enum. Binaries and tests may use `Box<dyn Error>` at the top level.
- **DEC-3** Newtype every position, generation, epoch, partition, batch identity, and
  mutation version where a bare integer could be mixed up. `distributed/types.rs`
  (`BatchId`, `BatchVersion`, `MutationVersion`, `PhysicalValueAddress`) is the model.
- **DEC-4** The native engine (`tidehunter/src/` outside `distributed/`) is out of scope
  for idiom and lint changes. Change it only to fix a defect, and keep the change
  minimal for rebasing on upstream MystenLabs.
- **DEC-5** The lint scaffold has two stages. Stage 1 (R6, zero-hit lints, enforced once
  R6 patch 17 lands): `deny` for `unwrap_used`, `expect_used`,
  `undocumented_unsafe_blocks`, `multiple_unsafe_ops_per_block`, `mutex_atomic`,
  `await_holding_lock`, `await_holding_refcell_ref`, `arc_with_non_send_sync`, and
  `unsafe_op_in_unsafe_fn`, plus a workspace `clippy.toml` that allows unwrap and expect
  in tests. Stage 2 (R7, deferred by the owner decision "option B"):
  `#![warn(clippy::pedantic)]`, `redundant_clone`, and `clone_on_ref_ptr`. Until stage 2
  lands, review enforces CLN-1 and CLN-2. A pedantic allow is item-level with a
  `reason`; only `module_name_repetitions` may be allowed module-wide.
- **DEC-6** Production and test files target 1,000 lines. Once R6 patch 17 lands,
  `.github/scripts/checks.py file-length` enforces a ratchet: a file listed in
  `.github/scripts/file-length-baseline.txt` may not grow, and any other file in the
  audited trees stays at or below 1,500 lines. Split a baseline file; never raise its count.
- **DEC-7** A hot-path change needs the local live bench at c=1, 4, and 16 with the
  no-waiting rule, 5 interleaved repeats, and p50/p99 within 3% or a written
  justification. Run it in a quiet window: load below 5, no other cargo, rustc, or dd.
- **DEC-8** Concurrency follows the `kvstore-concurrency` design order. A channel is
  bounded and documented; a lock order is written at the type; an atomic carries its
  ordering and reason.
- **DEC-9** A change to the WAL, replay, checkpoint, fencing, or publication path needs
  a crash-cut test at every new or moved boundary, in addition to the normal gate.
- **DEC-10** A request path never panics on user input. Programmer errors and corrupt
  on-disk state are fatal. A startup configuration contract may panic only with a
  named, documented message (for example the `TH_SYNC_WRITES` mismatch).
- **DEC-11** Default visibility in `distributed/` is private or `pub(crate)`. `pub` is
  for the Sui adapter boundary (`distributed/sui.rs`) and for the kv-research backend
  API, not for internal storage types.
- **DEC-12** Use `Bytes` for payloads on the wire and disk paths; do not copy them into
  `Vec<u8>`. Use `&[u8]` for borrowed views.
- **DEC-13** No new dependency without the `AGENTS.md` dependency review. Prefer the
  standard library and an existing crate.
- **DEC-14** The toolchain is Rust 1.90, edition 2024. A toolchain or MSRV change needs
  owner approval.
- **DEC-15** Record every benchmark result and its environment under the project
  evidence directory `checkpoint-work/` (not tracked in this repository). A performance
  claim without recorded evidence is not accepted.
- **DEC-16** Every Rust patch gets a guideline review from the Rust review contact
  (`RustReview`, successor of `RustRules`) before it is exported. Fix each rule-numbered
  finding, or state the justification in the patch report.
