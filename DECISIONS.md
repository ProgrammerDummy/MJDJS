
# MJDJS — Design Decisions Log


This is a running log of every place the implementation deviates from the
original 16-week roadmap, and why. The goal is to make divergence
*deliberate and traceable* rather than silent — so later weeks that assume
specific earlier decisions don't quietly break, and so the "Design decisions"
section required in the Week 16 README can be written from this log instead
of reconstructed from memory.


**Format per entry:** what the roadmap suggested, what was actually built,
and the reasoning — so a future reader (including future-me) can tell the
difference between "I didn't understand the plan" and "I understood it and
made a different, defensible call."


---


## Week 3 — Scheduler Concurrency Design


### 1. Job result reporting: shared `mpsc` instead of per-job `oneshot`


- **Roadmap suggested:** "Per-job result → oneshot (single use)"
- **What was built:** A single shared `mpsc::Sender<JobResult>`, cloned into
 every spawned job task, all reporting back to one long-lived
 `mpsc::Receiver` owned by the scheduler's dispatch loop.
- **Why:** `oneshot` is single-producer/single-consumer by design — exactly
 right for a single caller awaiting a single job's result directly. But the
 scheduler's `run()` loop needs to aggregate results from *many concurrently
 running jobs* into one continuously-polled stream over its entire
 lifetime — that's structurally what `mpsc` (multi-producer, single
 consumer) is for. `oneshot` would only make sense if some external caller
 (not yet built — `main.rs` is still a stub) wanted a direct, one-off
 answer for a specific job it submitted, separate from the scheduler's
 internal aggregation. That's a real, additive use case for `oneshot`
 later, not a replacement for the current mechanism.


### 2. Shutdown signaling: `CancellationToken` instead of `broadcast`


- **Roadmap suggested (Week 3, item 6):** "Shutdown signal → broadcast
 (one-to-many)"
- **What was built:** `tokio_util::sync::CancellationToken`, passed into
 `Scheduler::run()`.
- **Why:** Not actually a divergence — the roadmap's own very next item
 (item 7) explicitly instructs: *"Use `tokio_util::sync::CancellationToken`
 (not raw `AtomicBool`)"* for graceful shutdown. Read as a whole, the
 roadmap's Week 3 progression is "learn `broadcast` as one option, then use
 `CancellationToken` as the actual mechanism" — which is exactly what was
 built. No rework needed here.


### 3. Worker availability: `Notify` first, then upgraded to `watch` (both roadmap-suggested, used for the right reasons)


- **Roadmap suggested:** "Worker state → watch (latest value)"
- **First pass:** A `Mutex`-guarded `WorkerPool<T>` combined with a
 `tokio::sync::Notify`, fixing an initial busy-loop bug (the dispatch
 branch was resolving instantly on every poll regardless of whether real
 work existed).
- **Final version (now built):** `WorkerPool<T>` owns a
 `watch::Sender<bool>` internally, updated via a single private
 `update_availability()` helper called from `register_worker`,
 `assign_job`, and `free_worker` after each mutation. `Scheduler` holds one
 long-lived `watch::Receiver<bool>` (subscribed once in `new()`, stored as
 a struct field — NOT resubscribed per loop iteration, which was an early
 mistake). The dispatch branch's two distinct "nothing to do" paths now
 await the signal that actually matches their blocking condition: "no idle
 worker" awaits `watcher.changed()`, while "queue empty" still awaits
 `Notify` — since a worker freeing up doesn't help if there's nothing
 queued, and vice versa.
- **Why `WorkerPool` owns the sender, not `Scheduler`:** availability state
 is already fully encapsulated inside `WorkerPool` (this is the same
 reasoning that moved `WorkerStatus` out of `Worker` and into `PoolEntry`
 earlier) — the pool is the only thing that knows when its own state
 changes, so it should be the one responsible for keeping the signal
 accurate, rather than `Scheduler` needing to remember to update an
 external channel after every pool-mutating call site.
- **Why the update uses `send_if_modified`, not `send` or `send_modify`:**
 see entry 8 below — this was the fix for a real deadlock.


### 4. `in_flight` tracking: `Arc<Mutex<HashMap<u64, Job>>>` instead of a bare `HashMap`


- **Not a roadmap item directly, but worth logging as a structural
 decision.**
- **What changed:** `in_flight` started as a bare `HashMap<u64, Job>` owned
 directly by `Scheduler`, mutated only from inside `run()`. It was changed
 to `Arc<Mutex<HashMap<u64, Job>>>`, matching `queue` and `worker_pool`.
- **Why:** Once `run()` is spawned as a background task (`tokio::spawn`),
 ownership of `Scheduler` moves into that task. External code (tests, and
 eventually anything polling scheduler state) has no way to observe a field
 that was moved away. `queue` and `worker_pool` already solved this by
 being shared, lock-guarded state; `in_flight` needed the same treatment
 the moment something outside `run()` needed to read it.


### 5. Retry delay enforcement: `waiting_retry` map + per-retry spawned sleep


- **Not a roadmap item directly, but a correctness gap the roadmap's own
 test spec implied.**
- **The gap:** `RetryPolicy::next_delay()` correctly computes a backoff
 duration, but nothing enforced it — a retried job was re-enqueued
 immediately and could be redispatched on the very next dispatch-branch
 iteration, making the computed delay meaningless in practice.
- **What was built:** A new `waiting_retry: Arc<Mutex<HashMap<u64, Job>>>`
 field, structurally identical to `in_flight`. On `Retry`, the job is
 inserted into `waiting_retry` (in its own tightly-scoped lock, resolved
 before `tokio::spawn` is even called — no reliance on timing for
 correctness), and a dedicated task is spawned that sleeps for the
 computed `retry_after` duration, then removes the job from
 `waiting_retry`, re-enqueues it into `queue`, and calls `notify_one()`.
- **Why a `HashMap`, not a `BinaryHeap` ordered by ready-time:** rejected
 a heap — it only pays off if something needs to repeatedly ask "what's
 the earliest-ready entry," which matters for a periodic-scanner design.
 This uses one independent spawned sleep per retry instead, so nothing
 ever queries "what's next" — a plain keyed lookup is all that's needed,
 same reason `in_flight` uses one.
- **Why `waiting_retry` deliberately does NOT block graceful shutdown:**
 per the roadmap's own Week 3 spec — "queued jobs logged as not
 started" during shutdown — a job waiting out a retry delay isn't
 actively being worked on by anything. The shutdown exit check only
 considers `in_flight`, not `waiting_retry`.


### 6. Minimal dead-letter tracking: `dead_lettered` map (early, partial version of roadmap's Week 7 DLQ)


- **Roadmap context:** the full dead-letter queue (Phase 2, Week 7) is
 specced as `DashMap<JobId, DeadLetteredJob>`, with a richer struct
 (original job, failure reason, full attempt logs, final_failed_at) and
 gRPC inspection/requeue RPCs.
- **What was built now, ~4 weeks early:** a minimal
 `dead_lettered: Arc<Mutex<HashMap<u64, Job>>>`, storing just the `Job`
 itself (already carrying its final `DeadLettered` state). Built early
 because a retry-path integration test needed *somewhere* for a
 dead-lettered job to land — without it, `DeadLetter` jobs were removed
 from `in_flight` and simply vanished, making the dead-letter path
 untestable.
- **Explicitly not the final version** — no `DeadLetteredJob` struct, no
 attempt history, no inspection/requeue RPCs (no gRPC layer exists yet
 at all). This is a stepping stone for test visibility, to be replaced
 wholesale in Week 7.


### 7. Two silent state-machine bugs found via `let _ = transition(...)` discarding errors


- **The gap:** `transition()`'s `Result` was silently discarded
 (`let _ = transition(...)`) at every call site. This masked two real
 bugs, both only surfaced by watching integration tests fail and tracing
 backward from incorrect `retry_count`/final-state values:
 1. **Missing `Run` transition in the dispatch branch** — jobs were
    dispatched and executed without ever transitioning from
    `Queued`/`Retrying` into `Running`. Since `transition()`'s only
    valid `Fail` arm requires the prior state to be `Running`, every
    `Fail` call silently no-op'd via the catch-all arm — `retry_count`
    never incremented, causing jobs to retry infinitely instead of ever
    succeeding or exhausting retries. Fixed by adding
    `transition(&mut job, JobEvent::Run { worker_id, started_at })`
    before a job is inserted into `in_flight` and spawned.
 2. **Missing `(Failed, DeadLetter) → DeadLettered` arm** — the only
    existing `DeadLetter`-handling arm required the prior state to be
    `Retrying`, but a job about to be dead-lettered is actually in
    `Failed` state at that point (it was never really "retrying").
    Fixed by adding a direct `(Failed, DeadLetter) → DeadLettered` arm,
    rather than forcing a fake pass-through `Retrying` state that
    wouldn't have been semantically honest.
- **Open follow-up, not yet decided:** should `let _ = transition(...)`
 call sites at least log on `Err` (matching the existing `free_worker`
 error-logging pattern), given this exact silent-discard pattern has now
 hidden two real bugs? Deferred — revisit before Week 4.


### 8. `watch::Sender` deadlock: holding a `Ref` guard from `.borrow()` across `.send()`


- **The bug:** `update_availability()`'s first working version called
 `let current_value = self.watch_teller.borrow();`, then later called
 `self.watch_teller.send(...)` while `current_value` (a `watch::Ref`
 guard holding an internal read-lock on the channel's shared state) was
 still alive in scope. `send()` needs to acquire that same internal lock
 to write the new value and notify receivers — with the read-guard still
 held by the same thread, this deadlocked immediately and silently
 (no panic, no CPU spin — the task simply never made further progress).
 Diagnosed by adding `eprintln!` tracers at the very top of `run()` and
 working backward until the exact call that stopped producing output was
 isolated to `register_worker` → `WorkerPool::register_worker` →
 `update_availability`.
- **The fix:** switched to `watch::Sender::send_if_modified(|current_value| {
 ... })` — a single atomic borrow-mutate-notify operation via one closure,
 so there's never a separate guard alive when the internal write/notify
 happens. `send_if_modified` specifically (over `send_modify`) also
 restores the original "only notify on genuine change" goal, since its
 closure returns a `bool` indicating whether a real change occurred, and
 receivers are only woken when it's `true`.
- **General lesson, worth remembering for any future use of `watch`,
 `RwLock`, or similar guard-returning APIs:** binding a `.borrow()` (or
 `.read()`/`.lock()`) result to a variable keeps its guard alive for the
 variable's entire scope — including across later calls that need to
 acquire a conflicting lock on the *same* primitive. This is the same
 category of bug as holding a `std::sync::MutexGuard` across an `.await`
 point (caught earlier in `assign_job`), just manifesting through a
 different synchronization primitive's own internal locking instead of
 an external `Mutex`.


### 10. `Runnable::run()` implemented: replaced `simulate_job_execution` with a real worker-owned method


- **Roadmap context:** Week 2 originally specified
 `async fn run(&self, cancel: CancellationToken) -> Result<Output, Error>`
 on `Runnable`. This had sat as `todo!()` since Week 2, with
 `simulate_job_execution` (a free function, self-contained: took the job,
 slept, decided the outcome, sent the result itself) standing in as the
 Week 3 placeholder — explicitly noted at the time as *"this will replace
 simulate_job_execution... for now just a placeholder."*


- **Why now, and why it matters beyond "filling in a todo!()":** cross-
 checking Phase 2 (Week 5–6) revealed that real workers in this project's
 actual target architecture are **separate processes**, connected over
 gRPC — the scheduler tracks them as plain metadata
 (`workers: Arc<DashMap<WorkerId, WorkerInfo>>`), dispatches jobs to them
 over the network, and receives results back independently, later,
 through a separate channel (a heartbeat stream / RPC). `Worker::run()`
 being a method the scheduler calls and awaits directly is a *local
 simulation* of that eventual remote-dispatch pattern — not the final
 shape, but structurally the right stand-in, matching Week 3's own stated
 purpose ("prove logic before networking adds complexity").


- **`Worker` lost its `job_id` field entirely, and `Runnable` lost
 `get_job_id`/`change_job_id`.** Once `run()` takes the `Job` directly as
 a parameter, nothing on `Worker` itself needs to remember which job it's
 running — that fact already lives, singly, in `Scheduler.in_flight`.
 Keeping `job_id` on `Worker` too would have been the same
 two-sources-of-truth mistake already caught and fixed once with
 `WorkerStatus` (moved out of `Worker` into `PoolEntry`).


- **`Worker` now derives `Clone`, and `Runnable: Clone + Send + 'static`
 is a trait-level supertrait bound, not a local one.** Calling an async
 method requires holding the receiver across an `.await`, but a
 `std::sync::MutexGuard` from `worker_pool.lock()` cannot be held across
 an `.await` (the same deadlock class already hit once with `send()`
 inside `update_availability`). The resolution:
 `WorkerPool::get_worker(id) -> Option<T>` briefly locks, clones the
 specific worker out, and drops the lock — safe specifically because a
 worker with no meaningful mutable state of its own can never drift out
 of sync with the pool's authoritative copy. `Send + 'static` were added
 because, for the first time, a spawned task needed to capture a generic
 `T` directly (via `Arc<Mutex<WorkerPool<T>>>`), and `tokio::spawn`
 requires everything captured to be safely sendable across threads with
 no short-lived borrows. Both bounds were made trait-level rather than
 local to one `impl` block on the reasoning that every future `Runnable`
 implementor (eventually a `tonic` gRPC client handle) will always need
 both properties — this mirrors how real gRPC client types are
 conventionally cheap, `Arc`-backed, freely-cloneable handles, so the
 bound isn't a workaround, it's the correct shape for the contract.


- **The trait method signature uses `-> impl Future<Output = ...> + Send`
 instead of plain `async fn`.** Native `async fn` in traits does not
 automatically infer that the compiler-generated future type is `Send`,
 even when every value captured inside it clearly is — it must be stated
 explicitly on the trait's signature. The `impl` block itself still uses
 ordinary `async fn`; only the trait declaration needed the explicit
 form.


- **Bug found and fixed during this change: `JobOutcome::Cancelled` was
 handled with an empty match arm.** A cancelled job was never removed
 from `in_flight`, which meant `in_flight` could never become empty,
 which meant `run()`'s graceful-shutdown exit condition
 (`shutting_down && in_flight.is_empty()`) could never be satisfied —
 `graceful_shutdown_drains_in_flight` hung until its 2-second `timeout()`
 fired. Decided deliberately (not by default) that a cancelled job should
 neither retry nor dead-letter — it gets removed from `in_flight` and
 logged, on the reasoning that cancellation only happens during shutdown,
 which is already an accepted, deliberate wind-down, not a failure. No
 new `JobState::Cancelled` variant was added, since the job was
 genuinely abandoned mid-flight rather than reaching any real terminal
 state worth persisting.


### 11. Graceful shutdown gained a 30-second drain deadline


- **Roadmap context:** Week 3's own spec for graceful shutdown says
 "let in-flight complete (30s max)" — a cap that was never implemented
 when `CancellationToken`-based shutdown was first built (entry 2).
 `run()` previously waited for `in_flight.is_empty()` with no bound at
 all.
- **What was built:** a new `shutdown_cap_timer: Option<tokio::time::Instant>`
 field, set once — at the moment `shutting_down` first flips to `true`
 in the cancellation branch. A fourth `select!` branch, guarded by
 `if shutting_down`, races `tokio::time::sleep_until(start + 30s)`
 against the rest of the loop; if it fires, remaining `in_flight` jobs
 are logged and abandoned via `break`.
- **Why `sleep_until` and not `sleep`:** `select!` reconstructs every
 branch's future fresh on each loop iteration. A relative `sleep(30s)`
 would restart a fresh 30-second window every iteration and never
 actually expire. `sleep_until` anchors to the absolute instant shutdown
 began, so elapsed time correctly accumulates across iterations.
- **Why this needed a new `select!` branch rather than two separate loops
 (a "normal" loop and a "draining" loop):** rejected the two-loop
 design — it would require duplicating the entire results-handling
 match arm (`Success`/`Failure`/`Cancelled`) into a second loop, which is
 exactly the two-sources-of-truth risk already caught and fixed multiple
 times elsewhere in this project, just at the level of *code* instead of
 *data*. A single `select!` with per-branch `if` guards (already the
 established pattern for dispatch/cancellation) keeps one copy of the
 results logic active across both phases.
- **Defensive fallback, not a panic:** the deadline branch reads
 `shutdown_cap_timer.unwrap_or_else(|| { log; Instant::now() })` rather
 than `.unwrap()`. Traced whether `shutting_down == true` with
 `shutdown_cap_timer == None` is reachable — it isn't, since the only
 other path that sets `shutting_down = true` (the channel-closed branch)
 `break`s immediately and never reaches another poll of this branch. Kept
 the log-and-fallback anyway, consistent with this file's established
 discipline of defending against currently-unreachable states rather than
 trusting invariants silently.


### 12. Real timestamps: `now_millis()` replaces `0u64`/`started_at: 0` placeholders


- **What was built:** `job_data_structures::now_millis() -> u64`, using
 `SystemTime::now().duration_since(UNIX_EPOCH)` → milliseconds since the
 Unix epoch. Wired into the two places that previously hardcoded `0`:
 the dispatch branch's `JobEvent::Run { started_at }` and the results
 branch's `JobEvent::Success { completed_at }`.
- **Why `SystemTime`, not `Instant`:** per the DDIA guidance from the
 Week 5 reading list — monotonic clocks (`Instant`) are for measuring
 *durations* within a process; they're meaningless compared against a
 calendar date or across machines. `started_at`/`completed_at` are
 points in time meant to be human-readable/loggable, so `SystemTime` is
 correct here. (`shutdown_cap_timer`, by contrast, correctly uses
 `tokio::time::Instant` — a duration/deadline measurement, not a
 timestamp — the two time types were deliberately kept distinct for
 their distinct purposes.)
- **Why `.expect(...)` instead of a silent fallback on the `Err` case:**
 `duration_since` only fails if the host clock is set before 1970 —
 essentially a broken machine, not a scheduler logic bug. Considered
 `unwrap_or(0)`, but rejected it: `0` is not a harmless placeholder here,
 it's an actively wrong, plausible-looking date (1970) that would
 silently corrupt any downstream duration math or logging. Chosen to
 crash loudly instead, on the reasoning that a scheduler generating
 systematically wrong timestamps for an unknown period is worse than one
 that refuses to start.
- **No test changes required** — verified by tracing rather than
 assuming: `state_machine.rs`'s tests call `transition()` directly with
 hand-supplied literal timestamps, never invoking `now_millis()` at all;
 `scheduler.rs`'s tests never assert on the literal value of
 `started_at`/`completed_at`. The two layers were already decoupled by
 design.




 ### 13. New terminal state: `JobState::Abandoned` for jobs lost during shutdown


- **The gap:** graceful shutdown had no representation for a job that
 never reached a real terminal outcome — jobs cancelled mid-flight, jobs
 sitting in `waiting_retry` when shutdown began, and jobs still in
 `queue` and never dispatched were all silently dropped with no record.
 Two of these three cases (cancelled-in-flight, still-queued) were the
 exact correctness gap flagged in Week 3's own test spec ("queued jobs
 logged as not started").
- **What was built:** `JobState::Abandoned { reason: String,
 abandoned_at: u64 }` and a matching `JobEvent::Abandon { reason:
 String }`, with valid predecessor states `Queued`, `Running`, and
 `Retrying` in `transition()`. Deliberately no `(Failed, Abandon)` arm —
 nothing in `run()` currently leaves a job sitting in `Failed` across an
 `.await` point (the `Fail → determine_next_event → Retry/DeadLetter`
 sequence is synchronous), so that predecessor state is unreachable by
 construction. Noted in a comment at the `transition()` match's closing
 brace rather than silently omitted.
- **Why a new state instead of reusing `DeadLettered`:** a dead-lettered
 job genuinely exhausted its retries — that's a fact about the job's
 own execution history. An abandoned job's story is entirely about the
 *scheduler's* lifecycle, not the job's — conflating them would make
 Week 13's chaos tests (which need to prove zero job loss) unable to
 distinguish "this job failed too many times" from "this job never got
 a fair chance because the process was shutting down." Different facts,
 different variant.
- **Why not reuse `JobOutcome::Cancelled` as the state too:**
 `JobOutcome::Cancelled` is a worker-execution-layer signal — it means
 one specific call to `Runnable::run()` was interrupted. `Abandoned` is
 the job-state-machine-layer fact that the job will never be processed
 further. A `Cancelled` outcome is exactly the event that *causes* an
 `Abandon` transition in one of three cases, but they're not
 interchangeable — a job can become `Abandoned` (from `Queued` or
 `Retrying`) without any worker, and therefore any `JobOutcome`, ever
 having been involved at all.
- **New `Scheduler` field:** `abandoned: Arc<Mutex<HashMap<u64, Job>>>`,
 structurally identical to `succeeded`/`dead_lettered`/`waiting_retry` —
 same reasoning as entry 4 and entry 6: storage location and state
 truth are separate facts, and every other terminal outcome already
 keeps its bucket and its `state` field in agreement, so `Abandoned`
 does too.


### 14. Retry-delay tasks now race against cancellation


- **The gap:** the spawned retry-delay task built in entry 5
 (`tokio::spawn` + `sleep(retry_after)` + re-enqueue) had no reference
 to the scheduler's `CancellationToken` at all. If shutdown began while
 a job was sitting in `waiting_retry`, the task would still sleep out
 its full delay and re-enqueue into `self.queue` — a queue that, by
 then, nothing would ever dequeue from again, since `run()` may have
 already returned. Silent, untested job loss.
- **What was built:** the retry-delay task is now one arm of a
 `tokio::select!` racing a cloned `CancellationToken.cancelled()`
 against the original sleep-then-re-enqueue future — same shape as
 `Worker::run()`'s existing race against cancellation. On the
 cancellation arm winning, the job is removed from `waiting_retry`,
 transitioned `Retrying → Abandoned` (entry 13), and inserted into the
 new `abandoned` map instead of being re-enqueued.
- **Known remaining gap, deliberately not fixed yet:** this task is
 still detached — spawned via `tokio::spawn` with no retained
 `JoinHandle`, so `run()`'s own loop doesn't wait for it to actually
 finish reacting to cancellation before `run()` itself returns. In
 practice the window between `run()` observing `in_flight.is_empty()`
 and a given retry-task's cancellation branch executing should be
 small, but it isn't zero and isn't proven. Deferred rather than fixed
 now — revisit if it manifests as an actual lost job, and especially
 once Phase 2 persistence makes "job with no final record" an
 observable bug instead of an invisible one.


### 15. `now_millis()` is not test-injectable — bracket-and-bound used instead of exact-value assertions


- **The gap:** `Abandoned` (entry 13), like every other terminal state
 that carries a real timestamp, reads the system clock directly inside
 `transition()` via `now_millis()`. Tests can't predict or hardcode
 that value, which makes whole-`Job`/whole-`JobState` `assert_eq!`
 impossible once a state includes a clock reading.
- **What was built (test-side convention, not a code change):** tests
 take `before = now_millis()` immediately before triggering the
 transition under test and `after = now_millis()` immediately
 after, then assert `before <= abandoned_at && abandoned_at <= after`
 instead of an exact value. Applied to the three new `state_machine.rs`
 unit tests for `Abandon` and both new `scheduler.rs` integration
 tests.
- **Why not just hardcode a plausible value:** a hardcoded timestamp
 could pass by coincidence and hide a real bug (e.g. `abandoned_at`
 accidentally reading from `job.created_at` instead of `now_millis()`)
 — bracket-and-bound actually proves the value came from the real
 clock during the test's own execution window, which is a strictly
 stronger assertion.
- **Deferred, not fixed:** the real fix is abstracting the clock behind
 something injectable (a trait, or a passed-in closure) so tests can
 supply a fixed fake time instead of bracketing a real one. Not done
 now — revisit in Phase 2, which already has DDIA ch. 8 (unreliable
 clocks) on the Week 5 reading list.




### 16. 'available_retry_attempts' is unused within the entire codebase, removed to clean up information regarding Job


- I saw that available_retry_attempts was unused within the codebase, only being modified during the transition to Fail within transition().
 That being said, it is not read anywhere or referenced anywhere either. The one place it could have been used is in next_delay(retry_count: u64),
 but this uses the current retry count and compares it to the max_attempts which is defined within the RetryPolicy struct itself. Therefore available_retry_attempts
 can be treated as dead code which may prove to be a hazard in coding due to possible mix ups in information. Therefore a second per-job override mechanism is not needed
 since RetryPolicy lives in each Job and also expresses how many tries a job gets.




### 17. Worker dispatch is pull-based (`RequestWork`), diverging from a literal reading of Week 5's dispatch loop

- Roadmap suggested: Week 5's dispatch loop is described in push terms —
  "find available worker that supports job_type... dispatch to worker" —
  which reads as the scheduler actively selecting a worker and sending it
  a job.
- What was built (design decision, ahead of the code): dispatch is
  workers calling `RequestWork` and being handed a job in the response,
  not the scheduler calling into a worker. This follows from the Week 4
  proto schema (`RequestWork` exists precisely because a scheduler
  process cannot directly invoke a function running in a separate worker
  process) but is called out explicitly here since it's a real
  divergence from Week 5's wording, not something the roadmap
  unambiguously specified.
- Why:
  1. Load balancing is inherent and free — a worker only calls
     `RequestWork` when idle, so the scheduler never needs to track
     per-worker load to decide who's next.
  2. Fault tolerance is inherent and free — a dead worker just stops
     calling. No reassignment logic, no detecting a dead push target, no
     job silently in flight to a worker that no longer exists.
  3. Same shape as a Kafka consumer group: interchangeable consumers
     pulling from a shared source, no fixed assignment to lose track of
     when one dies.
- Tradeoff acknowledged: the Phase 1 `Notify`/`watch`-based dispatch
  signaling — built specifically to find an idle worker without
  busy-looping — doesn't carry forward as-is. It solved a problem
  ("how does the scheduler find an idle worker") that pull-based
  dispatch removes by construction: `RequestWork`'s handler doesn't
  search for anything, a worker announces its own availability by
  calling it.



### 18. RetryPolicy delay values validated at SubmitJob admission; both proto↔internal Job conversions are TryFrom, not From

- The gap: proto3 uint64 fields for delay_ms/base_ms/max_delay_ms have no
  upper bound. A value large enough to exceed prost_types::Duration's
  representable range causes std::time::Duration::try_from(...) to fail
  — found via a bare .unwrap() on that exact conversion.
- Where the check lives, and why: at SubmitJob's handler specifically —
  the one genuine "birth" moment for a Job today. RequestWork,
  ReportResult, GetJobStatus, ListJobs all operate on Jobs that already
  exist. Rejected putting the check inside the shared proto<->internal
  conversion since scheduler-core is depended on by worker-agent and
  scheduler-client, neither of which should inherit an admission policy
  they can't meaningfully enforce.
- Bound: 10-minute ceiling on delay_ms/max_delay_ms, chosen to prevent
  Duration-overflow, not to prevent retry-storm overload (that would
  need a floor, not a ceiling — separate, currently unenforced).
- Rejected: a bounded newtype making an invalid RetryPolicy
  unconstructable at the type level. Real cost (rework of next_delay()
  and every existing RetryPolicy test literal) against a risk
  (something other than SubmitJob constructing a real Job) that doesn't
  exist yet.
- Because validation is a runtime check at one call site rather than a
  type-level guarantee, a Job's retry_after is not provably safe to
  convert — so From<Job> for proto::Job was corrected to
  TryFrom<Job> for proto::Job, matching the other direction, rather
  than keeping a bare .unwrap() on an unproven assumption.
- Revisit trigger, concrete: the moment CreateTemplate/cron job
  instantiation (Phase 3) becomes a second path that constructs real
  Jobs. At that point the "one admission point" assumption this whole
  decision rests on is false, and the type-safe version should be
  reconsidered.

### 19. Job message serves double duty: general job representation and SubmitJob's request type

- The gap: proto::Job is used both as the full wire representation of an
  existing job (what GetJobStatus/ListJobs should return) and as
  SubmitJob's request type. TryFrom<proto::Job> for Job requires a real,
  valid id and a fully-formed state on the way in — but new_submitted()
  immediately overwrites both for a freshly submitted job. A client
  creating a brand-new job is currently forced to supply structurally
  valid but semantically meaningless placeholder values purely to
  satisfy a parser that discards them.
- Deferred, not fixed: the correct fix is a dedicated SubmitJobRequest
  message carrying only the fields a new job genuinely needs, distinct
  from Job. Not done now — this is a schema change, and schema changes
  mid-implementation are exactly the kind of divergence this log exists
  to make deliberate rather than accidental. Revisit before Phase 2
  persistence makes the schema harder to touch.

### 20. Week 4 status: SubmitJob and GetJobStatus both implemented and tested; five RPCs remain honest stubs

- SubmitJob and GetJobStatus are both fully implemented, matching Week
  4's own item 7, and both are proven against real failure modes, not
  just the happy path — verified via real client/server integration
  tests over an actual TCP connection: job submission success and
  InvalidArgument-rejection (retry policy over the 10-minute bound),
  status lookup success, NotFound (well-formed id, no matching job),
  and InvalidArgument (malformed id bytes).
- CancelJob, RequeueFromDLQ, CreateTemplate, ListJobs, and
  ListDeadLettered remain Status::unimplemented. All five are correctly
  out of Week 4's declared scope, not gaps — CreateTemplate depends on
  cron machinery that doesn't exist until Phase 3; RequeueFromDLQ
  depends on a real dead-letter store distinct from the single
  jobs: HashMap MySchedulerService currently holds; ListJobs/
  ListDeadLettered are server-streaming RPCs, a mechanism not yet
  exercised anywhere in this codebase. CancelJob is the closest to
  reachable today (the Abandon state-machine path already exists and
  is tested) but is deliberately deferred until real queue/worker
  infrastructure (Week 5's SchedulerState) lands, rather than building
  against MySchedulerService's current bare-HashMap shape, which is
  expected to change.
- Two genuine bugs found and fixed during this work, both instructive:
  a bare `Ok`/`Err` match arm with no `return` fell through to an
  unreachable `todo!()`, turning a malformed-input request into a
  server panic instead of a clean error; and a `.ok()` on a fallible
  proto conversion silently discarded a real error into `None`,
  reopening exactly the failure mode choosing TryFrom over From was
  meant to prevent.


### 21. Correction to entry 17: pull-based dispatch does not make worker fault tolerance free

- Entry 17 claimed a dead worker "just stops calling, no reassignment
  logic" as a free consequence of pull-based dispatch. This is only
  true while a worker is idle — nothing was ever assigned, nothing to
  reclaim.
- What's actually still unsolved: a worker that dies *after* calling
  RequestWork and receiving a job leaves that job stuck. The job left
  the queue the moment it was requested; nothing currently notices the
  worker is gone or requeues what it was holding. This needs the same
  category of machinery push-dispatch would have needed — heartbeat
  timeout detection and a reclaim path — which is why "worker fault
  tolerance" is Phase 3's own named topic, not incidentally solved
  by this week's architecture.
- What pull-dispatch genuinely does buy for free, precisely stated:
  the scheduler never has to search for or poll idle workers (the
  Week 3 busy-loop problem, solved structurally by inverting who
  initiates), and an idle worker's death is a true non-event. In-flight
  job loss on worker crash is not covered by either property.






---


## Template for future entries


```
### N. <short title>


- **Roadmap suggested:** <quote or paraphrase, with week/item reference>
- **What was built:** <what actually exists in the code>
- **Why:** <the actual reasoning — tradeoffs considered, not just "because">
```


Add an entry here any time the implementation meaningfully diverges from
what a given week's plan describes — not for every small naming choice, but
for anything that changes an architectural assumption a later week might
rely on.


