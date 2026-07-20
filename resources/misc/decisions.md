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