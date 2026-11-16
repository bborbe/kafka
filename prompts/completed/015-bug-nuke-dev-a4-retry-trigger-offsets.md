---
status: completed
spec: [bug-nuke-dev-a4]
summary: Added bounded retry with run.Retry around the startup trigger-offset computation, classified sarama.ErrNotLeaderForPartition as transient, and added the requested tests and changelog entry.
execution_id: 53171d7d-7aac-55c2-973d-2f4e95ef7e06-exec-015-bug-nuke-dev-a4-retry-trigger-offsets
dark-factory-version: dev
created: "2026-11-16T17:30:00Z"
queued: "2026-11-16T16:36:24Z"
started: "2026-11-16T16:36:26Z"
completed: "2026-11-16T16:43:13Z"
---

# Retry the startup trigger-offset computation on transient broker errors

<summary>
- Startup no longer dies because of a single partition-leadership blip while the consumer is working out where each partition should resume
- Only temporary broker conditions are retried; a genuine configuration, permission or data error still fails on the first attempt
- Retries are bounded in both attempt count and total waiting time, so a persistent outage still surfaces an error instead of hanging forever
- "Not the leader for this partition" is now recognised as temporary everywhere the library classifies broker errors, not only on this new path
- Cancelling the surrounding context interrupts the wait between attempts
- Healthy-broker behaviour is unchanged: one attempt, same offsets, same logs
- No new configuration, metric, marker type or module dependency is introduced
</summary>

<objective>
Make the startup trigger-offset computation survive transient broker errors such as `sarama.ErrNotLeaderForPartition` during a broker restart by retrying the computation with bounded backoff, so the consuming service no longer exits at startup on a single partition-leadership blip.
</objective>

<context>
Read `specs/in-progress/bug-nuke-dev-a4.md` — it is the contract for this change.

No `CLAUDE.md` exists in this repo. Read `README.md` and `docs/dod.md` for conventions. Note that `docs/dod.md`'s "Tests use Ginkgo/Gomega conventions" line is stale for the internal test package: `kafka_suite_test.go` holds the single permitted `RunSpecs`, so tests in `package kafka` use plain `func Test`. Do not "correct" the new tests to Ginkgo.

Read these files before writing code — they define the exact code you are changing:

- `kafka_consumer-offset.go` — `IsTransientBrokerError(err error) bool` is the existing transient classifier: it resolves `errors.Cause(err)` and tests it against `io.EOF`, `io.ErrUnexpectedEOF`, `syscall.ECONNREFUSED`, `syscall.ECONNRESET`, `sarama.ErrBrokerNotAvailable` using `errors.Is`. Its second consumer is `handleConsumerError`, which wraps a matching error in `*transientError` so a downstream reporter can suppress it.
- `kafka_consumer-offset-highwater-marks.go` — `createTriggerOffsets(ctx, saramaClient, topic, offsetManager) (map[Partition]Offset, error)` performs the three broker reads (`HighWaterMarks`, `ConsumerGroupOffsets`, `GetRealOffset`) and combines them with `combineConsumerGroupAndHighwaterMark`. Its only call site is inside the `ConsumerFunc` closure returned by `NewOffsetConsumerHighwaterMarksBatchWithProvider`.
- `kafka_highwatermarks.go` — `HighWaterMarks` loops every partition calling `HighWaterMark` → `sarama.Client.GetOffset`. This is where `ErrNotLeaderForPartition` surfaces today and aborts startup. Do not change these functions.
- `kafka_consumer-group-offset.go` and `kafka_offset.go` — the other two reads in the retried unit; both are idempotent.
- `kafka_consumer-offset_internal_test.go` — the in-package fakes `testSaramaClient` (minimal `SaramaClient`; `Partitions` returns `f.partitions`, `GetOffset` currently returns `(0, nil)`) and `fakeOffsetManager` (`nextOffset` field), plus the plain-`func Test` style and the `TestConsume_TransientBrokerErrorSatisfiesMarker` table.
- `github.com/bborbe/run` — already required in `go.mod`. Inspect the helper you must reuse with `go doc github.com/bborbe/run Retry` and `go doc github.com/bborbe/run Backoff`. `run.Retry(backoff Backoff, fn Func) Func` returns a `run.Func` (`func(context.Context) error`); `Backoff` has `Delay time.Duration`, `Factor float64`, `Retries int`, `IsRetryAble func(error) bool`. Its wait formula is `Delay + Delay*Factor*(attempt-1)`, and the waiter respects context cancellation.

Coding conventions: `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` and `/home/node/.claude/plugins/marketplaces/coding/docs/go-context-cancellation-in-loops.md`.

<!-- OPEN QUESTIONS FOR THE HUMAN REVIEWER (not instructions to the executing agent):
1. The spec does not pin backoff values. This prompt proposes Delay=1s, Factor=2, Retries=5 (about 25s of cumulative waiting before the last error is returned). Confirm these suit a broker restart, or adjust them before approval.
2. The spec offers two fixes — internal retry, or returning a Transient()-marked error so the caller's runner retries. This prompt implements the internal bounded retry only. Confirm retry-only is acceptable and the marker is not additionally required.
3. The spec's optional "raise replication factor for core-* topics" follow-up belongs to the service's deployment configuration in another repo and is intentionally out of scope here.
-->

<requirements>
1. In `kafka_consumer-offset.go`, extend `IsTransientBrokerError` so `sarama.ErrNotLeaderForPartition` is classified transient. Keep the existing implementation shape: resolve the wrapped cause with `errors.Cause` and test membership with `errors.Is`; append `|| errors.Is(cause, sarama.ErrNotLeaderForPartition)` to the existing chain. Update the GoDoc comment above the function to name "this broker is not the leader for the partition" alongside the existing conditions. Do not change the signature, do not remove or reorder existing conditions, and do not add any other sarama error to the list.

2. In `kafka_consumer-offset-highwater-marks.go`, add a package-level backoff configuration for the startup trigger-offset computation, with this shape:

```go
// triggerOffsetsBackoff bounds retries of the startup trigger-offset computation.
// Transient broker errors (for example ErrNotLeaderForPartition during a broker
// restart) are retried; every other error fails on the first attempt.
var triggerOffsetsBackoff = run.Backoff{
	Delay:       time.Second,
	Factor:      2,
	Retries:     5,
	IsRetryAble: IsTransientBrokerError,
}
```

This uses `github.com/bborbe/run`, already required by `go.mod`. Add the `time` import to the file's import block.

3. Change `createTriggerOffsets` in the same file to take a `backoff run.Backoff` as its last parameter and run its existing body through `run.Retry`, following this contract:

```go
func createTriggerOffsets(
	ctx context.Context,
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
	backoff run.Backoff,
) (map[Partition]Offset, error) {
	var triggerOffsets map[Partition]Offset
	err := run.Retry(backoff, func(ctx context.Context) error {
		// existing body: HighWaterMarks -> ConsumerGroupOffsets -> GetRealOffset
		// -> combineConsumerGroupAndHighwaterMark; assign to triggerOffsets and
		// return the existing errors.Wrapf values unchanged.
		return nil
	})(ctx)
	if err != nil {
		return nil, err
	}
	return triggerOffsets, nil
}
```

The retried unit is the whole computation — all three broker reads are idempotent reads, so re-running them from the first partition is safe. Preserve the body's current order, its existing `errors.Wrapf` messages (`get highwater marks failed`, `get consumer group offsets failed`), and its existing `glog.V(2)` lines. Return the error produced by `run.Retry` unchanged; the caller already wraps it with `create trigger offsets failed`. Do not retry individual partitions inside `HighWaterMarks`, and do not change `HighWaterMarks`, `HighWaterMark`, or `NewHighwaterMarkProvider`.

4. Update the single call site in `NewOffsetConsumerHighwaterMarksBatchWithProvider` (inside the `ConsumerFunc` closure) to pass `triggerOffsetsBackoff` as the new last argument. Confirm with `grep -rn 'createTriggerOffsets(' .` that no other call site exists before finishing.

5. Add `TestIsTransientBrokerError` to `kafka_consumer-offset_internal_test.go`, next to `TestIsOffsetOutOfRange`, in the existing plain `func Test` style. Make it table-driven and cover:
   - `sarama.ErrNotLeaderForPartition` → true (the new behaviour)
   - `errors.Wrapf(context.Background(), sarama.ErrNotLeaderForPartition, "wrapped")` → true (proves the wrapped cause is classified — this is the shape the retry path sees)
   - `sarama.ErrBrokerNotAvailable` → true (regression)
   - `io.EOF` → true (regression)
   - `sarama.ErrOffsetOutOfRange` → false
   - `stderrors.New("boom")` → false
   - `nil` → false

6. Add exactly one case to the table in `TestConsume_TransientBrokerErrorSatisfiesMarker`: `{name: "not leader for partition", err: sarama.ErrNotLeaderForPartition, transient: true}`. This is the classifier's second consumer; it proves the new classification reaches the consumer error marker, not only the new retry path. Do not change that test's structure or its other cases.

7. Extend `testSaramaClient` in `kafka_consumer-offset_internal_test.go` with four fields and replace its `GetOffset` method so tests can script broker errors:
   - `getOffset int64` — value returned on a successful call (zero value preserves today's `0`)
   - `getOffsetErr error` — when non-nil, returned on every call (persistent failure)
   - `getOffsetErrors []error` — a queue; when non-empty, each call consumes and returns the first entry (scripted fail-then-succeed)
   - `getOffsetCalls int` — incremented at the top of every `GetOffset` call

   Precedence: return `getOffsetErr` if non-nil; otherwise pop `getOffsetErrors` if non-empty; otherwise return `(getOffset, nil)`. Keep the zero-value behaviour identical to today so the other tests using this fake are unaffected.

8. Create `kafka_consumer-offset-highwater-marks_internal_test.go` in `package kafka` with the BSD license header (year 2026), plain `testing.T`, and no new Ginkgo suite. Reuse `testSaramaClient` and `fakeOffsetManager`. In every test, copy the production backoff and zero its delay so the test does not sleep:

```go
backoff := triggerOffsetsBackoff
backoff.Delay = 0
```

   Always set `partitions: []int32{0}` on the fake client (an empty partition list would skip `GetOffset` entirely) and set `fakeOffsetManager.nextOffset` to a non-negative value such as `Offset(5)` so `GetRealOffset` issues no extra `GetOffset` calls — that keeps `getOffsetCalls` a clean attempt counter. Add these tests:
   - `TestCreateTriggerOffsets_RetriesTransientErrorThenSucceeds`: `getOffsetErrors: []error{sarama.ErrNotLeaderForPartition}`, `getOffset: 10`; expect nil error, `getOffsetCalls == 2`, and result `map[Partition]Offset{0: 9}` (high-water mark 10, consumer-group offset 5 → 10-1).
   - `TestCreateTriggerOffsets_ExhaustsRetriesOnPersistentTransientError`: `getOffsetErr: sarama.ErrNotLeaderForPartition`, and set `backoff.Retries = 2`; expect a non-nil error, `getOffsetCalls == 3`, and `IsTransientBrokerError(err)` true (the exhausted error still carries the transient cause through the chain).
   - `TestCreateTriggerOffsets_DoesNotRetryNonTransientError`: `getOffsetErr: stderrors.New("boom")`; expect a non-nil error and `getOffsetCalls == 1`.
   - `TestCreateTriggerOffsets_SucceedsFirstAttempt`: no scripted errors, `getOffset: 10`; expect nil error, `getOffsetCalls == 1`, and result `map[Partition]Offset{0: 9}`.

9. Add a `## Unreleased` heading at the top of `CHANGELOG.md` (above the most recent version heading) with one entry:
   `- fix: Retry the startup trigger-offset computation with bounded backoff on transient broker errors and classify ErrNotLeaderForPartition as transient`

10. Before you finish, re-run `<verification>`, confirm it passes, and walk each requirement above against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT add a configuration knob, a metrics counter, or a disable switch — the retry policy is a fixed package-level value.
- Do NOT add a `Transient()` marker to the returned error; the internal bounded retry is the chosen fix (the spec offers retry or marker, not both).
- Do NOT change the signature or behaviour of `HighWaterMarks`, `HighWaterMark`, or `NewHighwaterMarkProvider`.
- Do NOT touch the offset-out-of-range, corruption-skip, or closed-partition-consumer recovery paths.
- Do NOT change topic replication factors or any deployment/manifest files — that optional follow-up lives in the service's deployment configuration, not this library.
- Do NOT add a new module dependency; `github.com/bborbe/run` is already required.
- Do NOT add a second Ginkgo `RunSpecs`; tests in `package kafka` use plain `testing.T`.
- Existing tests must still pass.
- No `//nolint` without an explanation.
</constraints>

<verification>
ROOTDIR=/workspace make precommit
go test -run 'TestIsTransientBrokerError|TestCreateTriggerOffsets|TestConsume_TransientBrokerErrorSatisfiesMarker' -count=1 .

Both must pass. `make precommit` must exit 0. The `go test -run` line makes the named evidence tests fail loudly if a test name drifts — without it, a renamed test would match nothing and exit 0.
</verification>
