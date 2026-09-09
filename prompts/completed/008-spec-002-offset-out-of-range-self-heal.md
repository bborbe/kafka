---
status: completed
spec: [002-offset-out-of-range-self-heal]
summary: Added opt-in WithAutoResetOffsetOutOfRange option, IsOffsetOutOfRange helper, and reset-on-out-of-range consume-loop self-heal (reset offset via OffsetManager, recreate partition consumer at fallback, warn-log) with 10 new in-package tests; make precommit passes
execution_id: kafka-offset-reset-exec-008-spec-002-offset-out-of-range-self-heal
dark-factory-version: dev
created: "2026-09-08T22:00:00Z"
queued: "2026-09-08T20:03:23Z"
started: "2026-09-08T20:03:25Z"
completed: "2026-09-08T20:10:52Z"
---

# Opt-in Offset-Out-of-Range Self-Heal in Offset Consumer

<summary>
- Offset consumers can opt in to self-heal when their stored offset falls outside the broker's retained range, instead of erroring and stalling the partition
- A new per-consumer functional option controls the mode and is OFF by default, so every existing consumer behaves exactly as today
- When ON, an out-of-range error logs a warn line with topic, partition, old and new (fallback) offset, resets the stored offset backward via the offset manager, and recreates the partition consumer at the fallback offset so the read position advances
- A reusable out-of-range error detection helper is added and is called by the consume loop's detection path, not dead code
- Only the offset-out-of-range error class is intercepted; all other consume errors still propagate through the error handler and to Sentry upstream
- The consumer error handler is not modified — the reset lives in the consume-loop / partition-consumer recreation path, never a swallowed error
- All new tests use the existing in-package fake pattern with plain testing.T
</summary>

<objective>
Add an opt-in `WithAutoResetOffsetOutOfRange(bool)` mode to the bborbe/kafka offset consumer so a consumer whose stored offset is outside the broker's retained range self-heals — logs a warn reset line, resets the stored offset backward, recreates the partition consumer at the fallback offset, and resumes consuming — instead of propagating the error and going down.
</objective>

<context>
No `CLAUDE.md` exists in this repo — read `README.md` and `docs/dod.md` for conventions. Error handling uses `github.com/bborbe/errors` (`errors.Wrapf`, `errors.Cause`); logging uses `github.com/golang/glog`; test style for the offset consumer is plain `testing.T` in-package fakes (NOT Ginkgo — one-`RunSpecs`-per-binary constraint).

Read these files before changing anything:
- `kafka_consumer-offset.go` — the offset consumer: `ConsumerOptions` struct, functional options (`WithSkipCorruptBatches`), `IsCorruptionError`, the `Consume` loop with the existing `SkipCorruptBatches` interception + `skipAndAdvance` method, and `consumeMessages` with its per-error-class interception. The new out-of-range handling mirrors this existing pattern exactly.
- `kafka_consumer-error-handler.go` — `ConsumerErrorHandler` / `NewConsumerErrorHandler`. MUST NOT be modified.
- `kafka_consumer-partition.go` — `CreatePartitionConsumer(ctx, consumerFromClient, metricsConsumer, topic, partition, fallbackOffset, nextOffset)` and the `OutOfRangeErrorMessage` constant. `CreatePartitionConsumer` already falls back to the fallback offset on creation-time out-of-range — do not change it.
- `kafka_offset-manager.go` — the `OffsetManager` interface, incl. `FallbackOffset()` and `ResetOffset(ctx, topic, partition, nextOffset)`.
- `kafka_consumer-offset_internal_test.go` — the in-package fake pattern (`fakePartitionConsumer`, `fakeOffsetManager`, `fakeMessageHandlerBatch`, `testSaramaConsumer`, `testSaramaClient`, `testSaramaClientProvider`, `fakeLogSampler`, `fakeWaiter`, `newOffsetConsumerForTest`, `buildSkipTestConsumer`) and the existing skip-ON tests to mirror. NOTE: this file is `package kafka`, so it cannot import the counterfeiter mock in `mocks/` (import cycle) — extend the hand-written fakes instead.

Coding-plugin guides (in-container path) for conventions: `/home/node/.claude/plugins/marketplaces/coding/docs/go-functional-options-pattern.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md`.

Spec (read in full, it is authoritative): `specs/in-progress/002-offset-out-of-range-self-heal.md`.
</context>

<requirements>

### 1. New functional option `WithAutoResetOffsetOutOfRange(bool)` — default OFF

In `kafka_consumer-offset.go`, add a `AutoResetOffsetOutOfRange bool` field to the `ConsumerOptions` struct, keeping the existing fields and zero-value defaults unchanged:

```go
type ConsumerOptions struct {
	TargetLag                 int64
	Delay                     libtime.Duration
	SkipCorruptBatches        bool
	AutoResetOffsetOutOfRange bool
}
```

Add the option func immediately after `WithSkipCorruptBatches`, following the exact same `func(*ConsumerOptions)` pattern:

```go
// WithAutoResetOffsetOutOfRange enables self-healing when the stored offset is outside the
// broker's retained range: the consumer resets to the fallback offset and resumes instead of erroring.
func WithAutoResetOffsetOutOfRange(reset bool) func(*ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.AutoResetOffsetOutOfRange = reset
	}
}
```

Do NOT change existing options, their defaults, or any constructor signature (the option is threaded through the existing variadic `options ...func(*ConsumerOptions)` in `NewOffsetConsumerBatchWithProvider` — no callers need updating).

Test: mirror `TestWithSkipCorruptBatches_Option` with `TestWithAutoResetOffsetOutOfRange_Option` (set true → field true; set false → field false).

### 2. `IsOffsetOutOfRange(err error) bool` helper

In `kafka_consumer-offset.go`, directly below `IsCorruptionError`, add (this is the load-bearing helper — the string fallback is intentionally pinned to the existing `OutOfRangeErrorMessage` constant in `kafka_consumer-partition.go`, same package, for backwards compatibility; do not widen it):

```go
// IsOffsetOutOfRange reports whether err indicates a Kafka offset-out-of-range condition.
func IsOffsetOutOfRange(err error) bool {
	if err == nil {
		return false
	}
	if errors.Cause(err) == sarama.ErrOffsetOutOfRange {
		return true
	}
	if kerr, ok := errors.Cause(err).(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
		return true
	}
	return strings.Contains(err.Error(), OutOfRangeErrorMessage)
}
```

`errors` here is `github.com/bborbe/errors` (already imported in the file). `sarama.ErrOffsetOutOfRange` is a `sarama.KError` (an `int16` type implementing `error`); `errors.Cause` walks the bborbe/pkg-errors cause chain, so wrapped errors resolve to the root `KError`. This mirrors the proven draft implementation (branch `improve-kafka-offset-out-of-range-error`, commit `cdef9053`) — the executing container has `.git` masked, so the helper is inlined here rather than read from git history.

Test: a table test `TestIsOffsetOutOfRange` covering:
- `sarama.ErrOffsetOutOfRange` → true
- `sarama.KError(sarama.ErrOffsetOutOfRange)` → true
- bborbe-wrapped `errors.Wrapf(ctx, sarama.ErrOffsetOutOfRange, "wrapped error")` → true
- bborbe-wrapped `errors.Wrapf(ctx, sarama.KError(sarama.ErrOffsetOutOfRange), "wrapped kerror")` → true
- `stderrors.New(OutOfRangeErrorMessage)` → true (string fallback path)
- `nil` → false
- `sarama.ErrNotLeaderForPartition` → false
- `stderrors.New("some other error")` → false

### 3. OFF behavior unchanged (default)

With `AutoResetOffsetOutOfRange` OFF (default): an `ErrOffsetOutOfRange` consumer error in the consume loop must propagate exactly as today — `consumeMessages` delivers it to `errorHandler.HandleError` (which returns it), it is wrapped with `"parition consumer returns error"` and returned, and `Consume` wraps it with `"consume failed"` and returns it. Add NO interception when the option is OFF.

Test `TestConsumeMessages_OffsetOutOfRange_ResetOff_PropagatesError`: build a `fakePartitionConsumer` whose `errors` channel carries `&sarama.ConsumerError{Topic: <topic>, Partition: <p>, Err: sarama.ErrOffsetOutOfRange}`, set `consumer.consumerOptions.AutoResetOffsetOutOfRange = false`, call `consumeMessages`, assert the returned error is non-nil.

### 4. ON behavior — sentinel error and consume-loop detection

Add a sentinel next to `errSkipCorruptBatch` in `kafka_consumer-offset.go`:

```go
// errOffsetOutOfRange is a sentinel error indicating the stored offset is outside the broker's retained range.
var errOffsetOutOfRange = stderrors.New("offset out of range")
```

In `consumeMessages`, in BOTH select blocks (the outer `select` and the inner `for`-loop `select`), add a branch adjacent to the existing `SkipCorruptBatches` corruption branch, keeping the error-class filter narrow:

```go
if c.consumerOptions.AutoResetOffsetOutOfRange && IsOffsetOutOfRange(err.Err) {
	if len(result) > 0 {
		return result, nil
	}
	return nil, errOffsetOutOfRange
}
```

Any other consumer error (rebalance, auth, connection) still falls through to `errorHandler.HandleError` and propagates — do not intercept anything except the offset-out-of-range class.

Test `TestConsumeMessages_OffsetOutOfRange_ResetOn_ReturnsSentinel`: option ON, inject the out-of-range `*sarama.ConsumerError`, assert the returned error is non-nil and its message contains `"offset out of range"`.

### 5. Reset-and-advance method + `Consume` loop wiring

Add a method on `offsetConsumer` in `kafka_consumer-offset.go`, mirroring `skipAndAdvance` (this is the reset path — it MUST recreate the partition consumer so the read position advances; it MUST NOT swallow the error):

```go
// resetOnOffsetOutOfRange resets the stored offset to the fallback offset and recreates the partition
// consumer at the fallback offset so the read position advances past the stuck offset. It must not be
// implemented by swallowing the error in the error handler — that would leave the partition consumer
// stuck at the invalid offset forever.
func (c *offsetConsumer) resetOnOffsetOutOfRange(
	ctx context.Context,
	consumer sarama.Consumer,
	oldPartitionConsumer sarama.PartitionConsumer,
	partition Partition,
	stuckOffset Offset,
) (sarama.PartitionConsumer, Offset, error) {
	fallbackOffset := c.offsetManager.FallbackOffset()
	glog.Warningf(
		"reset offset out of range in topic(%s) partition(%d): offset %s -> fallback %s",
		c.topic,
		partition,
		stuckOffset,
		fallbackOffset,
	)
	if err := c.offsetManager.ResetOffset(ctx, c.topic, partition, fallbackOffset); err != nil {
		return nil, Offset(0), errors.Wrapf(ctx, err, "reset offset to fallback %s failed", fallbackOffset)
	}
	newPC, err := CreatePartitionConsumer(
		ctx,
		consumer,
		c.metrics,
		c.topic,
		partition,
		fallbackOffset,
		fallbackOffset,
	)
	if err != nil {
		return nil, Offset(0), errors.Wrapf(ctx, err, "create partition consumer at fallback offset %s failed", fallbackOffset)
	}
	if err := oldPartitionConsumer.Close(); err != nil {
		glog.V(4).Infof("closing old partition consumer returned error: %v", err)
	}
	return newPC, fallbackOffset, nil
}
```

Notes on this method:
- The two trailing args of `CreatePartitionConsumer` are `fallbackOffset` (the offset used if creation hits an out-of-range error) and `nextOffset` (the requested offset). Passing `fallbackOffset` for both recreates the consumer at the fallback offset — the same fallback value `CreatePartitionConsumer` already uses on creation-time out-of-range.
- The warn log records the stuck offset (old) and the fallback value (new). The fallback is typically the `sarama.OffsetOldest` sentinel (`-2`) — the log records what the consumer was reset to, NOT a broker-resolved absolute offset.
- Failure ordering matches spec Desired Behavior 3a→3b→3c: log, then `ResetOffset`, then recreate. If `ResetOffset` fails the error propagates with no recreate (no partial offset commit). If recreate fails the connection error propagates (never silently advance). If the process dies mid-reset after `ResetOffset` ran, a restart reads the fallback and recreates the same valid consumer (idempotent — no torn state).

In the `Consume` loop, inside the existing `if err != nil` block and directly after the `SkipCorruptBatches`/`errSkipCorruptBatch` branch, add:

```go
if c.consumerOptions.AutoResetOffsetOutOfRange && errors.Is(err, errOffsetOutOfRange) {
	newPC, newOff, resetErr := c.resetOnOffsetOutOfRange(
		ctx,
		consumerFromClient,
		consumePartition,
		Partition(partition),
		nextOffset,
	)
	if resetErr != nil {
		return errors.Wrapf(ctx, resetErr, "reset offset out of range failed")
	}
	consumePartition = newPC
	nextOffset = newOff
	continue
}
```

`nextOffset` at that point is the stuck offset (it is only advanced to `msg.Offset + 1` after a successful batch), which is exactly what the warn log and `ResetOffset` need. The `continue` re-enters the loop against the recreated partition consumer.

Tests (direct-call style for `resetOnOffsetOutOfRange`, mirroring the existing `TestSkipAndAdvance_*` tests; goroutine style for the full `Consume` path, mirroring `TestSkipAndAdvance_RecreatesPartitionConsumerAtAdvancedOffset`):

- **`TestOffsetConsumerAutoResetOffsetOutOfRange`** (the spec-named regression test — full `Consume` path, option ON). Build the consumer like `buildSkipTestConsumer` but with `consumerOptions: ConsumerOptions{AutoResetOffsetOutOfRange: true}` (skipper may be nil — it is not exercised) and a `testSaramaConsumer.consumePartitionFn` that returns, on the first `ConsumePartition` call, a `fakePartitionConsumer` whose `errors` channel carries `&sarama.ConsumerError{Err: sarama.ErrOffsetOutOfRange}`, and on the second call a `fakePartitionConsumer` whose `messages` channel carries one good `*sarama.ConsumerMessage`. `fakeOffsetManager{nextOffset: <stuckOffset>, fallbackOffset: OffsetOldest}`. Run `Consume` in a goroutine, `waitForCalls(&callCount, 2)`, then `cancel()` and drain the goroutine. Assert:
  - (i) the recreated partition consumer was created at the fallback offset: `testSaramaConsumer.consumePartitionCalls` has 2 entries, `[stuckOffset.Int64(), OffsetOldest.Int64()]` (reuse the `assertConsumePartitionCalls`-style assertion);
  - (ii) the good message after the reset was delivered/processed: the `fakeMessageHandlerBatch` recorded a `ConsumeMessages` call containing the good message (extend `fakeMessageHandlerBatch` to record received messages — see Requirement 8);
  - (iii) `Consume` did NOT return the offset error: capture the goroutine's returned error and assert `errors.Is(err, context.Canceled)` is true (the loop survived the reset and only ended on the test's cancellation);
  - (iv) `ResetOffset` was invoked with the fallback offset: `fakeOffsetManager.resetOffsetCalled == 1` and the recorded reset offset equals `OffsetOldest` (extend `fakeOffsetManager` to record the reset offset argument — see Requirement 8).
- `TestResetOnOffsetOutOfRange_ResetOffsetError` — `fakeOffsetManager.ResetOffset` returns an error; assert `resetOnOffsetOutOfRange` returns a non-nil error and a nil `newPC`.
- `TestResetOnOffsetOutOfRange_CreatePartitionConsumerError` — recreate fails (make `consumePartitionFn` return an error on the fallback-offset call); assert the error propagates and is NOT an offset error.
- `TestResetOnOffsetOutOfRange_LogsWarn` — see Requirement 8.

### 6. Unrelated consume error still propagates with the option ON

Test `TestConsumeMessages_UnrelatedError_ResetOn_Propagates`: option ON, inject a non-offset consumer error (e.g. `stderrors.New("rebalance timeout")` as `*sarama.ConsumerError.Err`), assert `consumeMessages` returns it (non-nil, via the error handler). This proves only the `OffsetOutOfRange` class is intercepted.

### 7. Error handler NOT modified

Do NOT modify `kafka_consumer-error-handler.go` — `NewConsumerErrorHandler` must keep returning `err` from `HandleError`.

Test `TestConsumerErrorHandler_HandleError_OffsetOutOfRange_ReturnsNonNil`: `NewConsumerErrorHandler(NewMetrics()).HandleError(&sarama.ConsumerError{Topic, Partition, Err: sarama.ErrOffsetOutOfRange})` returns non-nil.

### 8. Test-fake extensions and log capture

- Extend `fakeMessageHandlerBatch` (in `kafka_consumer-offset_internal_test.go`) to record received messages: add a `received [][]*sarama.ConsumerMessage` field and append in `ConsumeMessages` (additive — existing tests still compile).
- Extend `fakeOffsetManager` to record the reset offset: add a `resetOffsetOffset Offset` field and set it in `ResetOffset` alongside the existing `atomic.AddInt32(&f.resetOffsetCalled, 1)` (additive).
- `TestResetOnOffsetOutOfRange_LogsWarn`: call `resetOnOffsetOutOfRange` directly (synchronous — no goroutine race) and capture glog's WARNING output:
  1. `flag.Set("logtostderr", "true")` with `defer flag.Set("logtostderr", "false")` — glog registers the `logtostderr` flag in its package `init()`, and with it true the WARNING severity is written to stderr (by default WARNING goes to the file sink, not stderr, so this flag is required).
  2. Swap `os.Stderr` for an `os.Pipe` (`r, w, _ := os.Pipe(); old := os.Stderr; os.Stderr = w`), call `resetOnOffsetOutOfRange(...)`, then `w.Close()` and restore `os.Stderr = old` before reading `io.ReadAll(r)`.
  3. Assert the captured output contains the topic, the partition, the stuck offset, and the fallback offset (e.g. loop over `[]string{topic.String(), strconv.FormatInt(partition.Int32(), 10), stuckOffset.String(), fallbackOffset.String()}` and assert `strings.Contains` for each).
  Add the imports `flag`, `io`, `os` to the test file.

### 9. Final self-check

Before finishing: re-run `<verification>` and confirm it passes; then walk each Acceptance Criterion in `specs/in-progress/002-offset-out-of-range-self-heal.md` against the change and confirm each is satisfied by code + test evidence.
</requirements>

<constraints>
- Frozen anti-pattern (MUST NOT): do NOT implement the reset by having the consumer error handler (`NewConsumerErrorHandler`) return nil to swallow `ErrOffsetOutOfRange`. The offset is only advanced via `MarkOffset` after a successful message, so a swallowed error leaves the partition consumer stuck at the same invalid offset forever — a silent stall. The reset MUST recreate the partition consumer at the fallback offset so the read position advances.
- Default behavior is frozen: with the option OFF, the error-propagate-and-fail path must be unchanged.
- The new option must follow the existing functional-option style in `kafka_consumer-offset.go`; existing options and their defaults must not change.
- Only the `OffsetOutOfRange` error class is intercepted. All other consume errors (rebalance timeouts, auth failures, connection errors) still reach `errorHandler.HandleError` and propagate — keep the error-class filter narrow.
- Tests must use the existing in-package fake pattern with plain `testing.T`, NOT Ginkgo (one-`RunSpecs`-per-binary constraint).
- `IsOffsetOutOfRange` must live in the kafka package (home file: `kafka_consumer-offset.go`, next to `IsCorruptionError`), not the trading repo.
- The reset must be idempotent across restarts: after `ResetOffset`, a restart reads the fallback offset and recreates the same valid consumer — no partial-state trap.
- Do NOT add a new metric for reset events — the warn log line is the observability; no new Prometheus metric, no new counter.
- Do NOT change `CreatePartitionConsumer` in `kafka_consumer-partition.go` or its creation-time out-of-range fallback (already works via the `OutOfRangeErrorMessage` string match) — out of scope for this prompt.
- Do NOT wire any trading-repo flag (e.g. a main.go argument) — that is a separate follow-up in the trading repo.
- Do NOT commit — dark-factory handles git.
- Repo-relative paths only (container executes with the repo mounted; no absolute/host paths).
- Existing tests must still pass; `make precommit` must pass.
</constraints>

<verification>
Run `make precommit` — must pass (this runs ensure, format, generate, test, check, addlicense).

Then run the targeted regression tests and grep checks. The repo has `hideGit: true` (`.git` is masked in the container), so do NOT use bare `git` commands here — filesystem/grep/go-test checks only:

- `go test -mod=mod -run 'TestOffsetConsumerAutoResetOffsetOutOfRange|TestConsumeMessages_OffsetOutOfRange|TestResetOnOffsetOutOfRange|TestIsOffsetOutOfRange|TestWithAutoResetOffsetOutOfRange|TestConsumeMessages_UnrelatedError_ResetOn_Propagates|TestConsumerErrorHandler_HandleError_OffsetOutOfRange_ReturnsNonNil' ./...` — must pass (exit 0)
- `grep -n 'WithAutoResetOffsetOutOfRange' kafka_consumer-offset.go` — must return ≥1 match
- `grep -n 'IsOffsetOutOfRange' kafka_consumer-offset.go` — must return ≥1 match
- `grep -n 'ResetOffset' kafka_consumer-offset.go` — must return ≥1 match
- `! grep -q 'return nil' kafka_consumer-error-handler.go` — must succeed (0 matches)
- `go test -mod=mod ./...` — must pass (full suite, exit 0)
</verification>
