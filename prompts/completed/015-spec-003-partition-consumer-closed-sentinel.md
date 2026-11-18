---
status: completed
spec: [003-bug-partition-consumer-closed-sentinel]
summary: Verified the existing closed-partition-consumer sentinel recovery implementation and tests satisfy all requirements; make precommit passes.
execution_id: 152272e4-4080-516f-8f60-497bb3030425-exec-015-spec-003-partition-consumer-closed-sentinel
dark-factory-version: dev
created: "2026-11-14T17:45:00Z"
queued: "2026-11-14T16:49:53Z"
started: "2026-11-14T16:49:54Z"
completed: "2026-11-14T16:57:22Z"
branch: dark-factory/bug-partition-consumer-closed-sentinel
---

<summary>
- A closed partition-consumer channel is treated as normal teardown, not a fatal error.
- All four places where a closed channel can be observed report one recoverable condition.
- On that condition the partition consumer is recreated at the last successfully handled offset and reading continues.
- Recovery is bounded: three consecutive recreations with no successful batch between them, then the run fails.
- A successful batch resets the consecutive-attempt count, so intermittent rebalances never exhaust it.
- A permanently closed client fails immediately and never attempts recreation.
- Recreation failures and unrelated consume errors propagate exactly as they do today.
- Exactly one warning line is emitted per recovery; no metric and no configuration switch are added.
- No public API changes; every consumer built from this library gets the behaviour with no call-site change.
</summary>

<objective>
Make a closed partition-consumer `Errors()`/`Messages()` channel a recoverable teardown instead of a fatal error: return a package-level sentinel from all four closed-channel branches, have the existing recovery primitive recreate the partition consumer at the last good offset, bound consecutive recoveries at three, and fail immediately when the client itself is closed. This ends the `NUKE-PROD-AJ` failure where a routine rebalance or connection drop aborts every partition in the consumer.
</objective>

<context>
Read `specs/in-progress/003-bug-partition-consumer-closed-sentinel.md` — it is the contract; this prompt implements Desired Behaviors 1-6 and Acceptance Criteria 1-6.

Read `kafka_consumer-offset.go` and anchor on these symbols (do not rely on line numbers):
- `errSkipCorruptBatch` and `errOffsetOutOfRange` — the existing package-level sentinel convention (`var x = stderrors.New("...")`, where `stderrors "errors"` is the stdlib alias; the package also imports `github.com/bborbe/errors` as `errors`).
- `consumeMessages(ctx context.Context, consumePartition sarama.PartitionConsumer) ([]*sarama.ConsumerMessage, error)` — contains the four closed-channel branches (outer `Errors()`, outer `Messages()`, inner `Errors()`, inner `Messages()`).
- `recoverFromConsumeError` — the existing recreation primitive; its return contract is `(sarama.PartitionConsumer, Offset, bool, error)`. The run loop in `Consume` swaps `consumePartition` and `nextOffset` when `handled` is true.
- The per-partition run closure inside `Consume` — this is where `nextOffset` is tracked and where a successful batch calls `c.messageHandlerBatch.ConsumeMessages`.
- `skipAndAdvance` advances to a new healthy offset; `resetOnOffsetOutOfRange` resets to the fallback offset. Neither preserves the current offset, so neither is usable for this path.

Read `kafka_consumer-partition.go` — `CreatePartitionConsumer` is the existing creation primitive and must be reused:
```go
func CreatePartitionConsumer(
	ctx context.Context,
	consumerFromClient sarama.Consumer,
	metricsConsumer MetricsPartitionConsumer,
	topic Topic,
	partition Partition,
	fallbackOffset Offset,
	nextOffset Offset,
) (sarama.PartitionConsumer, error)
```

Read `kafka_consumer-offset_internal_test.go` — it holds the in-package fakes and helpers (`fakePartitionConsumer`, `testSaramaClient`, `testSaramaConsumer`, `fakeOffsetManager`, `fakeMessageHandlerBatch`, `newClosedRecoveryConsumer`, `captureStderrDuring`, `assertClosedRecoveryWarnLine`) and the four existing closed-channel tests.

Read `kafka_consumer-offset_exclusive_internal_test.go` — `exclusiveSaramaConsumer` enforces sarama's one-open-consumer-per-topic/partition rule. A recreation must close the dead consumer before creating its replacement.

Read `docs/dod.md` and `README.md` for repo conventions. The offset-consumer tests in `kafka_consumer-offset_internal_test.go` are plain `testing.T` tests with in-package fakes, NOT Ginkgo — the Ginkgo/Counterfeiter rule in `docs/dod.md` does not apply to that file.

Library facts (verified):
- `sarama.Consumer` has no `Closed()` method. The run already holds `saramaClient`, and `SaramaClient` (see `kafka_sarama-client-creator.go`) embeds `sarama.Client`, which has `Closed() bool`.
- `github.com/bborbe/errors` exposes `Is`, `As`, `Cause`, `Wrapf`, `Errorf` (see `errors_is.go`, `errors_as.go`, `errors_cause.go` in the module).
- `github.com/golang/glog` `Warningf` is the warning-level logger used elsewhere in this file.

Reference the coding plugin docs at their in-container paths:
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`

The working tree may already contain part of this change. If a named symbol or helper already exists, treat it as satisfying the corresponding requirement: verify its behaviour matches and do not redeclare it.
</context>

<requirements>
1. Add the package-level sentinel `errPartitionConsumerClosed` next to `errSkipCorruptBatch` and `errOffsetOutOfRange`, following the same convention:
   ```go
   var errPartitionConsumerClosed = stderrors.New("partition consumer closed")
   ```
   Do not wrap it when returning it from `consumeMessages`; callers must be able to match it with `errors.Is`.

2. Return `errPartitionConsumerClosed` (bare) from all four closed-channel branches of `consumeMessages`:
   - the outer `select` case `err, ok := <-consumePartition.Errors()` when `!ok`;
   - the outer `select` case `msg, ok := <-consumePartition.Messages()` when `!ok`;
   - the inner `select` case `err, ok := <-consumePartition.Errors()` when `!ok`;
   - the inner `select` case `msg, ok := <-consumePartition.Messages()` when `!ok`.
   Leave the `ctx.Done()`, corruption (`errSkipCorruptBatch`), offset-out-of-range (`errOffsetOutOfRange`), and `handleConsumerError` paths unchanged.

3. Give `recoverFromConsumeError` the sarama client so it can detect permanent closure. The target signature (keep the existing return contract) is:
   ```go
   func (c *offsetConsumer) recoverFromConsumeError(
   	ctx context.Context,
   	client sarama.Client,
   	consumer sarama.Consumer,
   	consumePartition sarama.PartitionConsumer,
   	partition Partition,
   	nextOffset Offset,
   	attempt int,
   	err error,
   ) (sarama.PartitionConsumer, Offset, bool, error)
   ```
   Pass `saramaClient` (the client obtained from `c.saramaClientProvider.Client(ctx)` in `Consume`) at the call site.

4. Handle the sentinel in `recoverFromConsumeError` before the corrupt-batch and offset-out-of-range branches:
   - If `client.Closed()` is true, return `(nil, Offset(0), false, nil)`. The run loop then wraps and returns the original error, so the run fails without attempting recreation.
   - If `attempt > maxConsecutivePartitionConsumerRecoveries`, return `(nil, Offset(0), false, nil)` so the error propagates.
   - Otherwise emit exactly one WARN line containing the literal phrase `partition consumer closed`, the topic, the partition, the `nextOffset`, and the attempt number (e.g. via `glog.Warningf`).
   - Close `consumePartition` before creating the replacement; log a Close error at `glog.V(4)` (same pattern as `skipAndAdvance` / `resetOnOffsetOutOfRange`).
   - Recreate with `CreatePartitionConsumer(ctx, consumer, c.metrics, c.topic, partition, c.offsetManager.FallbackOffset(), nextOffset)`.
   - On creation success return `(newPC, nextOffset, true, nil)` — the offset must be the recorded `nextOffset`, not a fallback and not an advanced offset.
   - On creation failure return `(nil, Offset(0), false, errors.Wrapf(ctx, createErr, "create partition consumer at offset %s failed", nextOffset))`; this propagates immediately and is not counted as a recovery attempt.
   - Do not call `skipAndAdvance` or `resetOnOffsetOutOfRange` from this branch.

5. Add the constant `maxConsecutivePartitionConsumerRecoveries = 3` (untyped int const). Keep the counter local to each partition run:
   - Declare `consecutiveClosedRecoveries := 0` inside the per-partition run closure in `Consume`; it must NOT be a field on `offsetConsumer`, which every partition shares.
   - When `consumeMessages` returns an error for which `errors.Is(err, errPartitionConsumerClosed)` is true, increment the counter before calling `recoverFromConsumeError`, and pass it as `attempt`.
   - Reset the counter to `0` after `c.messageHandlerBatch.ConsumeMessages(ctx, messages)` succeeds (a successful batch), before the next loop iteration.
   - Consequence: three consecutive sentinel-triggered recoveries are allowed; the next closed-channel result propagates and the run fails. Two partitions keep independent counters.

6. Update and extend `kafka_consumer-offset_internal_test.go`:
   - Change the four existing closed-channel tests (outer Errors, outer Messages, inner Errors, inner Messages) from `strings.Contains(err.Error(), "... channel closed")` to `errors.Is(err, errPartitionConsumerClosed)`. Each branch keeps its own test case.
   - Add a continuation test: the first consumer has a closed `Errors()` channel; the replacement delivers one message. Assert `errors.Is` on the sentinel path is exercised, that exactly one recreation happened, that the recreated consumer received the recorded `nextOffset`, that the dead consumer was closed before recreation, and that the delivered message reaches `fakeMessageHandlerBatch`.
   - Add a recovery test that asserts the recreation call receives the recorded `nextOffset` and that exactly one WARN line names the topic, that offset, and the attempt number. Reuse `captureStderrDuring` and `assertClosedRecoveryWarnLine` if present; the WARN assertion must fail if the line is not WARN level, if more than one line is emitted, or if any of the three fields is missing.
   - Add a closed-client test: `testSaramaClient{closed: true}` drives a closed partition consumer. Assert `Consume` returns a non-nil error containing `errPartitionConsumerClosed`, and that no recreation was attempted — i.e. the total `ConsumePartition` call count is exactly 1 (the initial create), so recreation attempts are 0.
   - Add a bounded test: every `ConsumePartition` call returns a consumer with a closed `Errors()` channel and no message is ever delivered. Assert `Consume` returns an error containing `errPartitionConsumerClosed` and that the total `ConsumePartition` call count is exactly 4 (1 initial create + 3 recoveries).
   - Add a counter-reset test: drive `recovery, recovery, successful batch, recovery, recovery`. Assert `Consume` does NOT return `errPartitionConsumerClosed` and only ends on `context.Canceled` after cancellation, proving the successful batch reset the counter (a never-reset counter would fail on the third recovery).
   - Extend `testSaramaClient` with a settable `closed bool` field and have `Closed()` return it if not already present.
   - Keep these tests as plain `testing.T` in-package fakes; do not convert them to Ginkgo.

7. Before finishing, re-run `<verification>`, confirm it passes, and walk each Acceptance Criterion in the spec against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- No new exported API; the change is internal to the consumer.
- Reuse the existing recreation primitive (`CreatePartitionConsumer`); do not build a second one. Do not route the closed-channel path through `skipAndAdvance` (it advances the offset) or `resetOnOffsetOutOfRange` (it resets to the fallback offset).
- No disable switch, no configuration knob, no metric. Exactly one warning log line is permitted on the new path.
- Do NOT treat an offset-out-of-range or corruption error as this class; those keep their own recovery paths.
- Do NOT swallow the error permanently: a closed client, or recovery that exhausts its consecutive attempts, must still end the run with an error.
- Do NOT change `run.CancelOnFirstError` or the per-partition run wiring.
- Do NOT touch the Kafka producer or the non-offset consumer.
- Existing tests must still pass, with one deliberate exception: the four closed-channel tests change their assertion from message text to the sentinel, as described in requirement 6.
- Frozen symbols for this change: `consumeMessages`, `recoverFromConsumeError`, `errPartitionConsumerClosed`.
- Follow the existing in-file patterns for sentinels, `github.com/bborbe/errors` wrapping, and `glog` levels.
</constraints>

<verification>
`make precommit` — must pass from the project root.

`go test -run 'PartitionConsumerClosed|ConsumeMessages|RecoverFromConsumeError' -count=1 .` — must pass.
</verification>

<!--
Reviewer notes (not instructions for the executing agent):

1. The spec's AC5 evidence says "asserts the error on the third" recovery, while the code path allows three recoveries and propagates the next (fourth) closed-channel event. Requirement 5 encodes the implementation-consistent reading: 3 recoveries allowed, total create count 4 (1 initial + 3 recoveries), then the error. Confirm this is the intended boundary before approval.

2. AC4 says "a create-call count of 0". Because the initial partition consumer is also created via ConsumePartition, the test asserts a total call count of 1 (initial create) and therefore 0 recreation attempts. Requirement 6 states this explicitly.

3. As of HEAD (commit 6d79267, released v1.28.2) this change already exists in `kafka_consumer-offset.go`, `kafka_consumer-offset_internal_test.go`, and the CHANGELOG (v1.28.2 section). If this spec was re-opened intentionally, the prompt is a verify/no-op run; if not, the generation may be a duplicate re-trigger. Do not add a new CHANGELOG entry: the existing one is already released.
-->
