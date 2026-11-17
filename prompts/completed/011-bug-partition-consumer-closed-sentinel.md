---
status: completed
spec: [003-bug-partition-consumer-closed-sentinel]
summary: Closed partition-consumer channels now return a sentinel and are recovered by recreating the consumer at the last good offset, bounded to three consecutive attempts with per-partition counting
execution_id: kafka-aj-consumer-close-exec-011-bug-partition-consumer-closed-sentinel
dark-factory-version: v0.198.7
created: "2026-10-19T17:35:15Z"
queued: "2026-10-19T17:35:15Z"
started: "2026-10-19T17:35:17Z"
completed: "2026-10-19T17:38:22Z"
---

# Recover from a closed partition consumer instead of aborting the consumer

<summary>
- A closed partition-consumer channel becomes a recoverable class instead of a fatal error
- The four closed-channel branches return one sentinel
- Recovery closes the dead consumer and creates a new one at the last good offset
- Recovery is bounded at three consecutive attempts, counted per partition; a successful batch resets the counter
- A closed client fails immediately without attempting recreation
- The four existing closed-channel tests change their assertion from message text to the sentinel
</summary>

<objective>
`NUKE-PROD-AJ` fires whenever a Kafka rebalance or connection drop closes a partition consumer's `Errors()`/`Messages()` channels: the consumer returns a fresh error, `recoverFromConsumeError` does not recognise it, and `run.CancelOnFirstError` aborts every partition. Make that teardown recoverable so the run continues from the last good offset.
</objective>

<context>
Read `specs/003-bug-partition-consumer-closed-sentinel.md` — it is the contract; this prompt implements its Desired Behaviors 1-6.

Read `kafka_consumer-offset.go`:
- `consumeMessages` — the four closed-channel branches at `:536`, `:555`, `:570`, `:589`, each returning `errors.Errorf(ctx, "partition consumer … channel closed")`.
- `recoverFromConsumeError` — returns `(sarama.PartitionConsumer, Offset, bool, error)`; handles only `errSkipCorruptBatch` and `errOffsetOutOfRange`.
- The run loop at `:438-457` already swaps `consumePartition` and `nextOffset` when `handled` is true.
- `skipAndAdvance` and `resetOnOffsetOutOfRange` both recreate, but neither preserves the current offset: `skipAndAdvance` finds a *new healthy* offset and `resetOnOffsetOutOfRange` resets to the fallback. Neither is usable for this path.
- `errSkipCorruptBatch` / `errOffsetOutOfRange` at `:138` and `:141` show the sentinel convention to follow.

Read `kafka_consumer-offset_internal_test.go:474-550` — four tests assert the current message text via `strings.Contains(err.Error(), "… channel closed")`. They must change to `errors.Is(err, errPartitionConsumerClosed)`.

Read `kafka_consumer-offset_exclusive_internal_test.go` — `exclusiveSaramaConsumer` enforces sarama's one-open-consumer-per-topic/partition rule, and `prompts/completed/010-close-partition-consumer-before-reopen.md` records the close-before-reopen ordering that `skipAndAdvance` and `resetOnOffsetOutOfRange` already follow. A recreation must close the dead consumer first.

The client guard uses `sarama.Client.Closed()`. `sarama.Consumer` has no `Closed()`; the run already holds the client.

Read `README.md` and `docs/dod.md` for repo conventions. Offset-consumer tests are plain `testing.T` in-package fakes, NOT Ginkgo — `docs/dod.md`'s Ginkgo/Counterfeiter rule does not apply to `kafka_consumer-offset_internal_test.go`.
</context>

<requirements>
1. Add a package-level sentinel `errPartitionConsumerClosed` next to the existing sentinels, and return it from all four closed-channel branches in `consumeMessages` (both `Errors()` branches and both `Messages()` branches).
2. In `recoverFromConsumeError`, recognise the sentinel and recreate the partition consumer at the last good `nextOffset`, returning `handled=true`: close `consumePartition` first (log a Close error at `glog.V(4)`, as `skipAndAdvance` does), then call `CreatePartitionConsumer(ctx, consumer, c.metrics, c.topic, partition, c.offsetManager.FallbackOffset(), nextOffset)` and return the new consumer with `nextOffset` unchanged. Do **not** call `skipAndAdvance` (it advances the offset) or `resetOnOffsetOutOfRange` (it resets to the fallback offset).
3. Add a `client sarama.Client` parameter to `recoverFromConsumeError` and pass `saramaClient` at the run-loop call site. When `client.Closed()` reports true, do not attempt recreation — return the error so it propagates as today. Extend `testSaramaClient` with a settable `closed` field so a test can drive it.
4. Bound recovery at **three consecutive sentinel-triggered recoveries with no successful batch between them**. The counter is a **local variable in the per-partition run closure in `Consume`**, passed into `recoverFromConsumeError` as the attempt number — never a field on `offsetConsumer`, which every partition shares. A successful batch resets the counter. A `CreatePartitionConsumer` error is not one of these attempts — it propagates immediately, unchanged. There is no delay between attempts. After the third recovery the next sentinel error propagates, so the run emits three WARN lines and then fails.
5. Emit exactly one WARN log line per recovery naming the partition, the `nextOffset` and the attempt number. Add no metric.
6. Update the four existing closed-channel tests to assert `errors.Is(err, errPartitionConsumerClosed)` — those four cases are the per-branch coverage — and add tests for: continuation after recreation (the recreated fake delivers a message that reaches the batch handler, and the recreation call count is exactly 1); the recreation receives the recorded `nextOffset` and exactly one WARN line carries the partition, offset and attempt number; `createCalls == 0` when the client is closed; the bounded case (three consecutive recoveries with no batch → error) and the reset case (`recovery, recovery, successful batch, recovery, recovery` → no error).
7. Add under a new `## Unreleased` heading at the top of `CHANGELOG.md`: `- fix: Treat a closed partition consumer channel as recoverable and recreate the consumer at the last good offset instead of aborting every partition`.
8. Before finishing, re-run `<verification>`, confirm it passes, and walk each requirement against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- No new exported API; the change is internal to the consumer
- Do NOT build a second recreation primitive — reuse `recoverFromConsumeError`'s existing return contract and `CreatePartitionConsumer`; do not call `skipAndAdvance` or `resetOnOffsetOutOfRange` from the new branch
- Do NOT change `run.CancelOnFirstError`, the producer, or the non-offset consumer
- Do NOT add a metric, a configurable threshold, or a disable switch
- Do NOT change the offset-out-of-range or corruption paths
- Existing tests pass, except the four closed-channel assertions named in requirement 6
</constraints>

<verification>
`make precommit` — must pass from the project root.
`go test -run 'PartitionConsumerClosed|ConsumeMessages' -count=1 .` — must pass
</verification>
