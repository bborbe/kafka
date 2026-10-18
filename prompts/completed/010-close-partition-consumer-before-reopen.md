---
status: completed
summary: Closed old partition consumers before probing/reopening the same partition in corrupt-batch skip and offset-out-of-range reset, and made Consume close the current replacement consumer on exit, with an exclusive-consumer test fake enforcing sarama's one-consumer-per-partition rule
execution_id: kafka-skip-probe-exec-010-close-partition-consumer-before-reopen
dark-factory-version: dev
created: "2026-10-18T12:00:00Z"
queued: "2026-10-18T16:50:59Z"
started: "2026-10-18T16:51:50Z"
completed: "2026-10-18T16:56:37Z"
---

# Close the Partition Consumer Before Reopening the Same Partition

<summary>
- Skipping a corrupt batch works against a real broker instead of failing every time
- Today the skip path probes the partition while the consumer that hit the corruption is still open, and the Kafka client refuses a second consumer on the same topic/partition, so the skip always fails with "That topic/partition is already being consumed"
- The offset-out-of-range self-heal has the same defect: it opens the replacement consumer before closing the stuck one
- Both recovery paths now release the old consumer before opening anything else on that partition
- The consume loop always closes whichever consumer is current when it exits, so a replacement consumer is never leaked
- A new test fake enforces the real client's one-consumer-per-partition rule, so this class of bug can no longer hide behind permissive fakes
- No public API changes; consumers with both options off behave exactly as before
</summary>

<objective>
Make `skipAndAdvance` and `resetOnOffsetOutOfRange` in the offset consumer close the old partition consumer before any other `ConsumePartition` call on the same topic/partition, because sarama allows only one open partition consumer per topic/partition per `sarama.Consumer` and returns `ConfigurationError("That topic/partition is already being consumed")` otherwise. This bug makes `WithSkipCorruptBatches(true)` fail on every real corrupt batch (observed in production on v1.26.0, still present on v1.28.0).
</objective>

<context>
No `CLAUDE.md` exists in this repo. Read `README.md` and `docs/dod.md` for conventions. Errors use `github.com/bborbe/errors` (`errors.Wrapf`). Logging uses `github.com/golang/glog`. Offset-consumer tests are plain `testing.T` in-package fakes, NOT Ginkgo.

Read these before changing anything:
- `kafka_consumer-offset.go`:
  - `skipAndAdvance` calls `c.skipper.FindNextHealthyOffset`, which probes via `defaultCorruptionSkipper.isOffsetGood` → `consumer.ConsumePartition` on the same topic/partition.
  - Then it calls `CreatePartitionConsumer`. Only after both does it call `oldPartitionConsumer.Close()`.
  - `resetOnOffsetOutOfRange` calls `CreatePartitionConsumer` before `oldPartitionConsumer.Close()`, the same ordering defect.
  - `Consume` uses `defer consumePartition.Close()`. Go evaluates that receiver at defer time, so it closes the *original* consumer and never closes a replacement that `recoverFromConsumeError` assigned.
- `kafka_consumer-partition.go`: `CreatePartitionConsumer`.
- `kafka_consumer-offset_internal_test.go`:
  - Fakes to extend: `testSaramaConsumer`, `fakePartitionConsumer`, `fakeSkipper`, `fakeMetricsConsumer`, `buildSkipTestConsumer`.
  - Existing tests `TestSkipAndAdvance_*` and the out-of-range reset tests.
  - `fakePartitionConsumer.Close` is currently a no-op returning nil.
  - This file is `package kafka`, so it cannot use `mocks/`; extend the hand-written fakes.
- Sarama facts, verified in vendored v1.61.1 `consumer.go`:
  - `ConsumePartition` rejects a topic/partition that already has an open child.
  - `partitionConsumer.Close()` is idempotent (`closeOnce`) and returns only after the dispatcher has removed the child. A synchronous `Close()` therefore frees the partition for the next `ConsumePartition`.
</context>

<requirements>
1. In `skipAndAdvance`, read `HighWaterMarkOffset()` from the old consumer first, as today. Then close the old partition consumer (log a Close error at `glog.V(4)` as the existing code does). Only after that call `FindNextHealthyOffset` and `CreatePartitionConsumer`. Remove the later `oldPartitionConsumer.Close()` call.
2. In `resetOnOffsetOutOfRange`, close the old partition consumer as the first action, before `ResetOffset` (log a Close error at `glog.V(4)`), so every return path including a `ResetOffset` error leaves it closed. Keep `ResetOffset` before `CreatePartitionConsumer` as today. Remove the later Close call.
3. In both functions, an error after the old consumer was closed still returns `(nil, 0, wrapped error)`, as today. Update the doc comments to state that the old consumer is always closed when the function returns, on success and on error.
4. In `Consume`, replace `defer consumePartition.Close()` with a deferred closure that closes the current value of `consumePartition` at exit. A replacement consumer must be closed when the partition run returns. Closing an already-closed sarama partition consumer is safe. Log a Close error at `glog.V(4)` so errcheck stays clean.
5. Test fakes in `kafka_consumer-offset_internal_test.go`:
   - Add a conflict-enforcing sarama consumer fake, e.g. `exclusiveSaramaConsumer`. It tracks open topic/partition pairs and returns an error from `ConsumePartition` whose message contains `That topic/partition is already being consumed` when the pair is already open.
   - The partition consumers it hands out release the pair on `Close()`. `Close()` must be idempotent (guard with `sync.Once`; never close a channel twice) because `Consume`'s deferred close runs again on a consumer a failed recovery already closed. Guard the open-pair map and offset record with a `sync.Mutex`.
   - It records every `ConsumePartition` offset.
   - Give it a way to make probes at chosen offsets yield a buffered message (healthy) and others a buffered corruption error (`sarama.PacketDecodingError`) on `Errors()`, so the real `defaultCorruptionSkipper` can run against it. Never leave a probe with nothing to read — `isOffsetGood` would block the full 5s `probeTimeout`.
   - Give it an optional set of offsets at which `ConsumePartition` returns a non-conflict error, so `CreatePartitionConsumer`-error tests run against the exclusive fake.
6. New tests, plain `testing.T`:
   - `skipAndAdvance` with the real `defaultCorruptionSkipper` and the exclusive fake. The old consumer was opened through the fake and is still open. Expect: success, the returned offset equals the first healthy offset, the metric is incremented once, the old consumer is closed, and exactly one pair (the new consumer) is open at the end.
   - The same setup run against the pre-fix ordering must fail. Do not keep pre-fix code; just make sure the assertion would catch it, e.g. the test fails if any `ConsumePartition` returned the conflict error.
   - `resetOnOffsetOutOfRange` with the exclusive fake: success, consumer recreated at the fallback offset, old consumer closed.
   - Error paths: `skipAndAdvance` with `FindNextHealthyOffset` error and with `CreatePartitionConsumer` error; `resetOnOffsetOutOfRange` with `ResetOffset` error and with `CreatePartitionConsumer` error — in each, the old consumer is closed and nothing is left open (extend the existing reset error tests with these assertions).
   - Update `TestSkipAndAdvance_CreatePartitionConsumerError`, whose comment says "without closing the old consumer", to the new contract.
   - A `Consume`-level test: a corrupt batch is skipped, then the run exits (cancel the context); the replacement consumer is closed and no pair is left open.
   - Name every new test to match `Exclusive` (e.g. `TestSkipAndAdvance_Exclusive_*`, `TestResetOnOffsetOutOfRange_Exclusive_*`, `TestConsume_Exclusive_ReplacementClosedOnExit`).
7. Add under a new `## Unreleased` heading at the top of `CHANGELOG.md` (above `## v1.28.0`): `- fix: Close the old partition consumer before probing or reopening the same partition in corrupt-batch skip and offset-out-of-range reset, which previously failed against a real broker with "That topic/partition is already being consumed"`.
8. Before finishing, re-run `<verification>`, confirm it passes, and walk each requirement against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- No exported API changes (`WithSkipCorruptBatches`, `WithAutoResetOffsetOutOfRange`, `ConsumerOptions`, constructors stay identical)
- Do not change `defaultCorruptionSkipper`'s search algorithm, `CreatePartitionConsumer`, or `kafka_consumer-error-handler.go`
- Behaviour with both options OFF is unchanged
- Existing tests must still pass (adjusting only the one whose stated contract this fix changes)
- Use `errors.Wrapf(ctx, ...)` from `github.com/bborbe/errors`, never `fmt.Errorf`
- `docs/dod.md`'s Ginkgo/Counterfeiter rule does not apply to `kafka_consumer-offset_internal_test.go` (package-internal, plain `testing.T`, hand-written fakes); follow that file's existing style
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.
Run `go test -run 'SkipAndAdvance|OffsetOutOfRange|Exclusive' -count=1 .` — must pass.
</verification>
