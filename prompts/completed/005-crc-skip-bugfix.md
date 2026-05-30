---
status: completed
summary: 'Fixed CRC skip bug: skipAndAdvance now recreates partition consumer at healthy offset, threaded ctx through binary search, replaced fmt.Errorf sentinel with stderrors.New, added end-to-end test, and created .maintainer.yaml'
container: kafka-crc-skip-exec-005-crc-skip-bugfix
dark-factory-version: v0.173.0
created: "2026-05-30T19:26:05Z"
queued: "2026-05-30T19:26:05Z"
started: "2026-05-30T19:26:24Z"
completed: "2026-05-30T19:41:11Z"
---

# Fix CRC Skip Bug: skipAndAdvance Must Recreate the Partition Consumer

## Goal

Fix correctness bugs in the opt-in CRC-skip implementation in `kafka_consumer-offset.go`. The skip-and-advance path currently **does not work**: it closes the partition consumer but never recreates or reassigns it, so after a skip the consume loop reads from a closed consumer. Fix that and the related issues found in review, and add the missing end-to-end test that would have caught it.

This is a bugfix on top of the existing CRC-skip feature already on this branch. Read `kafka_consumer-offset.go` (the `Consume` loop ~lines 283-412, `skipAndAdvance` ~lines 473-526, `FindNextHealthyOffset`/`binarySearchEndOfCorruption`/`isOffsetGood`) and `kafka_consumer-offset_internal_test.go` in full before changing anything.

## 🚨 The core bug

In `Consume()` the skip branch is:
```go
if c.consumerOptions.SkipCorruptBatches && errors.Is(err, errSkipCorruptBatch) {
    if skipErr := c.skipAndAdvance(ctx, consumerFromClient, consumePartition, Partition(partition), nextOffset); skipErr != nil {
        return errors.Wrapf(ctx, skipErr, "skip and advance failed")
    }
    continue   // ← loops back to consumeMessages(ctx, consumePartition) with the SAME consumer
}
```
`skipAndAdvance` calls `oldPartitionConsumer.Close()`, finds the new offset, logs it — and returns `nil` **without creating a new partition consumer**. After `continue`, the loop calls `c.consumeMessages(ctx, consumePartition)` on the **closed** consumer → reads from a dead consumer forever (or panics). The offset is never actually advanced for the running loop. The skip is non-functional.

## Requirements

### 1. skipAndAdvance must recreate and return the new partition consumer + offset
Change `skipAndAdvance` to:
- find the first healthy offset (existing `FindNextHealthyOffset`)
- close the old partition consumer
- **create a new partition consumer at the new offset** via the existing `CreatePartitionConsumer(ctx, consumer, c.metrics, c.topic, partition, c.offsetManager.FallbackOffset(), newOffset)`
- when `FindNextHealthyOffset` returns `-1` (corruption to end of partition), recreate at the high-water mark so the consumer tails new messages
- increment the metric `CorruptBatchSkippedCounterInc`
- log the skipped range (corrupt-start → first-healthy)
- **return `(sarama.PartitionConsumer, Offset, error)`** — the new consumer and the offset it was created at

The `Consume()` loop must **reassign** its `consumePartition` and `nextOffset` from the return values before `continue`, so the next `consumeMessages` reads from the new consumer:
```go
newPC, newOff, skipErr := c.skipAndAdvance(ctx, consumerFromClient, consumePartition, Partition(partition), nextOffset)
if skipErr != nil { return errors.Wrapf(ctx, skipErr, "skip and advance failed") }
consumePartition = newPC
nextOffset = newOff
continue
```
Consumer-close lifecycle: the existing `defer consumePartition.Close()` (line ~339) closes whatever `consumePartition` points to at function exit — after reassignment that's the **final** (newest) consumer, closed exactly once. `skipAndAdvance` closes the **old** consumer exactly once internally. So each intermediate consumer is closed once; no double-close of the same object, no leak. Do not add extra Close calls.

### 2. Thread ctx through the binary search
`binarySearchEndOfCorruption` currently calls `s.isOffsetGood(context.Background(), ...)`. Pass the real `ctx` from `FindNextHealthyOffset` down into `binarySearchEndOfCorruption` and on to `isOffsetGood`, so probe cancellation (shutdown) is honored. No `context.Background()` anywhere in the skip path.

### 3. Sentinel error via bborbe/errors, not fmt.Errorf
`var errSkipCorruptBatch = fmt.Errorf("skip corrupt batch")` violates the repo error convention. Replace with a stdlib sentinel: `var errSkipCorruptBatch = stderrors.New("skip corrupt batch")` (import `stderrors "errors"`), or match however other sentinels in this package are declared. Keep `errors.Is(err, errSkipCorruptBatch)` working. Do NOT use `fmt.Errorf` for a static-message error.

### 4. End-to-end skip test that crosses the recreation boundary (the gap that hid the bug)
Add an in-package test (plain `testing.T`, the `fakePartitionConsumer` pattern — NOT Ginkgo) that exercises the **partition-consume loop's skip-and-reassign path**, not just `consumeMessages`. A `consumeMessages`-only test does NOT cross the recreation boundary and would NOT catch this bug — so it is insufficient.

The test MUST make a **second `ConsumePartition`/`CreatePartitionConsumer` call at the advanced offset observable.** Two acceptable approaches — pick one:
- **(a) Drive `Consume()` through the seam.** Inject: a fake `SaramaClientProvider` (interface in `kafka_sarama-client-provider.go`) whose client's `Partitions()` returns exactly one partition; a `saramaConsumerFunc` returning a fake `sarama.Consumer` whose `ConsumePartition` is **scripted** — first call returns a fake partition consumer that emits a `PacketDecodingError`, second call returns one that emits the good messages then a terminating signal; a fake `skipper` returning a known healthy offset; fake `offsetManager` and `messageHandlerBatch`. Because `Consume()` blocks in `run.CancelOnFirstError` once messages drain, the test MUST provide a termination mechanism (cancel the ctx after the good message is observed, or have the fake handler/consumer return a sentinel that ends the loop). Assert: (i) `ConsumePartition` was called a **second time at the advanced offset** (capture the offset arg), (ii) the good messages after the corrupt batch reached the message handler, (iii) the corrupt-batch metric fired once.
- **(b) Test `skipAndAdvance` directly + the reassignment.** Call `skipAndAdvance` with a fake `sarama.Consumer` (scripted `ConsumePartition`) and a fake `skipper`; assert it **returns a non-nil new partition consumer created at the advanced offset** (capture the offset passed to `ConsumePartition`), returns that offset, and incremented the metric once. Then a small assertion that the `Consume` loop reassigns from the return (can be a focused test of the reassignment contract).

For the metric assertion use a **hand fake** implementing the `metrics` interface (`MetricsConsumer` + `MetricsPartitionConsumer`, see `kafka_metrics.go`) that counts `CorruptBatchSkippedCounterInc` calls — the package currently uses zero `testutil`, so match that style; do not introduce `prometheus/testutil`.

This test must FAIL against the current broken `skipAndAdvance` (which never recreates) and PASS after requirement 1.

### 5. Add .maintainer.yaml (repo gate enablement)
Create `.maintainer.yaml` at the repo root so the pr-reviewer bot's APPROVE counts toward the branch-protection review gate:
```yaml
prReviewer:
  autoApprove: true
release:
  autoRelease: true
```

## Constraints

- Default behavior (skip OFF) stays unchanged — crash on `PacketDecodingError`. `grep -n 'return nil' kafka_consumer-error-handler.go` must still return 0 lines (error handler untouched).
- No `context.Background()` in the skip/probe path.
- No `fmt.Errorf` for the sentinel.
- Tests: in-package `testing.T` + `fakePartitionConsumer`, NOT Ginkgo.
- The skip must advance by recreating the partition consumer — never swallow-and-reread.
- Use `github.com/bborbe/errors` `Wrapf` for wrapped errors (already the pattern in this file).

## Acceptance Criteria

- [ ] `skipAndAdvance` returns `(sarama.PartitionConsumer, Offset, error)` and creates a new partition consumer at the healthy offset (or high-water mark when range runs to end) — `grep -n` shows the new signature + a `CreatePartitionConsumer` call inside it.
- [ ] The `Consume()` skip branch reassigns `consumePartition` and `nextOffset` from `skipAndAdvance`'s return before `continue`.
- [ ] No `context.Background()` in `kafka_consumer-offset.go` skip path — `grep -n 'context.Background()' kafka_consumer-offset.go` returns 0.
- [ ] Sentinel uses stdlib/bborbe errors, not fmt — `grep -n 'fmt.Errorf' kafka_consumer-offset.go` returns 0.
- [ ] New test crosses the recreation boundary: it makes a **second `ConsumePartition` at the advanced offset observable** (captured offset arg) and asserts good messages delivered + metric +1. It FAILS against the current broken `skipAndAdvance` and PASSES after the fix. `go test` exit 0.
- [ ] `.maintainer.yaml` exists at repo root with `prReviewer.autoApprove: true` + `release.autoRelease: true`.
- [ ] `make precommit` exits 0.

## Validation

```
make precommit
```
Expected: exit 0; new and existing tests pass; `grep -n 'context.Background()' kafka_consumer-offset.go` and `grep -n 'fmt.Errorf' kafka_consumer-offset.go` both return 0.

## Out of Scope

- The optional compaction-gap branch in `FindNextHealthyOffset` (intentionally omitted).
- Throughput/lag concerns.
- Trading-side `SKIP_CRC_CORRUPTION` arg (separate follow-up after this lib is tagged).
