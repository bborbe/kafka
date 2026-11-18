---
status: completed
spec: [003-bug-partition-consumer-closed-sentinel]
summary: Verified the closed-partition-consumer sentinel recovery implementation and tests already in the tree, and added the missing Unreleased CHANGELOG entry
execution_id: 28418769-b3b0-5b00-91ca-5793cfadaa49-exec-015-bug-partition-consumer-closed-sentinel
dark-factory-version: dev
created: "2026-11-15T15:28:59Z"
queued: "2026-11-15T15:39:49Z"
started: "2026-11-15T15:39:52Z"
completed: "2026-11-15T15:47:34Z"
branch: dark-factory/bug-partition-consumer-closed-sentinel
---

# Recover from a closed partition consumer instead of aborting the consumer

<summary>
- A closed partition-consumer channel becomes a recoverable teardown instead of a fatal error
- All four closed-channel branches in `consumeMessages` return one package-level sentinel
- The existing recovery path closes the dead consumer and recreates it at the last good offset
- Recovery is bounded to three consecutive sentinel-triggered attempts per partition; a successful batch resets the counter
- A permanently closed client fails immediately without attempting recreation
- Genuine consume errors and `CreatePartitionConsumer` failures still propagate unchanged
- The four existing closed-channel tests switch from message-text assertions to the sentinel
- New tests prove continuation after recreation, offset preservation, the WARN line, the client guard, the bound, and the reset
</summary>

<objective>
`NUKE-PROD-AJ` fires whenever a Kafka rebalance or connection drop closes a partition consumer's `Errors()`/`Messages()` channels: `consumeMessages` returns a freshly formatted error, `recoverFromConsumeError` does not recognise it, and `run.CancelOnFirstError` aborts every partition. Make that teardown recoverable so the run continues from the last good offset, for every consumer built from this library, with no call-site change.
</objective>

<context>
Read `specs/in-progress/003-bug-partition-consumer-closed-sentinel.md` — it is the contract. This prompt implements its Desired Behaviors 1-6 and Acceptance Criteria 1-6.

Read `kafka_consumer-offset.go`:
- `consumeMessages` — the four closed-channel branches: the outer `Errors()` and `Messages()` cases of the first `select`, and the inner `Errors()` and `Messages()` cases of the second `select`.
- `recoverFromConsumeError` — returns `(sarama.PartitionConsumer, Offset, bool, error)`; today it handles only `errSkipCorruptBatch` and `errOffsetOutOfRange`.
- The per-partition `run.Func` closure inside `Consume` — it already swaps `consumePartition` and `nextOffset` when `handled` is true.
- `skipAndAdvance` and `resetOnOffsetOutOfRange` — both recreate a partition consumer, but neither preserves the current offset: `skipAndAdvance` searches for a new healthy offset and `resetOnOffsetOutOfRange` resets to the fallback. Neither is usable for this path.
- `errSkipCorruptBatch` and `errOffsetOutOfRange` — the package-level sentinel convention to follow (`stderrors.New`, next to the other sentinels).

Read `kafka_consumer-partition.go` for the exact `CreatePartitionConsumer` signature:
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

Read `kafka_consumer-offset_internal_test.go` — plain `testing.T` in-package tests with hand-written fakes (`testSaramaConsumer`, `testSaramaClient`, `fakePartitionConsumer`, `fakeOffsetManager`, `fakeMessageHandlerBatch`, `fakeMetricsConsumer`, `testSaramaClientProvider`, `fakeLogSampler`, `fakeWaiter`). The four existing closed-channel tests assert the old message text via `strings.Contains(err.Error(), "… channel closed")` and must change. `docs/dod.md`'s Ginkgo/Counterfeiter rule does NOT apply here — keep the existing plain-`testing` in-package style.

The client guard uses `sarama.Client.Closed()`. `sarama.Consumer` has no `Closed()` method; the run already holds the `sarama.Client`.

Read `CHANGELOG.md` for the entry format and `docs/dod.md` for repo conventions.

Relevant coding guides (paths are inside the execution container):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — sentinel errors, `errors.Is`, `errors.Wrapf` with `github.com/bborbe/errors`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` — `glog.Warningf` vs `glog.V(n).Infof`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — behaviour-focused tests
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — CHANGELOG entry format
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — completion checklist

The current tree may already contain part of this change from a prior attempt on the same spec. Read the file first; treat the requirements below as the target state, verify each, and fill only the gaps (in particular the tests in requirement 6 and the CHANGELOG entry).
</context>

<requirements>
1. Add a package-level sentinel next to `errSkipCorruptBatch` and `errOffsetOutOfRange`:
   ```go
   var errPartitionConsumerClosed = stderrors.New("partition consumer closed")
   ```
   Add `const maxConsecutivePartitionConsumerRecoveries = 3` beside it.

2. In `consumeMessages`, return `errPartitionConsumerClosed` (not a formatted error) from all four closed-channel branches, each of which is currently an `if !ok { ... }` guard:
   - outer `select`, `case err, ok := <-consumePartition.Errors():`
   - outer `select`, `case msg, ok := <-consumePartition.Messages():`
   - inner `select`, `case err, ok := <-consumePartition.Errors():`
   - inner `select`, `case msg, ok := <-consumePartition.Messages():`
   Do not change any other behaviour in `consumeMessages` (corruption skip, offset-out-of-range reset, error-handler path, context cancellation, batch-size return).

3. In `recoverFromConsumeError`, add a first branch keyed on `errors.Is(err, errPartitionConsumerClosed)` before the `errSkipCorruptBatch` and `errOffsetOutOfRange` branches:
   - If `client.Closed()` is true, or `attempt > maxConsecutivePartitionConsumerRecoveries`, return `(nil, Offset(0), false, nil)`. Returning `handled=false` makes the caller propagate the original error; do NOT attempt recreation when the client is closed.
   - Otherwise emit exactly one WARN line naming the partition, the `nextOffset`, and the attempt number:
     ```go
     glog.Warningf(
         "partition consumer closed in topic(%s) partition(%d) at offset %s, recreating consumer (attempt %d)",
         c.topic, partition, nextOffset, attempt,
     )
     ```
   - Close the dead consumer first (log a close error at `glog.V(4)` only, as `skipAndAdvance` does), then recreate:
     ```go
     newPC, createErr := CreatePartitionConsumer(
         ctx, consumer, c.metrics, c.topic, partition,
         c.offsetManager.FallbackOffset(), nextOffset,
     )
     ```
   - On success return `(newPC, nextOffset, true, nil)` — `nextOffset` is unchanged, so the recreated consumer resumes at the last handled offset + 1.
   - On `createErr != nil`, return `(nil, Offset(0), false, errors.Wrapf(ctx, createErr, "create partition consumer at offset %s failed", nextOffset))` so it propagates immediately. A `CreatePartitionConsumer` failure is not one of the bounded recovery attempts: do not retry and do not count it.
   - Do NOT call `skipAndAdvance` or `resetOnOffsetOutOfRange` from this branch. Do not add a metric. Do not add a delay between attempts.

4. Add a `client sarama.Client` parameter to `recoverFromConsumeError` and pass `saramaClient` at the run-loop call site. The resulting signature is:
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

5. In the per-partition `run.Func` closure inside `Consume`, track the bound with a local variable, never a field on `offsetConsumer` (a field would be shared by every partition):
   - Declare `consecutiveClosedRecoveries := 0` after the initial `CreatePartitionConsumer` and before the `for {}` loop.
   - When `consumeMessages` returns an error, increment the counter only if `errors.Is(err, errPartitionConsumerClosed)`, then call `recoverFromConsumeError(ctx, saramaClient, consumerFromClient, consumePartition, Partition(partition), nextOffset, consecutiveClosedRecoveries, err)`.
   - If the recovery returns a non-nil error, return it. If `handled`, assign `consumePartition = newPC` and `nextOffset = newOff` and `continue`. Otherwise return `errors.Wrapf(ctx, err, "consume failed")`.
   - After `messageHandlerBatch.ConsumeMessages` succeeds, reset `consecutiveClosedRecoveries = 0` before updating `nextOffset` and marking the offset.
   - Net effect: one initial create plus at most three recreations (four `ConsumePartition` calls) with no successful batch; the next sentinel error propagates. A successful batch resets the counter.

6. Tests in `kafka_consumer-offset_internal_test.go` (plain `testing.T`, existing fakes; no Ginkgo, no Counterfeiter):
   - Change the four existing tests `TestConsumeMessages_OuterErrorsChannelClosed`, `TestConsumeMessages_OuterMessagesChannelClosed`, `TestConsumeMessages_InnerErrorsChannelClosed`, `TestConsumeMessages_InnerMessagesChannelClosed` to assert `errors.Is(err, errPartitionConsumerClosed)` instead of the old message text. These four cases are the per-branch coverage required by AC2.
   - Add a continuation test: a fake partition consumer whose `Errors()` channel is closed triggers exactly one recreation, and the recreated fake delivers a message that reaches `fakeMessageHandlerBatch`. Assert `errors.Is` on the sentinel where relevant, exactly one recreation, that the recreation is at `nextOffset`, that the dead consumer was closed, and that the message offset is the expected one. Recreation alone is not the claim — the delivered message is.
   - Add a recovery test that calls `recoverFromConsumeError` directly with `errPartitionConsumerClosed` and attempt 2: assert `handled == true`, the returned consumer is the recreated one, the returned offset equals `nextOffset`, the old consumer was closed, exactly one recreation occurred at `nextOffset`, and exactly one WARN line contains the topic, the offset, and `attempt 2`. Capture glog output by setting the `logtostderr` flag and piping `os.Stderr` for the duration of the call.
   - Add a closed-client test: drive `testSaramaClient` with `closed: true`. Assert `Consume` returns a non-nil error whose chain satisfies `errors.Is(err, errPartitionConsumerClosed)`, and assert zero recreation attempts — the only `ConsumePartition` call is the initial create (fake call count is 1, not 0).
   - Add a bounded test: drive four consecutive closed-channel consumers with no successful batch. Assert `Consume` returns an error satisfying `errors.Is(err, errPartitionConsumerClosed)` and the fake recorded 4 `ConsumePartition` calls (1 initial create + 3 recreations).
   - Add a reset test: drive `recovery, recovery, successful batch, recovery, recovery`. Assert exactly one successful batch reached the handler, that `Consume` ends on context cancellation rather than the sentinel (a never-reset counter would propagate on the third recovery, so this sequence discriminates), and that the fake recorded 6 `ConsumePartition` calls.
   - Add a `closed bool` field to `testSaramaClient` and implement `func (f *testSaramaClient) Closed() bool { return f.closed }` if not already present.

7. Add under a new `## Unreleased` heading at the top of `CHANGELOG.md`:
   ```
   - fix: Treat a closed partition consumer channel as recoverable and recreate the consumer at the last good offset instead of aborting every partition
   ```

8. Before finishing, run the `<verification>` commands from the project root, confirm they pass, and walk each numbered requirement against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- No new exported API; the change is internal to the consumer
- Reuse the existing recreation primitive (`CreatePartitionConsumer` and `recoverFromConsumeError`'s return contract); do NOT build a second recreation path and do NOT call `skipAndAdvance` or `resetOnOffsetOutOfRange` from the closed-channel branch
- No disable switch. The fix is unconditional; an opt-in flag would leave the reported error firing
- Do NOT treat offset-out-of-range or corruption errors as this class — they keep their own recovery paths
- Do NOT swallow the error permanently: a closed client, or recovery that exhausts its consecutive attempts, must still end the run with an error
- Do NOT add a metric. Exactly one WARN log line per recovery is permitted
- Do NOT change `run.CancelOnFirstError` or the per-partition run wiring beyond the local counter and the `recoverFromConsumeError` signature
- Do NOT touch the Kafka producer or the non-offset consumer
- Frozen symbols for this change: `consumeMessages`, `recoverFromConsumeError`, `errPartitionConsumerClosed`
- Existing tests must pass, with one deliberate exception: the four closed-channel tests change their assertion from message text to the sentinel
</constraints>

<verification>
Run from the project root:

```
make precommit
```

Must exit 0 (format + lint + test + security checks). `make precommit` includes `make test`, so no separate full test run is required.

```
go test -run 'PartitionConsumerClosed|ConsumeMessages' -count=1 .
```

Must pass; this exercises the sentinel branches, the recovery, the client guard, the bound, and the counter reset.
</verification>
