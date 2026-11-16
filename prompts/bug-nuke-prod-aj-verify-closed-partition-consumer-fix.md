---
spec: ["bug-nuke-prod-aj"]
status: draft
created: "2026-11-16T16:45:00Z"
---

# Pin the NUKE-PROD-AJ closed-Messages-channel recovery with a regression test

<summary>
- The library already treats a closed partition-consumer channel as recoverable; this change proves that with a test that pins the exact reported symptom
- A connection drop that closes the messages channel mid-run no longer aborts the consumer; it recreates the partition consumer and keeps consuming
- The new test covers the messages-channel branch end to end, which the existing recovery test does not (it uses the errors channel)
- No production code changes: the fix shipped in v1.28.2 and the current code already contains it
- No new configuration, metric, switch, dependency, or exported API
- The remediation the spec describes for the nuke-prod service (raising its dependency floor) is a change in that service's repo, not in this library
</summary>

<objective>
Confirm the library already contains the closed-partition-consumer recovery that fixes `NUKE-PROD-AJ`, and add one regression test that reproduces the exact reported path — a closed `PartitionConsumer.Messages()` channel while the consumer is running — proving the consumer recreates the partition consumer and continues instead of aborting every partition.
</objective>

<context>
Read `specs/in-progress/bug-nuke-prod-aj.md` — it is the contract for this change. Read `specs/in-progress/003-bug-partition-consumer-closed-sentinel.md` too: it is the spec that originally fixed this issue and it explains the intended semantics.

Read these files before writing anything:

- `kafka_consumer-offset.go`:
  - `errPartitionConsumerClosed` — the package-level sentinel declared near the top of the file; it means "the partition consumer's channels closed", which is normal sarama teardown (rebalance, connection drop, client close).
  - `consumeMessages(ctx, consumePartition)` — its four closed-channel branches all return `errPartitionConsumerClosed`: the outer `Errors()` branch, the outer `Messages()` branch, the inner `Errors()` branch, and the inner `Messages()` branch. The Sentry message names the messages channel, so the outer/inner `Messages()` branches are the reported path.
  - `recoverFromConsumeError(ctx, client, consumer, consumePartition, partition, nextOffset, attempt, err)` — recognises the sentinel. It returns `handled=false` when `client.Closed()` or `attempt > maxConsecutivePartitionConsumerRecoveries`; otherwise it closes the dead consumer, calls `CreatePartitionConsumer(..., nextOffset)`, and returns `handled=true` with the recreated consumer at the unchanged `nextOffset`.
  - `Consume`'s per-partition run loop — increments `consecutiveClosedRecoveries` when the sentinel is returned and resets it to `0` after a successful batch.
- `kafka_consumer-offset_internal_test.go`:
  - `fakePartitionConsumer` (`messages` and `errors` channels, `closeCount`, `isClosed()`), `testSaramaConsumer` (`consumePartitionFn`), `testSaramaClient` (`partitions`), `fakeOffsetManager` (`nextOffset`, `fallbackOffset`, `markOffsetCalled`), `fakeMessageHandlerBatch` (`received`), `waitForCalls`.
  - `newClosedErrorsPartitionConsumer()` — the existing helper for a closed errors channel.
  - `newClosedRecoveryConsumer(...)` — builds an `offsetConsumer` wired to the fakes above.
  - `TestConsume_PartitionConsumerClosed_RecoversAndContinues` — the existing recovery test. It closes the **Errors()** channel on the first consumer. The new test must mirror its structure but close the **Messages()** channel, which is the branch named in the `NUKE-PROD-AJ` Sentry message and is not covered at the `Consume` level today.
  - The four `TestConsumeMessages_*ChannelClosed` tests cover the branch in isolation; they do not exercise recreation and continuation.

Coding conventions: `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` and `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`.

Note: `docs/dod.md`'s "Tests use Ginkgo/Gomega" line is stale for this package. `kafka_suite_test.go` holds the only permitted `RunSpecs`, so tests in `package kafka` use plain `func Test`. Do not convert the new test to Ginkgo.

<!-- OPEN QUESTIONS FOR THE HUMAN REVIEWER (not instructions to the executing agent):
1. The spec's "Recommended Fix" says to bump the nuke-prod consumer service's github.com/bborbe/kafka from v1.28.1 to >= v1.28.2 in that service's go.mod/go.sum. That change lives in the nuke-prod service repo, not in this library repo — a library cannot depend on itself, so this prompt cannot perform the bump. This repo already contains the fix (commit 6d79267) and released it in v1.28.2 (CHANGELOG.md), so the library-side work is verification plus regression coverage.
2. This spec duplicates specs/in-progress/003-bug-partition-consumer-closed-sentinel.md, whose prompt 011 (completed) implemented the same fix. Confirm whether bug-nuke-prod-aj should instead be closed as a duplicate rather than executed; if so, do not approve this prompt.
3. If your release process requires a CHANGELOG entry for a test-only change, add one at approval time. This prompt deliberately adds none because v1.28.2 already records the fix.
-->

<requirements>
1. Verify (do not reimplement) that the closed-partition-consumer fix is present in the current code. Confirm all of the following by reading the source; if any is missing, STOP and report it instead of adding a second recovery primitive:
   - `errPartitionConsumerClosed` is declared in `kafka_consumer-offset.go`.
   - All four closed-channel branches in `consumeMessages` return `errPartitionConsumerClosed`: outer `Errors()`, outer `Messages()`, inner `Errors()`, inner `Messages()`.
   - `recoverFromConsumeError` recognises the sentinel, skips recreation when `client.Closed()` is true, bounds consecutive recoveries with `maxConsecutivePartitionConsumerRecoveries`, and otherwise closes the dead consumer and recreates it at the unchanged `nextOffset`.
   - `Consume`'s per-partition run loop resets `consecutiveClosedRecoveries` to `0` after a successful batch.
   - `CHANGELOG.md` records the fix under `## v1.28.2`.

2. In `kafka_consumer-offset_internal_test.go`, add a helper next to `newClosedErrorsPartitionConsumer()`:

   ```go
   func newClosedMessagesPartitionConsumer() *fakePartitionConsumer {
   	msgCh := make(chan *sarama.ConsumerMessage)
   	close(msgCh)
   	return &fakePartitionConsumer{
   		messages: msgCh,
   		errors:   make(chan *sarama.ConsumerError, 10),
   	}
   }
   ```

   The `errors` channel must be a live, empty, buffered channel so the `consumeMessages` select does not block on it and does not pick it; the closed `messages` channel is what must be observed.

3. Add `TestConsume_PartitionConsumerClosed_MessagesChannelClosedRecoversAndContinues` in `kafka_consumer-offset_internal_test.go`, mirroring `TestConsume_PartitionConsumerClosed_RecoversAndContinues`. This is the regression pin for `NUKE-PROD-AJ`; the existing test covers the errors-channel branch, this one covers the messages-channel branch named in the Sentry message. Build it as follows:
   - Topic `test-topic`, partition `0`, `nextOffset := Offset(100)`.
   - First `ConsumePartition` call returns `newClosedMessagesPartitionConsumer()`; second returns a `goodPC` whose buffered `messages` channel holds one `*sarama.ConsumerMessage` at `Offset: nextOffset.Int64()` for the same topic and partition, with a buffered `errors` channel.
   - Track `ConsumePartition` call count and the offsets it receives, as the existing test does.
   - Build the consumer with `newClosedRecoveryConsumer(&testSaramaClient{partitions: []int32{partition.Int32()}}, fakeConsumer, offsetManager, handler, 1)`, using `fakeOffsetManager{nextOffset: nextOffset, fallbackOffset: OffsetOldest}` and a `fakeMessageHandlerBatch`.
   - Run `c.Consume(ctx)` in a goroutine, `waitForCalls(&offsetManager.markOffsetCalled, 1)`, `cancel()`, then wait for the goroutine to finish.
   - Assert: the handler received exactly one batch of exactly one message with `Offset == nextOffset.Int64()`; exactly two `ConsumePartition` calls; the second call used `nextOffset.Int64()`; the first (dead) partition consumer is closed (`isClosed()`); and `errors.Is(consumeErr, context.Canceled)`.
   - Assert explicitly that the consumer did not abort on the closed channel: `if errors.Is(consumeErr, errPartitionConsumerClosed) { t.Errorf(...) }`.
   - Do not add a sleep or a fixed timeout; use `waitForCalls` so the test cannot hang on a regression.

4. Do not change production code, `errPartitionConsumerClosed`, `consumeMessages`, `recoverFromConsumeError`, `maxConsecutivePartitionConsumerRecoveries`, or `Consume`. This prompt adds regression coverage only; the fix is already in place.

5. Do not modify the four existing `TestConsumeMessages_*ChannelClosed` tests, `TestConsume_PartitionConsumerClosed_RecoversAndContinues`, the bounded-recovery test, or the reset-counter test.

6. Add no CHANGELOG entry, configuration knob, metric, switch, or dependency. `CHANGELOG.md` v1.28.2 already records the fix, and this change is test-only.

7. Before you finish, re-run `<verification>`, confirm it passes, and walk each requirement above against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- The fix is internal to the offset consumer: no call-site change and no exported-API change. Do not add one.
- Do NOT reimplement the fix or add a second recovery primitive; reuse `recoverFromConsumeError` and `CreatePartitionConsumer`.
- Do NOT add a configuration knob, metrics counter, or disable switch.
- Do NOT touch the offset-out-of-range, corruption-skip, or non-offset consumer paths.
- Do NOT change topic replication factors or any deployment/manifest files — that follow-up lives in the service's deployment configuration, not this library.
- Do NOT add a new module dependency.
- Do NOT add a second Ginkgo `RunSpecs`; tests in `package kafka` use plain `testing.T`.
- Existing tests must still pass.
- No `//nolint` without an explanation.
</constraints>

<verification>
ROOTDIR=/workspace make precommit

go test -run 'TestConsume_PartitionConsumerClosed_MessagesChannelClosedRecoversAndContinues|TestConsume_PartitionConsumerClosed_RecoversAndContinues|TestConsumeMessages_' -count=1 .

Both must pass. `make precommit` must exit 0. The `go test -run` line makes the new test fail loudly if its name drifts — without it, a renamed test would match nothing and exit 0.
</verification>
