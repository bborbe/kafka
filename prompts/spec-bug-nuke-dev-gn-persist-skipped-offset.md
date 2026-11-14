---
spec: ["bug-nuke-dev-gn"]
status: draft
created: "2026-11-14T16:51:30Z"
---

<summary>
- A consumer that skips a corrupt record batch now records where it resumed, so a restart does not re-walk into the same corrupt range.
- The recorded position is written through the consumer's existing offset manager, so consumers backed by a persistent store keep the skip across restarts.
- Consumers backed by an in-memory offset manager are unaffected apart from a harmless in-memory update.
- The opt-in skip behavior itself is unchanged: corruption is still fatal unless the skip option is enabled.
- No new configuration knobs, options, metrics, or exported API.
- If the offset manager fails to record the position, the consumer fails loudly instead of silently continuing from an unrecorded position.
- New tests prove the position is recorded on a normal skip, on a skip that runs to the end of the partition, and that a store failure propagates.
</summary>

<objective>
Make the CRC-corruption skip path durable. When the offset consumer skips a corrupt batch, persist the first healthy offset through the configured `OffsetManager` before recreating the partition consumer, so a consumer backed by a durable offset store resumes past the corrupt range after a restart instead of re-reading from its last committed offset. This is the library-side half of the spec's recommendation to "give the task-view reader a durable committed position so it stops re-walking from offset 0"; the deployment flag and broker-side repair are out of scope for this repository.
</objective>

<context>
Read these before changing anything:
- `kafka_consumer-offset.go` — especially `skipAndAdvance` (the function that closes the old partition consumer, finds the next healthy offset, and recreates the consumer), `resetOnOffsetOutOfRange` (the sibling recovery path that already persists via `ResetOffset`), and the `MarkOffset` call in the main `Consume` loop.
- `kafka_offset-manager.go` — the `OffsetManager` interface. Note the documented contract: `MarkOffset` records "the offset of the next message to read, not the last message read", and `ResetOffset` is its backward-moving counterpart.
- `kafka_offset-manager-store.go` and `kafka_offset-manager-sarama.go` — the two durable `OffsetManager` implementations. `NewSimpleOffsetManager` in `kafka_offset-manager-simple.go` is in-memory only.
- `kafka_consumer-simple.go` — `NewSimpleConsumer` / `NewSimpleConsumerBatch` currently build an in-memory `NewSimpleOffsetManager(initialOffset, initialOffset)`; they do not persist.
- `kafka_consumer-offset_internal_test.go` — the existing in-package fakes: `fakeOffsetManager` (fields `markOffsetCalled int32`, `markOffsetTopic`, `markOffsetPart`, `markOffsetOffset`, `markOffsetErr`), `fakeSkipper`, `fakePartitionConsumer`, `testSaramaConsumer` (records `consumePartitionCalls`), `buildSkipTestConsumer`, `waitForCalls`, `assertMetricIncremented`. Tests in this package use plain `testing.T`, NOT Ginkgo.
- `docs/offset-managers.md` — explains the durable vs in-memory manager distinction and the `FallbackOffset` semantics.

Reference docs (available inside the execution container):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-library-guide.md`

Open questions for the human reviewer (resolve at audit time; do not implement in this prompt):
1. The spec's primary recommended fixes — set `SKIP_CRC_CORRUPTION=true` on the nuke-dev agent-task-executor deployment and repair/rewrite partition 0 broker-side — are not in this repository. This prompt implements only the library-side durable-commit half. Confirm the deployment and broker-repair halves are tracked elsewhere.
2. Persisting the skip only helps if the task-view reader is backed by a durable `OffsetManager` (`NewStoreOffsetManager` or `NewSaramaOffsetManager`). `NewSimpleConsumer` uses an in-memory `NewSimpleOffsetManager`, which still restarts at its initial offset. Confirm whether the `CreateTaskView` wiring will be switched to a durable offset manager (application-repo change). Without that, this library change alone will not stop the re-walk from offset 0.
</context>

<requirements>
1. In `skipAndAdvance` in `kafka_consumer-offset.go`, after the `newOffset < 0` block resolves `newOffset` to `Offset(highWaterMark)`, and BEFORE the `CreatePartitionConsumer(...)` call, persist the resume position through the offset manager:

```go
	if err := c.offsetManager.MarkOffset(ctx, c.topic, partition, newOffset); err != nil {
		return nil, Offset(0), errors.Wrapf(ctx, err, "mark skipped offset failed")
	}
```

   - `c.topic` is the consumer's topic; `partition` is the function parameter; `newOffset` is the first healthy offset (or the high-water mark when corruption runs to the end of the partition).
   - This is the correct direction for `MarkOffset`: its contract is "the offset of the next message to read", and `newOffset` is exactly that.
   - Persist BEFORE creating the new partition consumer. If the store write fails, return the error without creating a replacement consumer, so no partition consumer is leaked (the old one was already closed earlier in the function). Do not create the consumer first and then persist.
   - Do not increment `CorruptBatchSkippedCounterInc` or emit the "skipped corrupt batch" log line when the store write fails; the skip did not complete.

2. Mirror the existing error-wrapping style in this file: use `errors.Wrapf` from `github.com/bborbe/errors` (already imported) with the message `"mark skipped offset failed"`.

3. Leave the closed-channel recovery path (`recoverFromConsumeError`'s `errPartitionConsumerClosed` branch) unchanged. It recreates the consumer at the same `nextOffset` without advancing, so there is nothing to persist.

4. Leave the default skip behavior unchanged. `skipAndAdvance` is only reachable when `SkipCorruptBatches` is enabled; do not change the option default, do not change the fatal-on-corruption path, and do not add any new option, field, metric, or exported function.

5. Add in-package unit tests in `kafka_consumer-offset_internal_test.go` (plain `testing.T`), driving the real `skipAndAdvance` with the existing fakes. Construct the consumer directly (mirroring `buildSkipTestConsumer` / `TestSkipAndAdvance_NewOffsetNegative_UsesHighWaterMark`), supplying every field `skipAndAdvance` reads:

```go
	om := &fakeOffsetManager{fallbackOffset: OffsetOldest}
	oldPC := &fakePartitionConsumer{messages: make(chan *sarama.ConsumerMessage, 10), highWaterMarkOffset: 1000}
	newPC := &fakePartitionConsumer{messages: make(chan *sarama.ConsumerMessage, 10)}
	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(string, int32, int64) (sarama.PartitionConsumer, error) { return newPC, nil },
	}
	c := &offsetConsumer{
		topic:          topic,
		offsetManager:  om,
		metrics:        &fakeMetricsConsumer{},
		consumerOptions: ConsumerOptions{SkipCorruptBatches: true},
		skipper:        &fakeSkipper{healthyOffset: healthyOffset},
	}
```

   a. `TestSkipAndAdvance_PersistsHealthyOffsetThroughOffsetManager`: `fakeSkipper{healthyOffset: Offset(200)}`, `fakeOffsetManager{fallbackOffset: OffsetOldest}`, topic `test-topic`, partition `0`. Call `skipAndAdvance` directly. Assert: no error; returned offset is `200`; `atomic.LoadInt32(&om.markOffsetCalled) == 1`; `om.markOffsetTopic == topic`; `om.markOffsetPart == partition`; `om.markOffsetOffset == Offset(200)`.
   b. `TestSkipAndAdvance_PersistsHighWaterMarkWhenCorruptionExtendsToEnd`: `fakeSkipper{healthyOffset: Offset(-1)}`, old consumer `highWaterMarkOffset: 500`. Assert: no error; returned offset is `500`; `om.markOffsetOffset == Offset(500)`; `atomic.LoadInt32(&om.markOffsetCalled) == 1`.
   c. `TestSkipAndAdvance_MarkOffsetError_PropagatesWithoutRecreating`: set `om.markOffsetErr = stderrors.New("offset store unavailable")` (use the stdlib `errors` package aliased as `stderrors`, already imported in the test file). Assert: `skipAndAdvance` returns a non-nil error; the returned consumer is nil; `len(fakeConsumer.consumePartitionCalls) == 0` (no replacement was created after the failed store write); the old partition consumer was closed.
   - Keep the tests synchronous (call `skipAndAdvance` directly, no goroutine) so the fake offset manager's fields can be read without a race.
   - Do not weaken or delete existing tests. The existing `waitForCalls(&offsetManager.markOffsetCalled, 1)` assertions are `>=` waits and remain valid after this change.
</requirements>

<constraints>
- Scope is the `bborbe/kafka` library only. Do NOT modify deployment manifests, environment variables, or the `SKIP_CRC_CORRUPTION` flag — they are not in this repository.
- Do NOT attempt broker-side partition repair or any operational action.
- Do NOT change the default behavior: corruption stays fatal unless `SkipCorruptBatches` is enabled.
- Do NOT change any exported API (`ConsumerOptions`, the functional options, the consumer constructors, or the `OffsetManager` interface).
- Do NOT add new tunability, configuration fields, metrics, or options.
- Use the existing `github.com/bborbe/errors` wrapping idiom (`errors.Wrapf(ctx, err, "...")`) already used throughout `kafka_consumer-offset.go`.
- New tests use the existing in-package fake pattern with plain `testing.T`, not Ginkgo (this package already runs one Ginkgo suite via `kafka_suite_test.go`).
- Existing tests must still pass.
- Do NOT commit — dark-factory handles git.
</constraints>

<verification>
Run `make precommit` — it must pass (format, lint, tests, security checks).

Also confirm by inspection:
- `grep -n "mark skipped offset failed" kafka_consumer-offset.go` returns exactly one line, inside `skipAndAdvance`, positioned before the `CreatePartitionConsumer` call.
- `grep -n "MarkOffset" kafka_consumer-offset.go` shows the new call in `skipAndAdvance` in addition to the existing call in the `Consume` loop.
- The three new tests are present and pass under `go test -run 'TestSkipAndAdvance_' ./...`.
- No new exported identifiers: `go doc` output for the package shows no new exported symbols.
</verification>
