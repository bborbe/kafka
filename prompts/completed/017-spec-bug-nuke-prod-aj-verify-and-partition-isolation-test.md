---
status: completed
spec: [bug-nuke-prod-aj]
summary: Confirmed the v1.28.2 closed-partition-consumer fix and sentinel/recovery wiring are present, added the two-partition isolation regression test TestConsume_TwoPartitions_OneClosedDoesNotAbortTheOther, and documented it under Unreleased; make precommit passed with no production-code, go.mod, or go.sum changes.
execution_id: bf75ef1f-b758-5aec-9298-bb108cef3700-exec-017-spec-bug-nuke-prod-aj-verify-and-partition-isolation-test
dark-factory-version: dev
created: "2026-11-14T18:05:00Z"
queued: "2026-11-14T17:11:50Z"
started: "2026-11-14T17:11:52Z"
completed: "2026-11-14T17:18:35Z"
---

# Verify NUKE-PROD-AJ fix and lock partition isolation

<summary>
- The library already contains the fix for the reported closed-partition-consumer crash; this prompt confirms that state rather than changing production behavior.
- No dependency bump is possible in this repository: it is the Kafka library itself, not the consumer service named in the report.
- A new regression test proves that when one partition's consumer channel closes normally, the other partitions keep running and the whole consumer is not aborted.
- The new test drives the real multi-partition consumer loop, not just a single partition in isolation.
- Existing recovery behavior, the sentinel error, and the bounded-retry logic are left untouched.
- No new configuration, options, metrics, or exported API.
- The only remaining action — upgrading the consumer service to a fixed library version — belongs in the service's own repository and is called out for the reviewer.
</summary>

<objective>
Confirm that the NUKE-PROD-AJ closed-partition-consumer regression is already fixed in this library and lock the exact reported symptom — one partition's normal teardown aborting the whole consumer — with a two-partition regression test. Make no production-code or dependency change, because the fix shipped in v1.28.2 and this module has no dependency on itself.
</objective>

<context>
This repository is the library `github.com/bborbe/kafka`, NOT the "nuke-prod consumer service" named in the spec. There is exactly one `go.mod` in the repo and it declares `module github.com/bborbe/kafka`; the repo does not depend on `github.com/bborbe/kafka`. Do not look for, or invent, a consumer-service `go.mod` here.

Read these before changing anything:
- `kafka_consumer-offset.go` — the `Consume` partition loop, `consumeMessages`, `recoverFromConsumeError`, the sentinel `errPartitionConsumerClosed`, and the constant `maxConsecutivePartitionConsumerRecoveries`.
- `kafka_consumer-offset_internal_test.go` — the in-package fakes and helpers the new test must reuse: `fakePartitionConsumer`, `testSaramaConsumer`, `testSaramaClient`, `fakeOffsetManager`, `fakeMessageHandlerBatch`, `newClosedErrorsPartitionConsumer`, `newClosedRecoveryConsumer`, `waitForCalls`. Tests in this file use plain `testing.T`, not Ginkgo.
- `CHANGELOG.md` — confirm the `## v1.28.2` entry documenting the closed-partition-consumer fix.
- `docs/dod.md` — project definition of done.

Reference docs (available inside the execution container):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`

Open questions for the human reviewer (resolve at audit time; do NOT implement in this prompt):
1. The spec's recommended fix is a dependency bump of `github.com/bborbe/kafka` from v1.28.1 to >= v1.28.2 in the **nuke-prod consumer service's** `go.mod`/`go.sum`. That service is a different repository. This repo is the library at v1.29.14 (descendant of the v1.28.2 fix commit `6d79267`), so there is no in-repo dependency to bump. Confirm the service-side bump is tracked in the service's own repo.
2. Confirm the nuke-prod deployment actually runs a library version >= v1.28.2 after that bump. This library cannot enforce the version a consumer selects.
</context>

<requirements>
1. **Confirm the fix is present (read-only — do not modify production code).** Verify by reading `kafka_consumer-offset.go`:
   - `consumeMessages` returns the sentinel `errPartitionConsumerClosed` (not a freshly formatted error) on all four closed-channel branches: the outer `Errors()` branch, the outer `Messages()` branch, the inner `Errors()` branch, and the inner `Messages()` branch. Each branch is the `if !ok { return nil, errPartitionConsumerClosed }` form.
   - `recoverFromConsumeError` handles `errors.Is(err, errPartitionConsumerClosed)` by closing the dead partition consumer and recreating it at `nextOffset`; it returns unhandled when `client.Closed()` is true or when `attempt > maxConsecutivePartitionConsumerRecoveries`.
   - The `Consume` partition loop increments `consecutiveClosedRecoveries` only when `errors.Is(err, errPartitionConsumerClosed)`, swaps in the recreated consumer when `handled` is true, and otherwise returns `errors.Wrapf(ctx, err, "consume failed")`.
   If and only if one of these is missing, restore it by mirroring the existing branches in the same file; otherwise make no production-code change.

2. **Confirm the fix is released (read-only).** Verify `CHANGELOG.md` contains the `## v1.28.2` entry with text `fix: Treat a closed partition consumer channel as recoverable and recreate the consumer at the last good offset instead of aborting every partition`. Do not add a duplicate changelog entry under `Unreleased`; the fix is already documented.

3. **Add a two-partition isolation regression test.** Add `TestConsume_TwoPartitions_OneClosedDoesNotAbortTheOther` to `kafka_consumer-offset_internal_test.go` (plain `testing.T`). It must prove the exact NUKE-PROD-AJ symptom: a normal closed channel on one partition must not abort the sibling partition or the whole consumer.
   - Use `newClosedRecoveryConsumer` with a `testSaramaClient{partitions: []int32{0, 1}}`, a `fakeOffsetManager{nextOffset: Offset(100), fallbackOffset: OffsetOldest}`, and a `fakeMessageHandlerBatch{}` (no messages are delivered, so the handler is never called concurrently).
   - Dispatch `ConsumePartition` per partition. Do NOT pass `testSaramaConsumer` directly: its `consumePartitionCalls` slice is appended without synchronisation and would race across the two partition goroutines. Add a small file-scope helper type that embeds `*testSaramaConsumer` and overrides `ConsumePartition`, for example:

```go
type partitionDispatchConsumer struct {
	*testSaramaConsumer
	fn func(partition int32, offset int64) (sarama.PartitionConsumer, error)
}

func (c *partitionDispatchConsumer) ConsumePartition(
	_ string,
	partition int32,
	offset int64,
) (sarama.PartitionConsumer, error) {
	return c.fn(partition, offset)
}
```

   - For each partition, the first `ConsumePartition` call returns `newClosedErrorsPartitionConsumer()`; every later call returns a blocking consumer `&fakePartitionConsumer{messages: make(chan *sarama.ConsumerMessage), errors: make(chan *sarama.ConsumerError)}`. Track per-partition call counts with `int32` and `sync/atomic` (two counters, one per partition).
   - Run `c.Consume(ctx)` in a goroutine with a cancellable context. Wait with `waitForCalls(&count0, 2)` and `waitForCalls(&count1, 2)`, then `cancel()` and wait for `Consume` to return.
   - Assert: `consumeErr` is non-nil and `errors.Is(consumeErr, context.Canceled)` is true; `errors.Is(consumeErr, errPartitionConsumerClosed)` is false; `atomic.LoadInt32(&count0) == 2` and `atomic.LoadInt32(&count1) == 2` (each partition recovered exactly once and kept running until cancellation).
   - Add a short comment above the test explaining that it locks the NUKE-PROD-AJ failure mode: one partition's normal teardown must not escalate to a whole-consumer abort.
   - Keep the test function under 80 lines and its statements under 50 so `funlen` passes; extract the dispatch closure into a local variable if needed.

4. **Do not change behavior or API.** No production-code edit, no `go.mod`/`go.sum` change, no new exported identifier, no new option, field, metric, or configuration knob.

5. **Self-check before finishing.** Re-run the `<verification>` commands and confirm each passes; walk requirements 1–4 against the resulting diff (which should contain only the new test plus, at most, a restored closed-channel branch if one was genuinely missing).
</requirements>

<constraints>
- Scope is the `bborbe/kafka` library only. Do NOT modify deployment manifests, environment variables, or any consumer-service repository.
- Do NOT edit `go.mod` or `go.sum` for this spec. This module has no dependency on `github.com/bborbe/kafka`; there is no version to bump here.
- Do NOT change the default behavior, the sentinel error, the bounded-recovery constant, or the `Consume` loop's recovery wiring.
- Do NOT change any exported API (`ConsumerOptions`, functional options, consumer constructors, or the `OffsetManager` interface).
- Do NOT add new tunability, configuration fields, metrics, or options.
- Do NOT add a duplicate `Unreleased` changelog entry for a fix already documented under v1.28.2.
- New tests use the existing in-package fake pattern with plain `testing.T`, not Ginkgo (this package already runs one Ginkgo suite via `kafka_suite_test.go`).
- Use the existing `github.com/bborbe/errors` helpers already imported in the test file (`errors.Is`); do not add new dependencies.
- Existing tests must still pass.
- Do NOT commit — dark-factory handles git.
</constraints>

<verification>
Run `make precommit` — it must pass (format, lint, tests, security checks).

Run `go test -mod=mod -run 'TestConsume_TwoPartitions_OneClosedDoesNotAbortTheOther' -count=1 .` — must pass.

Also confirm by inspection:
- `grep -c "return nil, errPartitionConsumerClosed" kafka_consumer-offset.go` prints `4` (the four closed-channel branches).
- `grep -q "TestConsume_TwoPartitions_OneClosedDoesNotAbortTheOther" kafka_consumer-offset_internal_test.go` succeeds.
- `grep -q "Treat a closed partition consumer channel as recoverable" CHANGELOG.md` succeeds.
- The working diff contains no change to `go.mod`, `go.sum`, or any production `.go` file beyond, at most, a restored closed-channel branch.
</verification>
