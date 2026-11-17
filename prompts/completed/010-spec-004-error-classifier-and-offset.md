---
status: completed
spec: [004-kafka-decode-error-attribution]
summary: Added ErrorClass/ClassifyError classifier, attached next-expected offset as error context data in Consume, and removed the duplicate handler log
execution_id: kafka-exec-010-spec-004-error-classifier-and-offset
dark-factory-version: v0.199.1
created: "2026-11-16T11:20:24Z"
queued: "2026-11-16T11:29:34Z"
started: "2026-11-17T01:04:51Z"
completed: "2026-11-17T01:07:52Z"
---

# Attribute consumer errors to the failing offset and classify them by type

<summary>
- A consumer error now carries the offset the partition consumer will read next, so an operator can start a recovery scan at the right place instead of binary-searching a log segment
- The offset reaches the error as structured context data, not as text, so the error messages other tooling greps for are byte-identical
- The offset is the partition consumer's creation offset when the error arrives before any message, and the last delivered offset plus one when a message was delivered
- Every consumer error is classified as corruption, broker-side, or other, from its Go type rather than from its message text
- A message that merely reads "not a leader" but is not a broker error type is classified as other
- The class values are a closed set with a single construction point, so no unbounded string can reach a metric label later
- The consumer error handler no longer logs the error it returns, so one failure is reported once instead of twice
- The error the handler returns still carries the offset, and crash-versus-skip behaviour is unchanged
</summary>

<objective>
Make a consumer decode failure attributable: the error the offset consumer returns carries the offset the partition consumer will read next as `github.com/bborbe/errors` context data, and a new classifier derives a closed-set error class from the error's Go type. Remove the duplicated log from the consumer error handler so the failure is reported once.
</objective>

<context>
Read `CLAUDE.md` for project conventions and `docs/dod.md` for the definition of done.

Read these files before changing anything:
- `kafka_consumer-offset.go` — `IsCorruptionError`, the `Consume` loop and its per-partition closure (where `nextOffset` lives), `consumeMessages` (note it has **two** select blocks: an outer `select` and an inner `for`-loop `select`), and the `errors.Wrapf` chain `parition consumer returns error` → `consume failed` → `run failed`. Note the file imports the standard library `errors` aliased as `stderrors` and `github.com/bborbe/errors` as `errors`.
- `kafka_consumer-error-handler.go` — `ConsumerErrorHandler`, `ConsumerErrorHandlerFunc`, `NewConsumerErrorHandler` (currently both increments a metric and logs with `glog.Warningf`).
- `kafka_sarama-client-provider-type.go` — the in-tree exemplar for a typed string enumeration with an `Available*` collection and `Contains`.
- `kafka_consumer-offset_internal_test.go` — the in-package fakes (`fakePartitionConsumer`, `fakeOffsetManager`, `fakeMessageHandlerBatch`, `testSaramaConsumer`, `testSaramaClient`, `testSaramaClientProvider`, `fakeLogSampler`, `fakeWaiter`, `newOffsetConsumerForTest`, `buildSkipTestConsumer`) and the plain `func Test` style.

Coding-plugin guides (in-container paths): `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-enum-type-pattern.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-context-cancellation-in-loops.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`.

Spec (read in full, it is authoritative): `specs/in-progress/004-kafka-decode-error-attribution.md`. This prompt covers its Desired Behavior 1, 2, 5 and 6, and Acceptance Criteria 1, 3, 4, 6 and 7. Acceptance Criterion 5's second half (every classifier output is a member of the exported enumeration) is covered here too; its first half (the registered label set) belongs to the sibling prompt.

`docs/dod.md` says "Tests use Ginkgo/Gomega conventions". That line is stale for the internal test package: `package kafka` and `package kafka_test` compile into one test binary and `kafka_suite_test.go` already holds the only permitted `RunSpecs`. New tests in `package kafka` use plain `testing.T`. Do not "correct" them to Ginkgo.

<requirements>

### 1. New typed enumeration and classifier in `kafka_consumer-error-class.go`

Create `kafka_consumer-error-class.go` in `package kafka`, mirroring `kafka_sarama-client-provider-type.go`'s structure and doc-comment style (every exported symbol needs a doc comment starting with its name — `revive` runs in `make precommit`):

```go
// ErrorClass classifies a consumer error for the error_class metric label.
type ErrorClass string

const (
	// ErrorClassCorruption is a corrupt record batch or an undecodable packet.
	ErrorClassCorruption ErrorClass = "corruption"
	// ErrorClassBrokerSide is an error the Kafka broker itself reported.
	ErrorClassBrokerSide ErrorClass = "broker_side"
	// ErrorClassOther is any consumer error that is neither of the above.
	ErrorClassOther ErrorClass = "other"
)

// ErrorClasses is a slice of ErrorClass values.
type ErrorClasses []ErrorClass

// AvailableErrorClasses lists every ErrorClass value the system emits.
var AvailableErrorClasses = ErrorClasses{
	ErrorClassCorruption,
	ErrorClassBrokerSide,
	ErrorClassOther,
}
```

Add `String() string` on `ErrorClass` (returns `string(e)`) and `Contains(class ErrorClass) bool` on `ErrorClasses`, both mirroring the exemplar.

Add the classifier:

```go
// ClassifyError derives the ErrorClass from the error's type. It never inspects the message text
// except where IsCorruptionError already does.
func ClassifyError(err error) ErrorClass
```

Semantics, in this order:
1. `IsCorruptionError(err)` returns true → `ErrorClassCorruption`. Reuse the existing predicate; do not reimplement or widen its substring matching.
2. Otherwise, if the chain contains a `sarama.KError` → `ErrorClassBrokerSide`. Use `errors.As` from `github.com/bborbe/errors` with `var kerr sarama.KError; errors.As(err, &kerr)`. `sarama.KError` is an `int16` newtype implementing `error`, so `*sarama.KError` is a valid `errors.As` target; `sarama.ErrNotLeaderForPartition` is a `KError` value.
3. Everything else, including `nil`, → `ErrorClassOther`.

`*sarama.ConsumerError` implements `Unwrap() error` returning its `Err` field, so both `errors.As` and `IsCorruptionError` see through a `ConsumerError` to the cause. Do not special-case `*sarama.ConsumerError` yourself.

Do NOT add `Validate`/`Parse`/`Ptr` for `ErrorClass`: nothing parses a class from configuration or input, and the closed set is guaranteed at the single construction point, `ClassifyError`.

### 2. Classifier tests — `kafka_consumer-error-class_internal_test.go` (`package kafka`, plain `testing.T`)

`TestClassifyError`, table-driven, must cover at least:
- `sarama.PacketDecodingError{Info: "CRC mismatch"}` → `corruption`
- `&sarama.ConsumerError{Topic: "t", Partition: 0, Err: sarama.PacketDecodingError{Info: "CRC mismatch"}}` → `corruption`
- `sarama.ErrNotLeaderForPartition` → `broker_side`
- `&sarama.ConsumerError{Topic: "t", Partition: 0, Err: sarama.ErrNotLeaderForPartition}` → `broker_side`
- `errors.Wrapf(context.Background(), sarama.ErrNotLeaderForPartition, "broker said no")` → `broker_side` (proves the chain is walked, not just the top value)
- `stderrors.New("not a leader")` → `other` (this is Acceptance Criterion 4 — the text says leader, the type is not a broker error)
- `&sarama.ConsumerError{Topic: "t", Partition: 0, Err: stderrors.New("not a leader")}` → `other`
- `stderrors.New("something else entirely")` → `other`
- `nil` → `other`

Then two assertions that make the closed set and the three-way split explicit:
- For every table entry, assert `AvailableErrorClasses.Contains(got)` is true (this is Acceptance Criterion 5's membership half).
- Assert the three canonical samples (`PacketDecodingError`, `ErrNotLeaderForPartition`, an unrelated error) produce three pairwise-distinct values, so no two classes share a value and `other` is actually reachable.

The new test file imports its own dependencies (Go imports are per-file): `testing`, `context`, `github.com/IBM/sarama`, the standard `errors` aliased as `stderrors`, and `github.com/bborbe/errors` — the same aliases `kafka_consumer-offset_internal_test.go` uses.

### 3. Offset context data — helper plus one attachment point in `Consume`

In `kafka_consumer-offset.go`:

a) Add the context key constant next to the other package constants:

```go
// offsetDataKey is the error-context-data key carrying the offset a consumer error occurred at.
const offsetDataKey = "offset"
```

b) Add the pure helper next to `consumeMessages`:

```go
// nextExpectedOffset returns the offset the partition consumer will read next: the last delivered
// message's offset plus one when a message was delivered, otherwise the offset the partition
// consumer was created at.
func nextExpectedOffset(
	creationOffset Offset,
	messages []*sarama.ConsumerMessage,
) Offset {
	if len(messages) > 0 {
		return Offset(messages[len(messages)-1].Offset + 1)
	}
	return creationOffset
}
```

c) `consumeMessages` must return the messages collected so far alongside its error, so the caller can compute the last delivered offset. In both select blocks, every genuine error return changes from `return nil, <err>` to `return result, <err>` — the `ctx.Done()` branch, both channel-closed branches, and the `errors.Wrapf(ctx, err, "parition consumer returns error")` branch. Leave the two sentinel returns (`return nil, errSkipCorruptBatch` and `return nil, errOffsetOutOfRange`) exactly as they are: they are reached only when `result` is empty, and they mean "skip this batch", not "this call failed". On the outer `select`, `result` is always empty, so the change is behaviour-neutral there; on the inner loop it is the load-bearing one. Existing tests that assert `result == nil` still pass because in those cases no message was ever delivered. Do not change the success paths or the wrap message.

d) In `Consume`'s per-partition closure, replace the error branch so the offset is attached before the error is wrapped:

```go
messages, err := c.consumeMessages(ctx, consumePartition)
if err != nil {
	offsetCtx := errors.AddToContext(
		ctx,
		offsetDataKey,
		nextExpectedOffset(nextOffset, messages).Int64(),
	)
	newPC, newOff, handled, recoverErr := c.recoverFromConsumeError(
		offsetCtx,
		consumerFromClient,
		consumePartition,
		Partition(partition),
		nextOffset,
		err,
	)
	if recoverErr != nil {
		return recoverErr
	}
	if handled {
		consumePartition = newPC
		nextOffset = newOff
		continue
	}
	return errors.Wrapf(offsetCtx, err, "consume failed")
}
```

Notes that are load-bearing:
- `errors.AddToContext(ctx, key string, value any)` returns a **context**, not an error. The value reaches the error only through the later `errors.Wrapf(offsetCtx, …)`, which calls `AddContextDataToError`. Do not try to attach data to the error directly.
- The stored value is an `int64` (`Offset.Int64()`), because the data map is untyped and `bborbe/sentry`'s `addErrorTags` renders it with `fmt.Sprintf("%v", v)`. Do not store the `Offset` newtype, a formatted string, or the `*sarama.ConsumerMessage`.
- Pass the **plain** `ctx` into `c.consumeMessages` and the `offsetCtx` only to the wrap and to `recoverFromConsumeError`. `errors.DataFromError` merges the chain from the outside in and an inner layer overwrites an outer one, so attaching the offset inside `consumeMessages` would either be impossible (the value is unknown there) or would shadow the correct outer value.
- `nextOffset` is the creation offset until the first successful batch advances it, which is exactly the "otherwise" case of the spec's definition.
- Do not add a `glog` line for the offset. The spec forbids adding a log line at a site that has none; the offset is operator-visible through Sentry's error tags.

### 4. Offset tests — append to `kafka_consumer-offset_internal_test.go` (`package kafka`, plain `testing.T`)

`TestNextExpectedOffset` — table over the pure helper: creation `100` with no messages → `100`; creation `100` with one message at `105` → `106`; creation `100` with messages at `105` and `106` → `107`.

`TestConsume_DecodeErrorCarriesCreationOffset` — **the incident case, and the one that fails a `0`-returning implementation.** Drive the **real `Consume(ctx)`**, not `consumeMessages`:
- Build an `&offsetConsumer{...}` directly (the internal package may do so), setting every field `Consume` dereferences before reaching the error handler: `saramaClientProvider` (`&testSaramaClientProvider{client: &testSaramaClient{partitions: []int32{0}}}`), `topic`, `offsetManager` (`&fakeOffsetManager{nextOffset: Offset(1000), fallbackOffset: OffsetOldest}`), `messageHandlerBatch` (`&fakeMessageHandlerBatch{}`), `metrics` (`&fakeMetricsConsumer{}`), `batchSize: BatchSize(1)`, `logSampler` (`&fakeLogSampler{}`), `waiter` (`&fakeWaiter{}`), `consumerOptions: ConsumerOptions{}` (both options OFF), `saramaConsumerFunc`, and **`errorHandler: NewConsumerErrorHandler(NewMetrics())`** — a decode error reaches `c.errorHandler.HandleError`, and `buildSkipTestConsumer` does not set `errorHandler`, so copying its field list blindly panics on a nil interface.
- `saramaConsumerFunc` returns a `&testSaramaConsumer{consumePartitionFn: …}` whose first call returns a `&fakePartitionConsumer{errors: <buffered chan, pre-filled with &sarama.ConsumerError{Topic: …, Partition: 0, Err: sarama.PacketDecodingError{Info: "CRC mismatch"}}>}` and whose `messages` channel is an empty buffered channel. With no message available the outer `select` picks the errors channel deterministically.
- Call `Consume(context.Background())` synchronously (it returns as soon as the single partition function returns an error) and assert `err != nil`, then `errors.DataFromError(err)["offset"]` equals `int64(1000)`.
- On the same returned error, assert `err.Error()` contains `parition consumer returns error`, `consume failed` and `run failed` (Acceptance Criterion 7 — the message substrings are frozen and context data must not enter the message).

`TestConsume_DecodeErrorCarriesLastDeliveredOffsetPlusOne` — the second case. Same consumer shape, but `offsetManager.nextOffset: Offset(100)` and the partition consumer's `messages` channel is pre-filled with one `&sarama.ConsumerMessage{Topic: …, Partition: 0, Offset: 105}`; the decode error is delivered **after** that message has been consumed. Pre-filling both channels is not deterministic: the outer `select` picks randomly between `Errors()` and `Messages()`, so roughly half the runs would report the creation offset and the test would flake. Instead, send the error from a goroutine that first waits for the messages channel to drain:

```go
msgCh := make(chan *sarama.ConsumerMessage, 1)
msgCh <- &sarama.ConsumerMessage{Topic: topic.String(), Partition: 0, Offset: 105}
errCh := make(chan *sarama.ConsumerError, 1)
pc := &fakePartitionConsumer{messages: msgCh, errors: errCh}
go func() {
	deadline := time.Now().Add(5 * time.Second)
	for len(msgCh) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	errCh <- &sarama.ConsumerError{
		Topic:     topic.String(),
		Partition: 0,
		Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
	}
}()
```

With `batchSize: BatchSize(1)` the batch is returned as soon as the one message is read, so the error can only arrive on the next `consumeMessages` call, whose outer `select` has no `default` and therefore blocks for it — the outcome is deterministic. Assert first that the message was handled before the error (`len(messageHandlerBatch.received) == 1` and `fakeOffsetManager.markOffsetOffset == Offset(106)`), then that `errors.DataFromError(consumeErr)["offset"]` equals `int64(106)`. A constant, a `0`, or `105` all fail.

Mid-batch arithmetic (a batch consumer with `batchSize > 1` where an error arrives after some messages were delivered but before the batch is returned) cannot be driven deterministically through the real `Consume` path, because the inner `select` has a `default` and races the error channel. It is covered by `TestNextExpectedOffset` on the pure helper, while the two `Consume` tests above cover the shipped wiring.

### 5. Remove the duplicated log from the consumer error handler

In `kafka_consumer-error-handler.go`, delete the `glog.Warningf("got consumer error(%T) => skip: %v", errors.Unwrap(err), err)` line and drop the now-unused `errors` (standard library) and `github.com/golang/glog` imports. Update the `NewConsumerErrorHandler` doc comment so it no longer claims the handler logs errors. `HandleError` must still return `err`.

The double report belongs to the consuming application, not this library: nothing in `bborbe/kafka` logs the error `Consume` returns, and the application reports it. Do not add a replacement log line anywhere.

Test evidence for Acceptance Criterion 6 is the grep in `<verification>`; also keep the existing `TestConsumerErrorHandler_HandleError_ReturnsNonNil` and `TestConsumerErrorHandler_HandleError_OffsetOutOfRange_ReturnsNonNil` green.

### 6. Scope boundary — do not touch the metric in this prompt

`MetricsConsumer.ErrorCounterInc` keeps its current two-parameter signature here. The `error_class` label and the third parameter are the sibling prompt's change; this prompt only introduces the classifier it will call. Do not add a metric, a label, a log line, or a config flag.

### 7. Final self-check

Before finishing: re-run `<verification>` and confirm it passes; then walk Desired Behavior 1, 2, 5, 6 and Acceptance Criteria 1, 3, 4, 6, 7 of `specs/in-progress/004-kafka-decode-error-attribution.md` against the change.

</requirements>

<constraints>
- The error must be built with `github.com/bborbe/errors` — `AddToContext` on the context, `Wrapf` to wrap. No `fmt.Errorf`, no bare `return err` at a new site.
- The class values are exactly `corruption`, `broker_side`, `other`. Adding one is a code change, not a config change.
- Classification comes from the error type, not its text. No new substring matching beyond the existing `IsCorruptionError`.
- Do not change crash-versus-skip behaviour: the consumer still returns on a decode failure, and `WithSkipCorruptBatches` stays off by default and is not enabled here.
- Do not migrate `consumerErrorCounter` from a gauge to a counter, do not rename it, and do not touch `consumerCorruptBatchSkippedCounter` — both are explicit spec Non-goals.
- Do not change the error message substrings `parition consumer returns error`, `consume failed` and `run failed`; context data never enters the message, so they must stay byte-identical.
- Do not add a log line at any site that has none today.
- Do not change `CreatePartitionConsumer` in `kafka_consumer-partition.go`, the `WithAutoResetOffsetOutOfRange` path, or the `WithSkipCorruptBatches` path.
- Tests in the internal package use plain `testing.T`, not Ginkgo (one `RunSpecs` per binary, already taken by `kafka_suite_test.go`).
- Do not add a new module dependency; `go.mod` and `go.sum` must be unchanged apart from anything `go mod tidy` does on its own.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `make precommit` must pass.
</constraints>

<verification>
Run `make precommit` — must pass (this runs ensure, format, generate, test, check and addlicense).

Then run the targeted checks; each must pass:

- `go test -mod=mod -run 'TestClassifyError|TestNextExpectedOffset|TestConsume_DecodeErrorCarriesCreationOffset|TestConsume_DecodeErrorCarriesLastDeliveredOffsetPlusOne' ./...` — the named evidence tests must exist and pass (without this line a renamed test makes the filter match nothing and exit 0).
- `! grep -q 'glog\.' kafka_consumer-error-handler.go` — Acceptance Criterion 6: the handler does not log.
- `grep -q 'AddToContext' kafka_consumer-offset.go` — the offset is attached in the shipped path.
- `grep -q 'offsetDataKey' kafka_consumer-offset.go` — the context key exists.
- `grep -q 'func ClassifyError' kafka_consumer-error-class.go` — the classifier is where the sibling prompt expects it.
- `grep -q 'AvailableErrorClasses' kafka_consumer-error-class.go` — the closed set is exported.

Absence assertions are written as `! grep -q` because `grep -c` exits 1 when the count is zero and would fail the step it was meant to pass. No `git`, `docker`, `kubectl`, `gh` or `dark-factory` command belongs in this block.
</verification>
