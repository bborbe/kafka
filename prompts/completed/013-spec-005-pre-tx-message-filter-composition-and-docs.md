---
status: completed
spec: [005-pre-tx-message-filter]
summary: Added the transaction-count composition tests for NewMessageHandlerBatchFilter and documented the decorator, its outside-the-transaction rule, and the feature changelog entry.
execution_id: kafka-offset-ttl-exec-013-spec-005-pre-tx-message-filter-composition-and-docs
dark-factory-version: v0.198.7
created: "2026-10-24T16:52:37Z"
queued: "2026-10-24T16:58:13Z"
started: "2026-10-24T17:03:13Z"
completed: "2026-10-24T17:04:32Z"
---

# Prove and document the outside-the-transaction composition

<summary>
- A test proves the point of the new decorator: a batch whose messages are all discarded opens no database transaction at all
- A second case proves a mixed batch opens exactly one transaction and hands only the surviving message to the transaction handler
- The test uses the dependency's own generated database fake, so it counts the real `Update` calls rather than a hand-rolled recorder
- The README gains a handler-composition section listing the decorator and the rule that it must sit outside the transaction-opening handler
- The section is linked from the table of contents, so a library user finds the contract where they look first
- The section also records the two things a caller must know: the predicate must be pure, and a fully-discarded batch is a no-op
- The changelog records the new constructor as a feature
- Nothing else in the library changes
</summary>

<objective>
Add the transaction-count test that proves `NewMessageHandlerBatchFilter` prevents the transaction for a fully-discarded batch, and document the decorator plus its composition rule in the README's handler-composition section and the CHANGELOG.
</objective>

<context>
Read `specs/in-progress/005-pre-tx-message-filter.md` — Acceptance Criterion 4 ("a message dropped by the filter never reaches the transaction-opening handler, so no transaction is opened for it") is what this prompt proves, and the Constraints section requires the README to carry the composition rule.

`prompts/1-spec-005-pre-tx-message-filter.md` has already created `kafka_message-handler-batch-filter.go` and `kafka_message-handler-batch-filter_test.go`. Read both before editing; extend the existing test file, do not create a second one.

Read these files before writing anything:

- `kafka_message-handler-batch-tx.go` — `NewMessageHandlerBatchTxUpdate(db libkv.DB, messageHandler MessageHandlerBatchTx) MessageHandlerBatch` returns a handler whose `ConsumeMessages` calls `db.Update(ctx, func(ctx, tx) error { return messageHandler.ConsumeMessages(ctx, tx, messages) })`. Do not modify this file.
- `mocks/kafka-message-handler-batch-tx.go` — `mocks.KafkaMessageHandlerBatchTx` provides `ConsumeMessagesCallCount() int` and `ConsumeMessagesArgsForCall(i int) (context.Context, libkv.Tx, []*sarama.ConsumerMessage)`.
- The dependency's database fake, at `$(go env GOMODCACHE)/github.com/bborbe/kv@v1.21.19/mocks/db.go`, import path `kvmocks "github.com/bborbe/kv/mocks"`. Verified facts you must build the test on:
  - `func (fake *DB) Update(arg1 context.Context, arg2 func(ctx context.Context, tx kv.Tx) error) error` records the call **before** consulting the stub, so `UpdateCallCount()` is correct whether or not the callback runs.
  - `func (fake *DB) UpdateCallCount() int` returns the number of `Update` calls.
  - `func (fake *DB) UpdateCalls(stub func(context.Context, func(ctx context.Context, tx kv.Tx) error) error)` installs the stub.
  - With no stub installed, `Update` returns its zero-value error and **does not invoke the callback**. So a test that wants the transaction body to actually run must install `UpdateCalls` and invoke the callback with a `&kvmocks.Tx{}`.
- `kafka_message-handler-batch-tx-metrics_test.go` — the existing pattern for pairing `kvmocks.Tx` with a generated mock in `package kafka_test`.
- `README.md` — `## Advanced Usage` currently holds `### Batch Processing`, `### Metrics Integration`, `### Transaction Support`, then `### Performance Tuning`. The Table of Contents near the top mirrors those four as `  - [Batch Processing](#batch-processing)` etc. There is no handler-composition section yet; this prompt creates it.
- `CHANGELOG.md` — the newest entry is `## v1.28.6`; there is no `## Unreleased` heading yet.
- `docs/dod.md` — Ginkgo/Gomega in `package kafka_test`, Counterfeiter mocks, no new `RunSpecs`.
</context>

<requirements>
1. In `kafka_message-handler-batch-filter_test.go`, add a second `Describe` block named `"MessageHandlerBatchFilter composed outside a transaction"`, alongside the existing block in the same file. Its `BeforeEach` builds a `db := &kvmocks.DB{}`, a `batchTx := &mocks.KafkaMessageHandlerBatchTx{}`, and the handler under test as `libkafka.NewMessageHandlerBatchFilter(skip, libkafka.NewMessageHandlerBatchTxUpdate(db, batchTx))`. The file already imports the package under test as `libkafka "github.com/bborbe/kafka"` (see `prompts/1-spec-005-pre-tx-message-filter.md`); keep that alias and do not add a second import of the same package. Add `libkv "github.com/bborbe/kv"` (for the stub's `libkv.Tx` parameter) and `kvmocks "github.com/bborbe/kv/mocks"`. Declare `ctx context.Context`, `messages []*sarama.ConsumerMessage` (two values with distinct `Offset` 0 and 1), and `skip func(*sarama.ConsumerMessage) bool` in the block's `var` list; `BeforeEach` sets `ctx`, builds `messages`, and constructs the handler, and each `It` assigns its own `skip` before calling — the closure captures the variable, so assigning per case is correct.

2. Case: a batch of two messages where the predicate drops both. Assert the returned error is nil, `db.UpdateCallCount()` is 0, and `batchTx.ConsumeMessagesCallCount()` is 0. This is the assertion that carries Acceptance Criterion 4 — a comment above it must say so, because nothing at runtime signals the miscomposition.

3. Case: a batch of two messages where the predicate drops exactly one. Install the stub that runs the transaction body, then call the filter, then assert:
   - `db.UpdateCallCount()` is 1 — one transaction for the batch, not one per message;
   - `batchTx.ConsumeMessagesCallCount()` is 1;
   - `_, _, argMessages := batchTx.ConsumeMessagesArgsForCall(0)` holds exactly the one surviving message (assert with `Equal` against a one-element slice holding that same pointer, and `HaveLen(1)`).

   The stub is:

```go
db.UpdateCalls(func(ctx context.Context, fn func(ctx context.Context, tx libkv.Tx) error) error {
	return fn(ctx, &kvmocks.Tx{})
})
```

   Install it before the call. Without it `kvmocks.DB.Update` returns its zero-value error without running the callback, so `batchTx` would never be reached and the call-count assertion would be vacuous. Put a one-line comment stating that.

4. Case: the composition still returns the wrapped handler's error. With the stub installed and `batchTx.ConsumeMessagesReturns(sentinelErr)` (stdlib `errors.New` sentinel), a mixed batch returns an error for which `errors.Is(err, sentinelErr)` holds, and `db.UpdateCallCount()` is 1. Do not assert on the error's text.

5. In `README.md`, add a `### Handler Composition` subsection under `## Advanced Usage`, placed after `### Transaction Support` and before `### Performance Tuning`. Add the matching table-of-contents line `  - [Handler Composition](#handler-composition)` between `  - [Transaction Support](#transaction-support)` and `  - [Performance Tuning](#performance-tuning)`.

   The section must contain, in this order:
   - one paragraph naming `NewMessageHandlerBatchFilter` as a `MessageHandlerBatch` decorator that removes messages before the wrapped handler runs, and stating the rule in bold terms: it must be composed **outside** the transaction-opening handler (`NewMessageHandlerBatchTxUpdate`), because composed inside it the messages are still dropped but the transaction is opened for every batch, so the saving is lost;
   - a code block showing the correct order:

```go
// Compose the filter OUTSIDE the transaction-opening handler.
// A message dropped here never opens a database transaction.
handler := kafka.NewMessageHandlerBatchFilter(
	func(msg *sarama.ConsumerMessage) bool {
		return shouldSkip(msg) // caller-supplied, pure, side-effect free
	},
	kafka.NewMessageHandlerBatchTxUpdate(db, batchTxHandler),
)
```

   - a short bullet list of the contract: the wrapped handler is called at most once per batch with the surviving messages in their original order; a batch where every message is dropped returns nil without calling the wrapped handler, so the consumer still marks the offset; the caller's slice is not modified; the returned handler is stateless and safe for concurrent use because the consumer runs one goroutine per partition, so all mutable state belongs to the predicate; the wrapped handler's error is returned unchanged.

   Keep the README free of any TTL, expiry, or schema concept — `shouldSkip` is a caller-supplied predicate, and the library does not know what makes a message discardable.

6. In `CHANGELOG.md`, add a new `## Unreleased` heading at the top, above `## v1.28.6`, containing exactly one bullet:

```
- feat: Add NewMessageHandlerBatchFilter, a MessageHandlerBatch decorator that removes messages before the wrapped handler runs so a discarded message never opens a database transaction
```

7. Before finishing, re-run `<verification>`, confirm it passes, and walk each requirement against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Do NOT modify `kafka_message-handler-batch-tx.go`, `kafka_message-handler-batch.go`, or `kafka_message-handler.go` — they must come out of this prompt byte-identical (do not rely on `git` here: the container's `.git` is a worktree pointer file, so `git status` fails inside it)
- Do NOT modify the filter implementation from the previous prompt beyond what a failing test proves necessary; the constructor signature `NewMessageHandlerBatchFilter(skip func(*sarama.ConsumerMessage) bool, handler MessageHandlerBatch) MessageHandlerBatch` is fixed
- Do NOT hand-roll a database recorder — use `kvmocks.DB`, the dependency's Counterfeiter fake
- Do NOT add a metric, a log line, a counter, or a `//nolint` directive
- Do NOT add a scenario
- Do NOT document a TTL, an expiry rule, or any other domain concept in the README or the changelog; the filter takes a predicate
- Do NOT wire the filter into any consuming service — that is a follow-up prompt in the consuming repo
- Do NOT touch the sibling spec `004-ttl-seek-initial-offset` or its code
- Existing tests must still pass
</constraints>

<verification>
`ROOTDIR=/workspace make precommit` — must pass.
`go test -count=1 -run TestSuite .` — must pass.
`grep -n 'Handler Composition' README.md` — at least two matches (table of contents and section heading).
`grep -n 'composed outside a transaction' kafka_message-handler-batch-filter_test.go` — at least one match (proves the new block exists, not just that the suite is green).
`grep -n 'NewMessageHandlerBatchFilter' README.md CHANGELOG.md` — at least one match in each.
</verification>
