---
status: prompted
approved: "2026-10-24T16:35:46Z"
generating: "2026-10-24T16:58:09Z"
prompted: "2026-10-24T16:58:09Z"
branch: dark-factory/pre-tx-message-filter
---

## Summary

- Add a batch-level filter decorator that removes messages **before** the wrapped handler runs, so a message the caller is certain to discard never reaches a transaction-opening handler.
- `NewMessageHandlerBatchTxUpdate` opens a write transaction around the whole handler chain before any handler has looked at a message. A consumer that discards a message for a reason it can determine from the message bytes alone still pays one database transaction and fsync per message.
- Composing the filter **outside** the transaction-opening handler removes that cost for discarded messages, without changing the transaction handler, its signature, or its semantics.
- Opt-in addition to `bborbe/kafka`; `bborbe/kafka` learns nothing about why a message would be discarded — the caller supplies the predicate.
- Scope is the `bborbe/kafka` library only. Wiring it into the task controller is a follow-up prompt in `bborbe/agent-task-controller` after this library is tagged.

## Problem

`NewMessageHandlerBatchTxUpdate` wraps a handler chain in `db.Update` before the chain runs, so the transaction is opened for every batch whether or not any handler will act on any message in it. The motivating consumer runs at batch size one, so that is one write transaction and fsync per message. A consumer whose handler discards a large fraction of messages — anything older than a TTL, for example — pays that cost for decisions that touch no stored state, and the cost is proportional to the backlog rather than to the work. At larger batch sizes the saving is one transaction per fully-dropped batch, and none attributable to the dropped messages inside a mixed one.

The library has no way to express "drop this message before the transaction opens": `NewMessageHandlerBatchTxUpdate` cannot know why a message would be discarded, and the handler that does know runs inside the transaction it would like to avoid.

## Goal

`bborbe/kafka` offers a `MessageHandlerBatch` decorator that removes messages before handing the remainder to the wrapped handler. Wrapping it outside a transaction-opening handler means discarded messages never open a transaction, while kept messages behave exactly as they do today.

A consumer that does not use the decorator behaves exactly as it does today.

## Non-goals

- Do NOT change `NewMessageHandlerBatchTxUpdate` to open the transaction lazily, or to inspect messages itself.
- Do NOT teach `bborbe/kafka` what an expired message is, or add any TTL, schema, or command concept. The filter takes a predicate.
- Do NOT change any existing handler, interface, or constructor signature.
- Do NOT add a metric. The filter is a cost optimisation, not an observable event; a caller that wants visibility can wrap its own predicate.
- Do NOT wire this into any consuming service. That is the follow-up prompt in the consuming repo.
- Do NOT add a scenario. The behaviour is reachable by unit tests with an injected database fake.
- Do NOT add a TTL-seek offset manager here. That is the sibling spec `ttl-seek-initial-offset.md`; the two are independent and neither depends on the other.

## Acceptance Criteria

- [ ] `NewMessageHandlerBatchFilter` exists in package `kafka` and returns a `MessageHandlerBatch` — evidence: the package compiles with an assignment to `MessageHandlerBatch`; `grep -n 'NewMessageHandlerBatchFilter' kafka_message-handler-batch-filter.go` returns at least one match; `go test ./...` exits 0.
- [ ] Only messages for which the predicate returns false reach the wrapped handler, in their original order — evidence: Ginkgo test with `mocks.KafkaMessageHandlerBatch` asserts `ConsumeMessagesArgsForCall(0)` holds exactly the non-matching messages, in input order; `go test` exits 0.
- [ ] The wrapped handler is called exactly once per batch, never once per message — evidence: Ginkgo test with `mocks.KafkaMessageHandlerBatch` asserts `ConsumeMessagesCallCount()` is 1 for a batch of five messages of which two are dropped.
- [ ] A message dropped by the filter never reaches the transaction-opening handler, so no transaction is opened for it — evidence: Ginkgo test wraps `NewMessageHandlerBatchTxUpdate` in the filter with `github.com/bborbe/kv/mocks`.DB (the dependency's Counterfeiter fake, not a hand-rolled recorder) and asserts `UpdateCallCount()` is 0 when both messages are dropped and 1 when one of two is dropped.
- [ ] An empty batch and an all-dropped batch both return nil without calling the wrapped handler — evidence: Ginkgo test asserts nil and a handler call count of 0 for each case.
- [ ] The caller's message slice is not mutated — evidence: Ginkgo test passes a three-element slice, captures a copy of it before the call, and asserts the slice equals the copy afterwards.
- [ ] The wrapped handler's error is returned unchanged — evidence: Ginkgo test with a handler returning a sentinel error asserts `errors.Is` on the returned error.
- [ ] Existing behaviour is untouched — evidence: `git diff --stat HEAD --` and `git status --porcelain --` for `kafka_message-handler-batch-tx.go`, `kafka_message-handler-batch.go`, and `kafka_message-handler.go` are both empty; `go test ./...` exits 0.
- [ ] `make precommit` exits 0.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — format, vet, lint, and the full test suite; expect exit code 0.
- `go test ./...` — expect exit code 0.
- `grep -n 'NewMessageHandlerBatchFilter' kafka_message-handler-batch-filter.go` — expect at least one match.

## Desired Behavior

1. `NewMessageHandlerBatchFilter(skip func(*sarama.ConsumerMessage) bool, handler MessageHandlerBatch) MessageHandlerBatch` is available. A message for which `skip` returns true is dropped; every other message is passed through.
2. The wrapped handler is called once with the surviving messages, in their original order. Its return value is returned unchanged.
3. When every message is dropped, or the batch is empty, the wrapped handler is not called and nil is returned — the consumer marks the offset and advances, exactly as it would have after the skip-errors wrapper inside the transaction swallowed the same decision.
4. The caller's slice is not mutated; the filter builds a new slice for the surviving messages.
5. The doc comment states that the filter must be composed **outside** a transaction-opening handler to have its effect, because only then is the transaction never opened for a dropped message, and that the predicate must be pure and side-effect free.

## Assumptions

- Callers can decide from the message bytes alone whether a message is discardable. For the motivating case (a command older than a TTL) that is true: the timestamp is in the payload.
- A dropped message needs no compensating write. For the motivating caller's composition — where `NewMessageHandlerTxSkipErrors` sits **inside** the transaction, so an expiry error is swallowed before `db.Update` returns — the existing path writes nothing either, so the filter preserves the observable outcome for both fully-dropped and mixed batches. This assumption does NOT extend to a caller that composes `NewMessageHandlerBatchSkipErrors` **outside** `NewMessageHandlerBatchTxUpdate`: there the transaction rolls back on the first error while the consumer still marks the offset past the whole batch, so a mixed batch silently loses its non-expired messages. The filter improves that case rather than preserving it.

## Constraints

- Default behaviour is frozen. No existing handler, interface, or constructor changes.
- The filter must preserve batch semantics: exactly one call to the wrapped handler with the surviving messages, original order, error returned unchanged. It must not call the handler once per message.
- The filter must not mutate the caller's slice.
- The returned handler must be stateless and safe for concurrent use: the consumer runs one goroutine per partition, all sharing one `MessageHandlerBatch`. All mutable state belongs to the caller's predicate.
- The README's handler-composition section lists the new decorator with the outside-the-transaction composition rule, so the contract sits where a library user looks first.
- Tests follow the repo's `docs/dod.md`: Ginkgo/Gomega in `package kafka_test`, with `mocks.KafkaMessageHandlerBatch` and `github.com/bborbe/kv/mocks`.DB for the transaction-count assertion. Do not add a second `RunSpecs` bootstrap — reuse `kafka_suite_test.go`.
- The new file follows the repo's flat naming: `kafka_message-handler-batch-filter.go`, BSD license header, GoDoc on every exported symbol.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---------|-------------------|----------|-----------|
| Predicate panics | The panic propagates out of `ConsumeMessages` and fails the consumer run | The consumer's retry wrapper reconnects; an operator confirms recovery when the consumer resumes and the panic no longer appears in the log | Panic in the consumer goroutine |
| Predicate drops every message in a batch | Wrapped handler not called; nil returned; the consumer marks the offset | None needed; an operator confirms the offset still advances in the consumer's completion log line | Offset advancing while no handler work is logged |
| Predicate is wrong (drops a message that should have been handled) | The message is silently lost — the filter cannot know | Operator narrows the predicate and republishes or replays the affected range | Absence of the expected side effect; no library-level signal |
| Wrapped handler returns an error | Error returned unchanged; the consumer's existing error path handles it | Unchanged from today | Existing consumer error log |
| Batch contains no messages | Wrapped handler not called; nil returned | None needed | — |
| Filter composed **inside** `NewMessageHandlerBatchTxUpdate` instead of outside it | Output is correct but the transaction is still opened for every batch, so the change delivers no saving | Operator reorders the composition so the filter is outermost and confirms `UpdateCallCount()` is 0 for a fully-dropped batch | Nothing at runtime — AC 4's `UpdateCallCount()` assertion is the only guard |
| Predicate carries state across calls | Drops become order-dependent and non-deterministic | Operator makes the predicate pure and restarts the consumer | Nothing at library level — the doc contract and code review are the only guards |

## Do-Nothing Option

Doing nothing leaves the transaction-before-decision cost paid by every consumer that discards messages from their bytes. It is safe — nothing corrupts — but a consumer draining a backlog of messages it will discard pays a database fsync per message for the privilege, so the cost of a restart is proportional to the backlog rather than to the work.
