---
status: completed
summary: Added structural Transient() classification for broker-unreachable offset-consumer errors via a shared handler helper, with table-driven and inner-call-site tests
execution_id: kafka-exec-009-transient-consumer-error-marker
dark-factory-version: dev
created: "2026-10-18T12:30:00Z"
queued: "2026-10-18T12:44:01Z"
started: "2026-10-18T12:46:15Z"
completed: "2026-10-18T12:53:43Z"
---

# Mark transient broker-unreachable consumer errors

<summary>
- Errors the offset consumer returns for a transient broker-unreachable cause become identifiable by a structural marker method
- A downstream reporter can recognise that class without this library importing the reporter, or any shared marker package
- The consumer's behaviour is unchanged: same error value, same message, same teardown
- The classification is a narrow positive list — offset-out-of-range, corruption and unclassified errors stay unmarked
- No new module dependency is introduced
</summary>

<objective>
Make the offset consumer's transient broker-unreachable errors satisfy the structural interface `interface{ Transient() bool }` with `Transient()` returning true, so a reporter can suppress them at its own boundary without this library knowing the reporter exists. Runtime behaviour, error values and error text stay byte-identical.
</objective>

<context>
Read CLAUDE.md for project conventions.
Read `docs/dod.md` — note its "Tests use Ginkgo/Gomega conventions" line; requirement 8 deliberately overrides it for the internal test package, because `package kafka` and `package kafka_test` compile into one test binary and `kafka_suite_test.go` already holds the single permitted `RunSpecs`. Do not "correct" the new tests to Ginkgo.
Read `kafka_consumer-offset.go` — `Consume`, the per-partition run function, `consumeMessages` (note it has **two** `c.errorHandler.HandleError(err)` call sites: one in the outer `select`, one in the inner `for` loop), the `run.CancelOnFirstError` collapse, and the `errors.Wrapf` chain (`parition consumer returns error` → `consume failed` → `run failed`). Note this file imports the standard `errors` package aliased as `stderrors`.
Read `kafka_consumer-error-handler.go` — the `ConsumerErrorHandler` interface and `NewConsumerErrorHandler`.
Read `kafka_consumer-offset_internal_test.go` — the in-package fakes (`fakePartitionConsumer`, `newOffsetConsumerForTest`, `buildSkipTestConsumer`) and the existing plain `func Test` style.
</context>

<requirements>
1. Add a transient classification to the kafka package. An error is **transient** if and only if the cause at the end of its wrapped chain is one of:
   - `io.EOF`
   - `io.ErrUnexpectedEOF`
   - a connection-refused dial error (`errors.Is(err, syscall.ECONNREFUSED)`, which traverses `*net.OpError` / `*os.SyscallError`)
   - a connection-reset error (`errors.Is(err, syscall.ECONNRESET)`)
   - `sarama.ErrBrokerNotAvailable` (`github.com/IBM/sarama`)
   Walk the chain with `errors.Is` / `errors.As`. This file aliases the standard package as `stderrors`; `github.com/bborbe/errors` also exposes `Is`/`As` and delegates to it — either is fine, but match what the surrounding code already uses. Place the classification next to the existing `IsCorruptionError` and `IsOffsetOutOfRange` helpers in `kafka_consumer-offset.go`.

2. Make the **returned error's chain** contain a value satisfying `interface{ Transient() bool }` whose `Transient()` returns `true` when requirement 1 classifies it transient. (The outermost `errors.Wrapf(…, "run failed")` value does not itself need to implement the marker — the chain must contain one that does.) The marker must be reachable via `errors.As` from the **returned** error, through the full `run failed: consume failed: parition consumer returns error: …` chain, not only from the innermost cause.
   **`consumeMessages` has two `c.errorHandler.HandleError(err)` call sites — the outer `select` and the inner `for` loop. Apply the marker through ONE shared helper that both call sites invoke**, so a one-sided edit is impossible by construction rather than merely discouraged. Do not duplicate the marking logic at each site.
   The marker wrapper must implement `Unwrap() error` returning the original `*sarama.ConsumerError`, so the original cause stays reachable through the chain — a wrapper carrying only `Error()` and `Transient()` would satisfy the marker while cutting the cause off.

3. `OffsetOutOfRange` errors must NOT be marked. They are self-healed by the existing opt-in reset path (spec 002) and are a distinct class.

4. Errors for which the existing `IsCorruptionError(err)` returns true must NOT be marked. They are owned by the CRC-recovery workstream and must stay loud.

5. Any error not classified transient by requirement 1 must NOT be marked — either it does not satisfy the marker interface, or `Transient()` returns false. Both are acceptable; be consistent.

6. The returned error's message text and underlying cause must not change: `parition consumer returns error`, `consume failed` and `run failed` must all still appear in the chain, in that order, and `errors.Is` / `errors.As` must still reach the original `*sarama.ConsumerError`. Adding the marker wrapper to the chain (requirement 2) is the one permitted structural addition.

7. Do NOT modify `NewConsumerErrorHandler` to return `nil`. That is a frozen anti-pattern: swallowing the error leaves the partition consumer stalled at an invalid offset. The marker labels; it does not suppress.

8. Add tests in `kafka_consumer-offset_internal_test.go` using plain `func Test`. This deliberately overrides the `docs/dod.md` line "Tests use Ginkgo/Gomega conventions", which is stale for the internal test package — one `RunSpecs` per binary is the binding constraint, and it is already taken by `kafka_suite_test.go`.
   - Name the primary transient test **`TestConsume_TransientBrokerErrorSatisfiesMarker`** (spec 003 AC 1's evidence).
   - **Drive the real `Consume(ctx)`**, not `consumeMessages` directly, and assert on the value `Consume` **returns**.
   - Construct the consumer so that every field `Consume` dereferences before reaching the error handler is non-nil. Derive that set from `Consume`'s own source. Do **not** copy an existing helper's field list blindly: `newOffsetConsumerForTest` sets only `batchSize`, `errorHandler` and `metrics` (so `saramaClientProvider` is nil and `Consume` panics on its first statement), while `buildSkipTestConsumer` sets most fields but **not `errorHandler`** — it is safe only for its own corruption test, which short-circuits before the handler call. A transient error does not short-circuit; it reaches `c.errorHandler.HandleError`.
   - Cover, table-driven: each transient cause from requirement 1 → marker is true on the returned error; a rebalance-timeout / auth-failure consumer error → not marked; an `OffsetOutOfRange` error → not marked; a corruption-class error → not marked.
   - **Add one case that exercises the inner-loop `HandleError` call site.** ⚠️ A single pre-filled run is **not** deterministic: with both channels pre-filled the outer `select` picks randomly between `Errors()` and `Messages()`, and the inner `select` has a `default` that returns the batch as soon as no error is ready — so a one-sided (outer-only) marker passes whenever the outer `select` happens to pick the errors channel. Run the scenario in a **loop of ~50 iterations with a fresh fake partition consumer and a fresh consumer each iteration**, asserting the marker on the returned error every iteration; any iteration that reaches the inner site fails a one-sided implementation. (Requirement 2's shared-helper mandate is the structural guarantee; this loop is the behavioural check on top of it.)
   - Assert the wrapper preserves the chain: `errors.Is(returnedErr, io.EOF)` (or `errors.As` to the original `*sarama.ConsumerError`) is still true.
   - For the corruption case, leave `consumerOptions.SkipCorruptBatches` at its default `false` so the corruption error reaches the handler and is returned (the marker assertion then sees it). If you instead want to exercise the skip path, pin `SkipCorruptBatches=true` **and** supply a non-nil `skipper`, since that path calls `c.skipper.FindNextHealthyOffset`.
   - Assert the returned error's message still contains `parition consumer returns error` and `consume failed`.

9. Do not add a metric, a log line, a config flag, or a tunable cause list.

Before you finish: re-run `<verification>` and confirm it passes; walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT import the reporter library or introduce a shared marker package. The marker is structural: `interface{ Transient() bool }`.
- Do NOT make the consumer error handler return nil (requirement 7).
- Do NOT change the returned error's message text or wrapping order.
- Do NOT mark `OffsetOutOfRange` or corruption-class errors.
- Do NOT add retry, backoff, reconnect or coalescing logic — this classifies only.
- Do NOT add a new module dependency; `go.mod` and `go.sum` must be unchanged.
- Tests in the internal package use plain `testing.T`, not Ginkgo (requirement 8).
- Existing tests must still pass.
</constraints>

<verification>
ROOTDIR=/workspace make precommit
go test -run TestConsume_TransientBrokerErrorSatisfiesMarker ./...
! grep -q 'return nil' kafka_consumer-error-handler.go

All three must pass. `make precommit` must exit 0. The `go test -run` line makes the spec's named evidence test fail loudly if the test name drifts — without it, a renamed test makes that command match nothing and exit 0. The `grep` line asserts the frozen anti-pattern was not introduced; it is written as `! grep -q` because `grep -c` exits 1 when the count is zero and would fail the step it was meant to pass. (`ROOTDIR=/workspace` is the canonical container form; this repo's `Makefile` does not itself consume `ROOTDIR`, so the prefix is harmless rather than load-bearing. No git command appears in this verification, so `.git` masking does not matter here.)
</verification>
