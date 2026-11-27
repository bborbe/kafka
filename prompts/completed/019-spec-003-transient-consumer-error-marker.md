---
status: completed
spec: [003-transient-consumer-error-marker]
summary: Verified the pre-existing transient-error marker implementation against all spec 003 requirements, added the missing 'run failed' assertion to the primary test, and added the Unreleased changelog entry
execution_id: 3697a795-eb4f-5ca3-bdab-df0522872f7f-exec-019-spec-003-transient-consumer-error-marker
dark-factory-version: dev
created: "2026-11-27T06:49:43Z"
queued: "2026-11-27T06:58:12Z"
started: "2026-11-27T06:58:14Z"
completed: "2026-11-27T07:08:09Z"
branch: dark-factory/transient-consumer-error-marker
---

# Mark transient broker-unreachable consumer errors

<summary>
- Errors the shared offset consumer returns for a transient broker-unreachable cause become identifiable by a structural marker method.
- A downstream reporter can recognise that class without this library importing the reporter or any shared marker package.
- The consumer's behaviour is unchanged: the same error value, the same message text, the same teardown.
- The classification is a narrow positive list — unrelated, offset-out-of-range, and corruption errors stay unmarked.
- The marker survives the full wrapping chain, so it is reachable from the error the consumer actually returns.
- No new module dependency and no new shared package are introduced.
- A table-driven in-package test drives the real consume path and asserts the marker on the returned error.
- The frozen anti-pattern of swallowing the error in the handler is not reintroduced.
</summary>

<objective>
Make the offset consumer's transient broker-unreachable errors satisfy the structural interface `interface{ Transient() bool }` with `Transient()` returning true, so a reporter can suppress that class at its own boundary without this library knowing the reporter exists. Runtime behaviour, error values, and error text must stay identical — only the error's shape gains a method.
</objective>

<context>
Read `docs/dod.md` for project conventions. Note its line "Tests use Ginkgo/Gomega conventions": requirement 8 deliberately overrides it for the internal test package. `package kafka` and `package kafka_test` compile into one test binary and `kafka_suite_test.go` already holds the single permitted `RunSpecs`, so the internal test file uses plain `func Test` (29 of them). Do not "correct" the new tests to Ginkgo.

Read `kafka_consumer-offset.go` — specifically:
- `func (c *offsetConsumer) Consume(ctx context.Context) error` (first statement `c.saramaClientProvider.Client(ctx)`; ends with `run.CancelOnFirstError(ctx, runs...)` then `errors.Wrapf(ctx, err, "run failed")`).
- `func (c *offsetConsumer) consumeMessages(ctx context.Context, consumePartition sarama.PartitionConsumer) ([]*sarama.ConsumerMessage, error)` — note it has TWO error-handler call sites: one in the outer `select`, one in the inner `for` loop. Both wrap the handler result as `errors.Wrapf(ctx, err, "parition consumer returns error")`.
- The existing `IsCorruptionError` and `IsOffsetOutOfRange` helpers, and the wrapping chain `parition consumer returns error` → `consume failed` → `run failed`.

Read `kafka_consumer-error-handler.go` — `ConsumerErrorHandler`, `ConsumerErrorHandlerFunc`, `NewConsumerErrorHandler`. The frozen anti-pattern lives here: the handler must not return `nil`.

Read `kafka_consumer-offset_internal_test.go` — the in-package fakes and helpers: `fakePartitionConsumer` (fields `messages`, `errors`, `highWaterMarkOffset`, `closeCount`), `newOffsetConsumerForTest` (sets only `batchSize`, `errorHandler`, `metrics` — `saramaClientProvider` is nil so `Consume` panics on its first statement; it is a `consumeMessages`-level helper only), `buildSkipTestConsumer` (sets most fields but NOT `errorHandler`; safe only for its own corruption test, which short-circuits before the handler call). Derive the field path from `Consume`'s own source; do not copy either helper's field list.

Read `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` for the `github.com/bborbe/errors` wrapping / `Unwrap` conventions, and `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` for table-driven test style.

PRE-EXISTING STATE (read this before writing code): a prior pass already added `IsTransientBrokerError`, the `transientError` wrapper, `handleConsumerError`, and `TestConsume_TransientBrokerErrorSatisfiesMarker` to this repo, and they are merged into the current tree. Treat the requirements below as "ensure/verify", not "add blindly". If a required symbol is present, verify it satisfies the requirement and keep it; do not duplicate it. Two later changes extended `IsTransientBrokerError` to also classify `syscall.EPIPE` and network timeouts as transient — those additions are intentional and outside this spec; KEEP them, do not narrow the list by removing them. If every requirement is already satisfied, make no behavioural change and still run the full verification block.
</context>

<requirements>
1. Ensure a transient classification exists in package `kafka`, in `kafka_consumer-offset.go` next to `IsCorruptionError` and `IsOffsetOutOfRange`, with the signature `func IsTransientBrokerError(err error) bool`. It returns true when the cause at the end of the wrapped chain is one of:
   - `io.EOF`
   - `io.ErrUnexpectedEOF`
   - a connection-refused dial error (`errors.Is(cause, syscall.ECONNREFUSED)`, traversing `*net.OpError` / `*os.SyscallError`)
   - a connection-reset error (`errors.Is(cause, syscall.ECONNRESET)`)
   - `sarama.ErrBrokerNotAvailable` (`github.com/IBM/sarama`)
   Walk the chain with `errors.Is` / `errors.As` (the file aliases stdlib as `stderrors` and also imports `github.com/bborbe/errors`; match the surrounding code — the existing helper does `cause := errors.Cause(err)` then `errors.Is(cause, ...)` / `errors.As(cause, ...)`). It must return false for `nil`.
   Keep any additional clearly-transient causes already present (e.g. `syscall.EPIPE`, network timeouts) — do not remove them.

2. Ensure the value placed into the returned error's chain satisfies `interface{ Transient() bool }` and that its `Transient()` returns true when requirement 1 classifies the error transient. The existing implementation uses `type transientError struct { err error }` with `Error() string`, `Unwrap() error`, and `Transient() bool`. The marker must be reachable via `errors.As` from the RETURNED error — through the full `run failed: consume failed: parition consumer returns error: …` chain — not only from the innermost cause. The wrapper's `Unwrap()` must return the original `*sarama.ConsumerError` so `errors.Is` / `errors.As` still reach the cause; a wrapper carrying only `Error()` and `Transient()` would cut the cause off and is wrong.

3. Apply the marker through ONE shared helper that BOTH `consumeMessages` error-handler call sites invoke (the outer `select` and the inner `for` loop). The existing helper is `func (c *offsetConsumer) handleConsumerError(err *sarama.ConsumerError) error`, which calls `c.errorHandler.HandleError(err)` and wraps the result with the marker when transient. Do not duplicate the marking logic at each site; a one-sided edit must be impossible by construction.

4. `OffsetOutOfRange` errors must NOT be marked. They are self-healed by the existing opt-in reset path (spec 002) and are a distinct class. Do not route them through the marker.

5. Errors for which the existing `IsCorruptionError(err)` returns true must NOT be marked. They are owned by the CRC-recovery workstream and must stay loud.

6. Any error not classified transient by requirement 1 must NOT be marked — either it does not satisfy the marker interface or `Transient()` returns false. Be consistent.

7. The returned error's message text and underlying cause must not change: `parition consumer returns error`, `consume failed`, and `run failed` must all still appear in the chain, and `errors.Is` / `errors.As` must still reach the original `*sarama.ConsumerError`. Adding the marker wrapper to the chain (requirement 2) is the one permitted structural addition. Do NOT add a metric, a log line, a config flag, or a tunable cause list.

8. Do NOT modify `NewConsumerErrorHandler` (or any handler) to return `nil`. This is the frozen anti-pattern: swallowing the error leaves the partition consumer stalled at an invalid offset. The marker labels; it does not suppress. `grep -n 'return nil' kafka_consumer-error-handler.go` must return 0 lines.

9. Add (or verify) in-package tests in `kafka_consumer-offset_internal_test.go` using plain `func Test` (see requirement 8's note about the Ginkgo collision). This deliberately overrides the `docs/dod.md` line "Tests use Ginkgo/Gomega conventions".
   - Name the primary transient test exactly `TestConsume_TransientBrokerErrorSatisfiesMarker` (spec 003 AC 1 evidence).
   - Drive the REAL `Consume(ctx)` — not `consumeMessages` directly, and not a constructor-only fixture — and assert on the value `Consume` RETURNS.
   - Construct the consumer so every field `Consume` dereferences before reaching the error handler is non-nil. Derive that set from `Consume`'s own source: at minimum `saramaClientProvider`, `saramaConsumerFunc`, `topic`, `offsetManager`, `metrics`, `errorHandler`, `consumerOptions`, and (for the inner-loop path that reads a message first) `batchSize`, `waiter`, and `logSampler`. A transient error does NOT short-circuit to the corruption branch; it falls through to `c.errorHandler.HandleError`. Do NOT copy `newOffsetConsumerForTest` (nil `saramaClientProvider` → panic) or `buildSkipTestConsumer` (no `errorHandler` → panic at the handler call).
   - Deliver each cause by sending a `*sarama.ConsumerError{Topic, Partition, Err: <cause>}` on `fakePartitionConsumer.errors`, and assert on the returned error that `var t interface{ Transient() bool }; errors.As(err, &t) && t.Transient()` is true.
   - Table-driven over ALL causes from requirement 1: `io.EOF`, `io.ErrUnexpectedEOF`, a `connection refused` dial error, a `connection reset` error, and `sarama.ErrBrokerNotAvailable` (keep the last only if it reaches the consumer through this path — if implementation shows it never does, remove it from the classification and from this test together so they stay in step).
   - Assert the marker is found through the outer wraps: the returned error message contains `run failed` and `consume failed` (and `parition consumer returns error`).
   - Negative cases in the same table, asserting the marker is absent or returns false: a rebalance-timeout / auth-failure `*sarama.ConsumerError`, an `OffsetOutOfRange` error (`sarama.ErrOffsetOutOfRange`), and a corruption-class error for which `IsCorruptionError` is true (e.g. `sarama.PacketDecodingError{Info: "CRC mismatch"}`). Leave `consumerOptions.SkipCorruptBatches` at its default `false` so the corruption error reaches the handler and is returned; if you instead exercise the skip path, pin `SkipCorruptBatches=true` AND supply a non-nil `skipper`.
   - Assert the wrapper preserves the chain: `errors.Is(returnedErr, <original cause>)` (or `errors.As` to the original `*sarama.ConsumerError`) is still true.
   - Add a loop test (e.g. `TestConsume_TransientBrokerError_InnerErrorCallSite`, ~50 iterations, fresh fake and consumer each iteration) that pre-fills BOTH `errors` and `messages` with a transient cause and a message, so the random outer-`select` choice and the inner `select` both get exercised; assert the marker on every returned error. This is the behavioural check that both handler call sites are marked.

Before you finish: run the full `<verification>` block and walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change consumer behaviour: the error is still returned, the consumer still tears down, the partition function still fails. This adds a method to an error, nothing more.
- Do NOT make the consumer error handler return `nil` for transient errors. Frozen anti-pattern carried from spec 002; `grep -n 'return nil' kafka_consumer-error-handler.go` must return 0 lines.
- Do NOT mark an offset-out-of-range error (spec 002 owns it) or a corruption / CRC error (CRC-recovery owns it).
- Do NOT import the reporter library or add a shared "errors"/marker package. The marker is structural: `interface{ Transient() bool }`.
- Do NOT add retry, backoff, reconnect, or coalescing logic — this classifies only.
- Do NOT change the returned error's message text (`parition consumer returns error`, `consume failed`, `run failed`) or the wrapping order.
- Do NOT add a metric, a log line, an opt-in flag, or a configurable cause list.
- Do NOT add a new module dependency; `go.mod` and `go.sum` must stay unchanged.
- Tests in the internal package use plain `testing.T`, not Ginkgo (requirement 9).
- Existing tests must still pass.
</constraints>

<verification>
make precommit
go test -run 'TestConsume_TransientBrokerErrorSatisfiesMarker|TestConsume_TransientBrokerError_InnerErrorCallSite' ./...
! grep -q 'return nil' kafka_consumer-error-handler.go

All three must exit 0. `make precommit` is the repo's full gate (format, generate, test, lint, vet, vuln/security, license). The targeted `go test -run` makes the spec's named evidence tests fail loudly if their names drift — without it a renamed test makes the command match nothing and exit 0. The `grep` line asserts the frozen anti-pattern was not introduced; it is written `! grep -q` because `grep -c` exits 1 when the count is zero and would fail the step it was meant to pass.

Module-dependency invariance (spec 003 AC 9, `git diff origin/master..HEAD -- go.mod go.sum`) is NOT checked here: this repo runs with `.git` masked (hideGit), so a git diff inside the container reads empty regardless and would be a false positive. Dependency invariance is verified on the spec's operator-side ladder. Do not add a dependency regardless.
</verification>
