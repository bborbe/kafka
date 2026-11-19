---
status: completed
spec: [003-transient-consumer-error-marker]
summary: Confirmed the transient broker-unreachable marker implementation and required tests are in place and passing, and added the missing Unreleased changelog entry.
execution_id: df0d4f74-cbc6-533a-9b06-a54cb6c72c1e-exec-018-spec-003-transient-consumer-error-marker
dark-factory-version: dev
created: "2026-11-19T02:00:12Z"
queued: "2026-11-19T02:08:29Z"
started: "2026-11-19T02:08:30Z"
completed: "2026-11-19T02:16:23Z"
branch: dark-factory/transient-consumer-error-marker
---

# Mark transient broker-unreachable consumer errors

<summary>
- Errors the offset consumer returns for a transient broker-unreachable cause gain a structural marker method
- A downstream reporter can recognise that class without this library importing the reporter, or any shared marker package
- The consumer's runtime behaviour, returned error value and error text are unchanged — the marker only labels
- The classification is a narrow positive list; offset-out-of-range, corruption, auth and unclassified errors stay unmarked
- No new module dependency, metric, log line, config flag or tunable list is introduced
- New in-package tests drive the real consume path and cover every marked and unmarked class
</summary>

<objective>
Make the offset consumer's transient broker-unreachable errors satisfy the structural interface `interface{ Transient() bool }` with `Transient()` returning true, so a reporter can suppress that class at its own boundary without this library knowing the reporter exists. The returned error value, message text and runtime behaviour stay byte-identical.
</objective>

<context>
Read `docs/dod.md`. Its line "Tests use Ginkgo/Gomega conventions" is stale for the internal test package and requirement 7 deliberately overrides it: `package kafka` and `package kafka_test` compile into one test binary, and `kafka_suite_test.go` already holds the single permitted `RunSpecs`. Do not convert the new tests to Ginkgo.

Read `kafka_consumer-offset.go`:
- `Consume(ctx) error` — the real entry point; note every field it dereferences on the path to the error handler (requirement 7).
- `consumeMessages` — it has **two** `c.errorHandler.HandleError(err)` call sites: one in the outer `select`, one in the inner `for` loop. Both must be covered.
- The existing helpers `IsCorruptionError` and `IsOffsetOutOfRange` — the new classifier sits next to them.
- The wrap chain produced with `errors.Wrapf` (`github.com/bborbe/errors`): `parition consumer returns error` (in `consumeMessages`) → `consume failed` (per-partition run) → `run failed` (`run.CancelOnFirstError` collapse in `Consume`). Note the misspelled `parition` — preserve it exactly. This file imports the standard `errors` package as `stderrors` and `github.com/bborbe/errors` as `errors`; `io`, `syscall` and `sarama` are already imported.

Read `kafka_consumer-error-handler.go` — `ConsumerErrorHandler`, `ConsumerErrorHandlerFunc`, `NewConsumerErrorHandler` (it returns the error, never nil).

Read `kafka_consumer-offset_internal_test.go` — the in-package fakes (`fakePartitionConsumer`, `fakeOffsetManager`, `fakeMetricsConsumer`, `testSaramaClientProvider`, `testSaramaClient`, `testSaramaConsumer`, `fakeSkipper`, `fakeLogSampler`, `fakeWaiter`) and the plain `func Test` style (no Ginkgo in this file). Note the imports: `stderrors "errors"`, `"github.com/bborbe/errors"` as `errors`, `io`, `net`, `os`, `syscall`, `sarama`.

Read `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` for the repo's error-wrapping conventions, and `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` for test style.
</context>

<requirements>
1. Add `IsTransientBrokerError(err error) bool` to `kafka_consumer-offset.go`, placed next to `IsCorruptionError` and `IsOffsetOutOfRange`. It returns `false` for a nil error. It returns `true` iff the cause at the end of the wrapped chain is one of:
   - `io.EOF`
   - `io.ErrUnexpectedEOF`
   - a connection-refused error: `errors.Is(cause, syscall.ECONNREFUSED)` (traverses `*net.OpError` / `*os.SyscallError`)
   - a connection-reset error: `errors.Is(cause, syscall.ECONNRESET)`
   - `sarama.ErrBrokerNotAvailable` (`github.com/IBM/sarama`)

   Resolve the cause with `errors.Cause(err)` (`github.com/bborbe/errors`, which delegates to `github.com/pkg/errors` and walks the `Unwrap` chain), then test membership with `errors.Is`. `*sarama.ConsumerError` implements `Unwrap()` returning its `Err`, so the cause is reached through the consumer-error wrapper. Do not add any cause beyond this list — the classification is deliberately narrow. (`errors.Is` on the cause also matches a `*net.OpError` / `*os.SyscallError` that wraps the syscall, which is what the connection-refused and connection-reset causes look like in practice.)

2. Add an unexported error-wrapper type (e.g. `transientError`) with three methods:
   - `Error() string` returning the wrapped error's message, so the text is unchanged;
   - `Unwrap() error` returning the original error, so `errors.Is` / `errors.As` still reach the original `*sarama.ConsumerError` and its cause;
   - `Transient() bool` returning `true`.

   The marker is structural: `interface{ Transient() bool }`. Do not import the reporter library and do not introduce a shared marker package.

3. Route **both** `c.errorHandler.HandleError(err)` call sites in `consumeMessages` through a single shared helper on `*offsetConsumer` (e.g. `func (c *offsetConsumer) handleConsumerError(err *sarama.ConsumerError) error`). The helper:
   - calls `c.errorHandler.HandleError(err)`;
   - returns `nil` when the handler returned `nil` (preserve the handler's existing contract — do not add a new `return nil` path inside `NewConsumerErrorHandler`);
   - if `IsTransientBrokerError(handledErr)` is true, returns `&transientError{err: handledErr}`;
   - otherwise returns `handledErr` unchanged.

   A one-sided edit must be impossible by construction: do not duplicate the marking logic at each call site.

4. The marker must be reachable via `errors.As` from the **value `Consume` returns**, through the full `run failed: consume failed: parition consumer returns error: …` chain — not only from the innermost cause. Because the helper wraps the handler's error before the three `errors.Wrapf` wraps are applied, `errors.As(returnedErr, &marker)` must find the wrapper on the returned error.

5. Negative classes must NOT be marked (neither satisfy the marker nor return true): offset-out-of-range errors (`IsOffsetOutOfRange`), corruption-class errors (`IsCorruptionError`), auth failures, rebalance timeouts, and any other unclassified error. The positive-list classifier naturally excludes these; do not add special-case branches for them.

6. Behaviour and text are unchanged: the same error value with the same wrapped message is returned from the same call sites on the same conditions. `parition consumer returns error`, `consume failed` and `run failed` must all still appear in the returned error's message, in that order, and `errors.Is` / `errors.As` must still reach the original `*sarama.ConsumerError` and its cause. The one permitted structural addition is the wrapper from requirement 2.

7. Add tests to `kafka_consumer-offset_internal_test.go` using plain `func Test` (this deliberately overrides the stale `docs/dod.md` Ginkgo line — see `<context>`).
   - Name the primary test `TestConsume_TransientBrokerErrorSatisfiesMarker`.
   - Drive the **real** `Consume(ctx)` — not `consumeMessages` directly — and assert on the value `Consume` **returns**.
   - Construct the consumer so **every field `Consume` dereferences before reaching the error handler is non-nil**: `saramaClientProvider` (whose `Client(ctx)` returns a `testSaramaClient` with `partitions: []int32{0}`), `saramaConsumerFunc` (returning a `testSaramaConsumer` whose `consumePartitionFn` returns your `*fakePartitionConsumer`), `offsetManager`, `metrics`, `errorHandler`, `batchSize`, and `topic`. Do NOT copy an existing helper's field list blindly: `newOffsetConsumerForTest` sets only `batchSize`, `errorHandler` and `metrics` (so `saramaClientProvider` is nil and `Consume` panics on its first statement), and `buildSkipTestConsumer` omits `errorHandler` (safe only for its corruption test, which short-circuits before the handler; a transient error does not short-circuit and reaches `c.errorHandler.HandleError`). Set `skipper` only if you exercise the corruption-skip path.
   - Table-driven, with a fresh `*fakePartitionConsumer` and a fresh consumer per case, delivering each cause as the `Err` of a `*sarama.ConsumerError` on the fake's `Errors()` channel:
     - transient (marked): `io.EOF`; `io.ErrUnexpectedEOF`; a connection-refused `*net.OpError` wrapping `os.NewSyscallError("connect", syscall.ECONNREFUSED)`; a connection-reset `*net.OpError` wrapping `os.NewSyscallError("read", syscall.ECONNRESET)`; and `sarama.ErrBrokerNotAvailable`;
     - not marked: a rebalance-timeout / auth-failure `*sarama.ConsumerError`; a `sarama.ErrOffsetOutOfRange` error; and a corruption-class error (`sarama.PacketDecodingError{Info: "CRC mismatch"}`).
   - Pin `consumerOptions` explicitly for the negative cases rather than leaving it unset: for the corruption case set `SkipCorruptBatches: false` (so the corruption error reaches the handler and is returned); for the offset-out-of-range case set `AutoResetOffsetOutOfRange: false` (same reason). If you instead exercise the skip/reset paths, supply a non-nil `skipper` because those paths call `c.skipper.FindNextHealthyOffset`.
   - Assert on the returned error: declare `var m interface{ Transient() bool }`, require `errors.As(returnedErr, &m)` to be true, and require `m.Transient()` to equal the expected boolean; also require the message to contain `parition consumer returns error` and `consume failed`, and require `errors.Is(returnedErr, <cause>)` to still be true (the wrapper preserves the chain).
   - Add one case that exercises the **inner-loop** `HandleError` call site: pre-fill both `Errors()` and `Messages()`, run ~50 iterations with a fresh fake and a fresh consumer each iteration, and assert the marker on the returned error every iteration. (Both channels pre-filled makes the outer `select` pick randomly, so a one-sided outer-only implementation fails some iterations; requirement 3's shared helper is the structural guarantee, this loop is the behavioural check.)
   - Tests in this internal package must use plain `testing.T`; do not use Ginkgo/Gomega.

8. Do not add a metric, log line, config flag, opt-in switch, tunable cause list, retry, backoff, reconnect or coalescing logic. The marker is the whole observable.

Before you finish: re-run `<verification>` and confirm it passes; walk each acceptance criterion from the spec against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change consumer runtime behaviour: the same error is returned, the consumer still tears down, the partition function still fails.
- Do NOT make the consumer error handler return `nil` for transient errors. Frozen anti-pattern (carried from spec 002): swallowing the error stalls the partition at an invalid offset. The marker labels; it does not suppress.
- Do NOT mark offset-out-of-range or corruption-class errors.
- Do NOT import the reporter library or introduce a shared marker package; the marker is structural `interface{ Transient() bool }`.
- Do NOT change the returned error's message text or wrapping order (`parition consumer returns error` → `consume failed` → `run failed`).
- Do NOT add retry, backoff, reconnect or coalescing logic — this classifies only.
- Do NOT add a new module dependency; `go.mod` and `go.sum` must be unchanged.
- Tests in the internal package use plain `testing.T`, not Ginkgo/Gomega.
- Existing tests must still pass.
</constraints>

<verification>
```
make precommit
go test -mod=mod -run TestConsume_TransientBrokerErrorSatisfiesMarker -count=1 ./...
! grep -q 'return nil' kafka_consumer-error-handler.go
```

All three must pass. `make precommit` must exit 0. The `go test -run` line makes the spec's named evidence test fail loudly if the test name drifts — without it, a renamed test matches nothing and exits 0. The `grep` line asserts the frozen anti-pattern was not introduced; it is written as `! grep -q` because `grep -c` exits 1 when the count is zero and would fail the step it was meant to pass. No git command is used here, so a masked worktree `.git` cannot produce a false pass.
</verification>
