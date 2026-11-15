---
spec: ["bug-nuke-dev-fb"]
status: draft
created: "2026-11-15T15:40:51Z"
---

# Classify broken-pipe broker writes as transient

<summary>
- Broker write failures caused by a broken pipe are now treated as transient
- The offset consumer no longer aborts every partition when the broker closes the write side of the connection
- Downstream Sentry reporting can suppress this expected class, matching the existing connection-refused and connection-reset handling
- Behaviour for every other error class is unchanged
- New test cases cover the broken-pipe cause both directly and through a network-error wrapper
- A changelog entry records the fix
</summary>

<objective>
Add `syscall.EPIPE` to `IsTransientBrokerError`'s positive cause list so a sarama `ConsumerError` caused by a broken-pipe broker write is marked transient, restoring the default `Transient()` Sentry exclusion and preventing the partition abort (`consume failed -> run failed -> pod restart`) seen in `NUKE-DEV-FB`.
</objective>

<context>
Read `specs/in-progress/bug-nuke-dev-fb.md` — it is the contract for this prompt.

Read `kafka_consumer-offset.go`:
- `IsTransientBrokerError(err error) bool` — the positive cause list. It already imports `syscall`, and imports the standard `errors` package as `stderrors` while `github.com/bborbe/errors` is imported as `errors`. Use the existing `errors.Is` idiom already in this function; do not add imports.
- `transientError` and its `Transient() bool` / `Unwrap() error` methods, and `handleConsumerError`, which marks transient errors. Do not change either.
- `errors.Cause(err)` is already used in this function and in `IsOffsetOutOfRange`; `*sarama.ConsumerError` implements `Unwrap()` (verified in `github.com/IBM/sarama@v1.61.1`), so the cause reaches `syscall.EPIPE` when a broker write fails with a broken pipe.

Read `kafka_consumer-offset_internal_test.go`:
- `TestConsume_TransientBrokerErrorSatisfiesMarker` — the table-driven test that drives the real `Consume(ctx)` through a `*sarama.ConsumerError`. It already builds `connectionRefused` / `connectionReset` as `*net.OpError` wrapping `os.NewSyscallError(...)`, and asserts both `hasTransientMarker(returnedErr)` and `errors.Is(returnedErr, tt.err)`.
- `newTransientTestConsumer`, `newConsumerError`, `hasTransientMarker` — reuse them; no new scaffolding is needed.
- Tests in this internal package are plain `testing.T`, NOT Ginkgo. `docs/dod.md`'s "Tests use Ginkgo/Gomega conventions" line is stale for this file: `package kafka` and `package kafka_test` share one test binary and `kafka_suite_test.go` holds the single permitted `RunSpecs`. Do not convert the new rows to Ginkgo.

Read `kafka_broken-pipe-error.go` — `IsBrokenPipeError` uses a message-suffix check. It is a separate helper and is NOT the mechanism for this fix; do not modify it.

Read `CHANGELOG.md` — there is an existing `## Unreleased` section with one bullet; append to it.

Read `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` for the repo's error-wrapping conventions.
</context>

<requirements>
1. In `IsTransientBrokerError` in `kafka_consumer-offset.go`, add `errors.Is(cause, syscall.EPIPE)` to the existing positive cause list, alongside `io.EOF`, `io.ErrUnexpectedEOF`, `syscall.ECONNREFUSED`, `syscall.ECONNRESET`, and `sarama.ErrBrokerNotAvailable`. Do not reorder or remove the existing causes. No new imports are required.
2. Update the doc comment above `IsTransientBrokerError` to include a broken pipe among the transient conditions, preserving the existing rationale that these errors are expected during broker restarts/failover and do not require operator attention. Keep the line wrapping within the repo's 100-column `golines` limit.
3. Extend the `TestConsume_TransientBrokerErrorSatisfiesMarker` table in `kafka_consumer-offset_internal_test.go` with EPIPE rows that follow the existing `connectionRefused` / `connectionReset` pattern. Define a `brokenPipe` value as `&net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", syscall.EPIPE)}` next to the existing op-error values, then add:
   - `{name: "broken pipe", err: os.NewSyscallError("write", syscall.EPIPE), transient: true}`
   - `{name: "broken pipe op error", err: brokenPipe, transient: true}`
   The existing loop drives `Consume(ctx)` and asserts both the transient marker and `errors.Is(returnedErr, tt.err)`; no new test function or helper is needed. Do not change any other table row.
4. Do not modify `kafka_broken-pipe-error.go` or `IsBrokenPipeError`. The fix must be syscall-based so it survives the wrapped `*sarama.ConsumerError` chain, not message-suffix-based.
5. Append a new bullet to the existing `## Unreleased` section of `CHANGELOG.md` (do not replace the existing bullet): `- fix: Treat a broken pipe (syscall.EPIPE) on a broker write as a transient broker-unreachable error so it no longer aborts the partition`.
6. Out of scope: bumping `github.com/bborbe/kafka` in the downstream trading Kafka modules (a different repository) and bounded partition-consumer recovery (a separate spec). Do not attempt either here.
7. Before finishing, re-run `<verification>` and confirm it passes; walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Do NOT add a new module dependency; `go.mod` and `go.sum` must be unchanged
- Do NOT change the existing positive cause list order or any existing cause
- Do NOT add a metric, log line, config flag, tunable threshold, or disable switch
- Do NOT change `transientError`, `handleConsumerError`, `Consume`, or `consumeMessages`
- Do NOT modify `IsBrokenPipeError` or `kafka_broken-pipe-error.go`
- Tests in the internal package use plain `testing.T`, not Ginkgo
- Existing tests must still pass
</constraints>

<verification>
`make precommit` — must pass from the project root.
`go test -mod=mod -run TestConsume_TransientBrokerErrorSatisfiesMarker -count=1 .` — must pass.
`grep -q 'syscall.EPIPE' kafka_consumer-offset.go` — must pass (the classifier uses EPIPE).
`grep -q 'syscall.EPIPE' kafka_consumer-offset_internal_test.go` — must pass (the EPIPE test rows exist).
</verification>
