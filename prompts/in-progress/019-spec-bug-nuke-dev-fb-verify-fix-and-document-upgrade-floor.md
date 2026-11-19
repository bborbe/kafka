---
status: approved
spec: [bug-nuke-dev-fb]
created: "2026-11-19T03:20:00Z"
queued: "2026-11-19T02:15:15Z"
---

<!--
OPEN QUESTION FOR THE HUMAN REVIEWER (resolve at audit time):
The spec's "Recommended Fix" asks to bump github.com/bborbe/kafka from v1.29.10 to v1.30.1
(or latest v1.30.x) in every bborbe/trading Kafka-consuming module and redeploy the candle
consumers. This repository IS github.com/bborbe/kafka (see go.mod: `module github.com/bborbe/kafka`),
so there is no github.com/bborbe/kafka entry in this go.mod to bump, and the trading modules and
candle consumers are different repositories not present in this worktree.

The library-side fix named in the spec is already present: `IsTransientBrokerError` in
`kafka_consumer-offset.go` includes `errors.Is(cause, syscall.EPIPE)`, its table test carries the
broken-pipe rows, and CHANGELOG.md records the fix under `## v1.30.1`. The current tag is
v1.30.1 + later commits, so the released floor is v1.30.1.

Consequence: there is no source, test, or dependency change to make in this repository. The only
actionable, non-redundant deliverable here is a consumer-facing note telling downstream users which
version contains the fix. If the intent was to patch the trading service repos, re-point the spec
there instead of running this prompt here.
-->

# Verify NUKE-DEV-FB transient broken-pipe fix and document the upgrade floor

<summary>
- The library already contains the fix for the reported Sentry issue.
- A broken-pipe broker write is now classified as a transient broker-unreachable error.
- Because the error is marked transient, downstream Sentry reporting can suppress it as expected noise instead of treating it as a fatal partition abort.
- Existing automated tests already lock the broken-pipe classification, so no new tests are added.
- No source code, test, or dependency changes are required in this repository.
- The only change is a short consumer-facing note stating the minimum version that contains the fix.
- Bumping the dependency in the downstream trading modules is out of scope because those are different repositories.
- The fix is already recorded in the changelog, so no changelog entry is added.
</summary>

<objective>
Confirm that this library already ships the `syscall.EPIPE` transient classification that resolves `NUKE-DEV-FB`, and record in the consumer-facing documentation the minimum library version (`v1.30.1`) that downstream consumers must upgrade to. Make no behavioural, test, or dependency change.
</objective>

<context>
Read `specs/in-progress/bug-nuke-dev-fb.md` — it is the contract for this prompt.

This repository is the library itself: `go.mod` declares `module github.com/bborbe/kafka`. The spec's "Recommended Fix" (bump `github.com/bborbe/kafka` in the downstream trading Kafka-consuming modules and redeploy the candle consumers) therefore cannot be performed here. Do NOT look for or edit a `github.com/bborbe/kafka` line in this repo's `go.mod`/`go.sum` — it does not and cannot exist.

Read `kafka_consumer-offset.go` to confirm the fix is present (do not change it):
- `IsTransientBrokerError(err error) bool` — the positive cause list already contains `errors.Is(cause, syscall.EPIPE)` alongside `io.EOF`, `io.ErrUnexpectedEOF`, `syscall.ECONNREFUSED`, `syscall.ECONNRESET`, and `sarama.ErrBrokerNotAvailable`.
- The doc comment above `IsTransientBrokerError` already mentions a broken-pipe connection.
- `transientError` with its `Transient() bool` / `Unwrap() error` methods, and `handleConsumerError`, which wraps a classified error as `&transientError{err: handledErr}`.

Read `kafka_consumer-offset_internal_test.go` to confirm the behaviour is already tested (do not add duplicates):
- `TestConsume_TransientBrokerErrorSatisfiesMarker` — the table-driven test that drives the real `Consume(ctx)` through a `*sarama.ConsumerError`. It defines a `brokenPipe` `*net.OpError` wrapping `os.NewSyscallError("write", syscall.EPIPE)` and has two rows: `"broken pipe"` (bare `os.NewSyscallError`) and `"broken pipe op error"` (the `*net.OpError`), both `transient: true`. The loop asserts `hasTransientMarker(returnedErr)` matches `tt.transient` and `errors.Is(returnedErr, tt.err)` still reaches the cause.
- `hasTransientMarker` — the structural `Transient() bool` interface check that mirrors how a downstream reporter (e.g. bborbe/service) recognises a transient error without importing the concrete type.
- `newTransientTestConsumer`, `newConsumerError` — the existing helpers. Tests in this internal package use plain `testing.T`, NOT Ginkgo; `kafka_suite_test.go` holds the single permitted `RunSpecs`.

Read `README.md`:
- `## Installation` is followed by the subsection `### Minimum version for closed-partition-consumer recovery` (the `NUKE-PROD-AJ` note), then `## Quick Start`. The new subsection goes immediately after the existing `### Minimum version for closed-partition-consumer recovery` block and before `## Quick Start`.

Read `CHANGELOG.md`:
- `## v1.30.1` already records the fix with the bullet `- fix: Treat a broken pipe (syscall.EPIPE) on a broker write as a transient broker-unreachable error so it no longer aborts the partition`. Do NOT add another changelog bullet; the fix is already documented.

Read `docs/dod.md` for repo conventions. Note that its Ginkgo/Counterfeiter line does not apply to `kafka_consumer-offset_internal_test.go`, which uses plain `testing.T`.

Read `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` for the repo's error-wrapping conventions (context only; no code change).
</context>

<requirements>
1. Do NOT change any `.go` file, `go.mod`, or `go.sum`. The fix and its tests are already present. Before editing anything, confirm the following anchors exist exactly as described, using read-only inspection:
   - `IsTransientBrokerError` in `kafka_consumer-offset.go` includes `errors.Is(cause, syscall.EPIPE)` in its positive cause list.
   - The doc comment above `IsTransientBrokerError` mentions a broken-pipe connection.
   - `handleConsumerError` wraps a classified error as `&transientError{err: handledErr}`.
   - `TestConsume_TransientBrokerErrorSatisfiesMarker` in `kafka_consumer-offset_internal_test.go` has the `brokenPipe` `*net.OpError` value and the two broken-pipe table rows, and asserts `hasTransientMarker(returnedErr)`.
   - `CHANGELOG.md`'s `## v1.30.1` section contains the broken-pipe `fix:` bullet quoted in `<context>`.
   If any anchor is missing, STOP and report the discrepancy instead of re-implementing the fix — the canonical implementation is prompt `prompts/completed/016-bug-nuke-dev-fb-epipe-transient.md`.
2. Confirm the existing tests listed in `<context>` are present and unmodified. Do NOT add new tests: `TestConsume_TransientBrokerErrorSatisfiesMarker` already drives the real `Consume(ctx)` path and asserts both the transient marker and the preserved cause for broken pipe, so duplicating it would add maintenance cost without new coverage.
3. Add exactly one new subsection to `README.md`, immediately after the existing `### Minimum version for closed-partition-consumer recovery` block and before `## Quick Start`, titled `### Minimum version for broken-pipe transient classification`. Its body must state, in prose:
   - Consumers must use `github.com/bborbe/kafka` v1.30.1 or newer to receive the fix for `NUKE-DEV-FB`.
   - In older versions (for example v1.29.10) a broken-pipe broker write (`syscall.EPIPE`) is not classified as transient, so a sarama `ConsumerError` escapes the downstream `Transient()` Sentry exclusion and aborts the partition (`consume failed -> run failed`).
   - From v1.30.1 `IsTransientBrokerError` treats `syscall.EPIPE` as a transient broker-unreachable condition, so the error is wrapped as a transient error and the partition is not aborted.
   Use the exact heading text above so the anchor is stable. Keep the prose accurate to the code you verified in requirement 1; do not claim behaviour the code does not implement.
4. Do NOT add or modify a `CHANGELOG.md` bullet. `## v1.30.1` already records the fix.
5. Out of scope, and must not be attempted here: bumping `github.com/bborbe/kafka` in the downstream trading Kafka-consuming modules (different repositories), redeploying the candle consumers, releasing a new tag, or changing `IsTransientBrokerError` / `transientError` / `handleConsumerError` behaviour.
6. Before finishing, re-run `<verification>` and confirm every command passes; walk each requirement above against the diff (which should contain only the new README subsection).
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Do NOT change any `.go` file, `go.mod`, or `go.sum`
- Do NOT add new tests — the broken-pipe classification contract is already covered
- Do NOT add a CHANGELOG bullet — the fix is already documented in `## v1.30.1`
- Do NOT add a metric, log line, config flag, tunable threshold, or disable switch
- Do NOT edit the downstream trading Kafka-consuming modules or candle consumers (different repositories)
- Do NOT change the existing positive cause list order or any existing cause in `IsTransientBrokerError`
- Existing tests must still pass
</constraints>

<verification>
`make precommit` — must pass from the project root.
`go test -mod=mod -run TestConsume_TransientBrokerErrorSatisfiesMarker -count=1 .` — must pass.
`grep -q 'syscall.EPIPE' kafka_consumer-offset.go` — the classifier uses EPIPE.
`grep -q 'broken pipe' kafka_consumer-offset_internal_test.go` — the broken-pipe test rows exist.
`grep -q 'NUKE-DEV-FB' README.md` — the note names the issue.
`grep -q 'v1.30.1' README.md` — the upgrade floor is documented.
</verification>
