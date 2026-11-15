---
status: completed
spec: [bug-nuke-prod-aj]
summary: Verified the closed-partition-consumer recovery fix and its tests are already present, and documented the v1.28.2 upgrade floor for NUKE-PROD-AJ in README.md with no source, test, dependency, or changelog changes.
execution_id: ac38495b-bdc4-5b89-b4b0-7abf6e28efe7-exec-017-spec-bug-nuke-prod-aj-verify-fix-and-upgrade-floor
dark-factory-version: dev
created: "2026-11-15T17:05:00Z"
queued: "2026-11-15T16:07:39Z"
started: "2026-11-15T16:07:48Z"
completed: "2026-11-15T16:40:02Z"
---

<!--
OPEN QUESTION FOR THE HUMAN REVIEWER (resolve at audit time):
The spec's "Recommended Fix" asks to bump github.com/bborbe/kafka from v1.28.1 to >= v1.28.2 in
the DOWNSTREAM nuke-prod consumer service's go.mod/go.sum. This repository IS
github.com/bborbe/kafka (see go.mod: `module github.com/bborbe/kafka`), so there is no
github.com/bborbe/kafka entry in this go.mod to bump. The library-side fix named in the spec
(upstream commit 6d79267, "Recover from a closed partition consumer instead of aborting the
consumer") is already present in this branch's history and was released in v1.28.2 (see
CHANGELOG.md). The current tag is v1.29.14, well above the required floor.

Consequence: there is no source, test, or dependency change to make in this repository. The only
actionable, non-redundant deliverable here is a consumer-facing note telling downstream users which
version contains the fix. If the intent was to patch the downstream service repo, re-point the spec
there instead of running this prompt here.
-->

# Verify NUKE-PROD-AJ library fix and document the upgrade floor

<summary>
- The library already contains the fix for the reported Sentry issue.
- A normally-closed partition consumer channel no longer aborts the whole consumer.
- The consumer recreates the partition consumer at the last good offset instead.
- Recovery is bounded to a small number of consecutive attempts and resets after a successful batch.
- Existing automated tests already lock this recovery behaviour, so no new tests are added.
- No source code, test, or dependency changes are required in this repository.
- The only change is a short consumer-facing note stating the minimum version that contains the fix.
- Bumping the dependency in the downstream service is out of scope because that service is a different repository.
</summary>

<objective>
Confirm that this library already ships the closed-partition-consumer recovery that resolves `NUKE-PROD-AJ`, and record in the consumer-facing documentation the minimum library version (`v1.28.2`) that downstream consumers must upgrade to. Make no behavioural, test, or dependency change.
</objective>

<context>
Read `specs/in-progress/bug-nuke-prod-aj.md` — it is the contract for this prompt.

This repository is the library itself: `go.mod` declares `module github.com/bborbe/kafka`. The spec's "Recommended Fix" (bump `github.com/bborbe/kafka` in the downstream nuke-prod consumer service) therefore cannot be performed here. Do NOT look for or edit a `github.com/bborbe/kafka` line in this repo's `go.mod`/`go.sum` — it does not and cannot exist.

Read `kafka_consumer-offset.go` to confirm the fix is present (do not change it):
- `errPartitionConsumerClosed` — the sentinel declared near `errSkipCorruptBatch` / `errOffsetOutOfRange`.
- `consumeMessages` — the four closed-channel branches (both `Errors()` branches and both `Messages()` branches) return `errPartitionConsumerClosed` instead of a fresh `errors.Errorf`.
- `recoverFromConsumeError` — when `errors.Is(err, errPartitionConsumerClosed)`, it returns `handled=true` and recreates the partition consumer at the recorded `nextOffset`, after closing the dead consumer; it does not recover when `client.Closed()` is true or when `attempt > maxConsecutivePartitionConsumerRecoveries`.
- `maxConsecutivePartitionConsumerRecoveries` — the bound (3).
- The per-partition run loop in `Consume` — increments `consecutiveClosedRecoveries` on a sentinel error and resets it to `0` after a successful `ConsumeMessages` batch.

Read `kafka_consumer-offset_internal_test.go` to confirm the behaviour is already tested (do not add duplicates):
- `TestConsumeMessages_OuterErrorsChannelClosed`, `TestConsumeMessages_OuterMessagesChannelClosed`, `TestConsumeMessages_InnerErrorsChannelClosed`, `TestConsumeMessages_InnerMessagesChannelClosed` — the four branch tests, each asserting `errors.Is(err, errPartitionConsumerClosed)`.
- `TestConsume_PartitionConsumerClosed_RecoversAndContinues` — recreates once at the last good offset, delivers the replacement's message, closes the dead consumer.
- `TestConsume_PartitionConsumerClosed_ClientClosedFailsWithoutRecreation` — no recreation when the client is closed.
- `TestConsume_PartitionConsumerClosed_BoundedAfterThreeRecoveries` — three recoveries then propagation.
- `TestConsume_PartitionConsumerClosed_SuccessfulBatchResetsAttemptCounter` — a successful batch resets the counter.

Read `README.md`:
- `## Installation` currently only shows `go get github.com/bborbe/kafka`. The new note goes directly after that block, before `## Quick Start`.

Read `CHANGELOG.md`:
- `## Unreleased` already contains the `fix:` bullet for this behaviour, and `## v1.28.2` records it as released. Do NOT add another changelog bullet; the fix is already documented.

Read `docs/dod.md` and `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` for repo conventions. Note that `docs/dod.md`'s Ginkgo/Counterfeiter line does not apply to `kafka_consumer-offset_internal_test.go`, which uses plain `testing.T`.
</context>

<requirements>
1. Do NOT change any `.go` file, `go.mod`, or `go.sum`. The fix and its tests are already present. Before editing anything, confirm the following anchors exist exactly as described, using read-only inspection:
   - `errPartitionConsumerClosed` is declared in `kafka_consumer-offset.go` and returned from all four closed-channel branches in `consumeMessages`.
   - `recoverFromConsumeError` returns `handled=true` for `errPartitionConsumerClosed` only when `!client.Closed()` and `attempt <= maxConsecutivePartitionConsumerRecoveries`, and it recreates at `nextOffset`.
   - `maxConsecutivePartitionConsumerRecoveries` is declared.
   - The run loop increments and resets `consecutiveClosedRecoveries`.
   If any anchor is missing, STOP and report the discrepancy instead of re-implementing the fix — the canonical implementation is upstream commit `6d79267` and prompt `prompts/completed/011-bug-partition-consumer-closed-sentinel.md`.
2. Confirm the existing tests listed in `<context>` are present and unmodified. Do NOT add new tests: the four branch tests plus the four `Consume`-level recovery tests already cover the `NUKE-PROD-AJ` contract, and duplicating them would add maintenance cost without new coverage.
3. Add exactly one new subsection to `README.md`, immediately after the `## Installation` code block and before `## Quick Start`, titled `### Minimum version for closed-partition-consumer recovery`. Its body must state, in prose:
   - Consumers must use `github.com/bborbe/kafka` v1.28.2 or newer to receive the fix for `NUKE-PROD-AJ`.
   - In older versions a normally-closed sarama partition-consumer channel is reported as a fatal `consume failed` error, which `run.CancelOnFirstError` escalates into a whole-consumer abort.
   - From v1.28.2 the consumer recreates the partition consumer at the last good offset instead, bounded to three consecutive recoveries and reset by a successful batch.
   Use the exact heading text above so the anchor is stable. Keep the prose accurate to the code you verified in requirement 1; do not claim behaviour the code does not implement.
4. Do NOT add or modify a `CHANGELOG.md` bullet. `## Unreleased` already carries the `fix:` entry and `## v1.28.2` records the release.
5. Out of scope, and must not be attempted here: bumping `github.com/bborbe/kafka` in the downstream nuke-prod consumer service (a different repository), releasing a new tag, or changing the `Consume`/`consumeMessages`/`recoverFromConsumeError` behaviour.
6. Before finishing, re-run `<verification>` and confirm every command passes; walk each requirement above against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- No call-site or exported-API change; the fix is internal to `offsetConsumer` (copied from the spec)
- Do NOT change any `.go` file, `go.mod`, or `go.sum`
- Do NOT add new tests — the recovery contract is already covered
- Do NOT add a CHANGELOG bullet — the fix is already documented in `## Unreleased` and `## v1.28.2`
- Do NOT add a metric, log line, config flag, tunable threshold, or disable switch
- Do NOT edit the downstream nuke-prod consumer service (a different repository)
- Existing tests must still pass
</constraints>

<verification>
`make precommit` — must pass from the project root.
`go test -mod=mod -run 'PartitionConsumerClosed|ConsumeMessages' -count=1 .` — must pass.
`grep -q 'errPartitionConsumerClosed' kafka_consumer-offset.go` — the sentinel is declared and used.
`grep -q 'maxConsecutivePartitionConsumerRecoveries' kafka_consumer-offset.go` — the recovery bound is present.
`grep -q 'TestConsume_PartitionConsumerClosed_BoundedAfterThreeRecoveries' kafka_consumer-offset_internal_test.go` — the bounded-recovery test is present.
`grep -q 'v1.28.2' README.md` — the upgrade floor is documented.
`grep -q 'NUKE-PROD-AJ' README.md` — the note names the issue.
</verification>
