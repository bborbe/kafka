---
status: completed
spec: [bug-nuke-dev-gn]
summary: Verified the existing opt-in CRC-corruption skip and its restart-safe offset persistence, and documented opt-in, version floors (v1.23.0/v1.30.1), and the durable-offset-manager requirement in README.md only.
execution_id: 3697a795-eb4f-5ca3-bdab-df0522872f7f-exec-020-spec-bug-nuke-dev-gn-verify-and-document-skip-optin
dark-factory-version: dev
created: "2026-11-27T07:10:00Z"
queued: "2026-11-27T07:05:41Z"
started: "2026-11-27T07:08:11Z"
completed: "2026-11-27T07:10:19Z"
---

<!--
OPEN QUESTIONS FOR THE HUMAN REVIEWER (resolve at audit time; do NOT implement in this prompt):

1. The spec's "Recommended Fix" is almost entirely operational, not code:
   (a) set SKIP_CRC_CORRUPTION=true on the nuke-dev agent-task-executor deployment,
   (b) repair/rewrite partition 0 broker-side, OR wire a durable OffsetManager into CreateTaskView,
   (c) mark NUKE-DEV-GN resolved in Sentry.
   None of these can be done in this repository (github.com/bborbe/kafka): there is no deployment
   manifest, no broker access, no Sentry client, and `CreateTaskView` does not exist here
   (`grep -rn CreateTaskView .` returns nothing — it lives in the agent-task-executor repo).
   This prompt therefore implements the only library-side deliverable that remains: verify the
   opt-in path and the skip-offset persistence are present and released, and document how
   downstream consumers opt in and make the skip durable. If the intent was to patch the
   deployment / agent-task-executor repo, re-point the spec there instead of running this prompt here.

2. The library-side code fix for NUKE-DEV-GN is ALREADY present and released in this branch:
   - `WithSkipCorruptBatches(bool)` opt-in — released in v1.23.0 (CHANGELOG).
   - skip-path resume-offset persistence in `skipAndAdvance` — released in v1.30.1 (CHANGELOG),
     implemented by prompts/completed/016-spec-bug-nuke-dev-gn-persist-skipped-offset.md.
   - Three persist unit tests already exist in kafka_consumer-offset_internal_test.go.
   So there is no source/test change to make here. This prompt is verify + document only.

3. Persisting the skip only helps a consumer backed by a durable OffsetManager. The
   "non-group simple consumer" named in the spec (`NewSimpleConsumer` / `NewSimpleConsumerBatch`)
   uses the in-memory `NewSimpleOffsetManager`, which does NOT persist across restarts — that is the
   root cause of "replays from offset 0". Confirming that the downstream `CreateTaskView` wiring is
   switched to `NewOffsetConsumerBatch` with `NewStoreOffsetManager` / `NewSaramaOffsetManager`
   (or that the SKIP_CRC_CORRUPTION flag is enabled so each restart re-skips instead of
   crash-looping) is an agent-task-executor repo change, not a change here.
-->

<summary>
- The library already contains and has released the code fix for the reported corruption crash-loop.
- This prompt changes no Go code, no tests, and no dependencies.
- It confirms the opt-in corruption-skip path and its restart-safe offset persistence are present.
- It adds a short consumer-facing README note explaining how to opt in and which versions include it.
- The note records that corruption is fatal by default and that skipping is opt-in.
- The note records that the skip only survives a restart when the consumer uses a durable offset manager.
- The note explains the in-memory simple consumer re-skips (not crash-loops) on restart when the flag is on.
- Operational steps from the spec (deployment flag, broker repair, Sentry resolution) stay out of this repository and are raised as open questions.
</summary>

<objective>
Confirm that this library already ships the opt-in CRC-corruption skip and its restart-safe offset persistence that resolve `NUKE-DEV-GN`, and document in `README.md` how downstream consumers opt in, the minimum versions, and the durable-offset-manager requirement. Make no behavioural, test, or dependency change.
</objective>

<context>
Read `specs/in-progress/bug-nuke-dev-gn.md` — it is the contract for this prompt. Note its "Recommended Fix" is mostly operational; the library-side pieces already exist.

This repository is the library itself: `go.mod` declares `module github.com/bborbe/kafka`. The deployment flag (`SKIP_CRC_CORRUPTION`), the broker-side partition repair, the `CreateTaskView` wiring, and the Sentry resolution cannot be performed here. Do NOT look for those files in this repo.

Read (read-only, do NOT change) `kafka_consumer-offset.go` to confirm the fix is present:
- `ConsumerOptions.SkipCorruptBatches bool` (defaults to the zero value `false`).
- `WithSkipCorruptBatches(skip bool) func(*ConsumerOptions)` — the opt-in functional option.
- `IsCorruptionError(err error) bool` — detects `sarama.PacketDecodingError` and CRC-message text.
- `consumeMessages` — the two `if c.consumerOptions.SkipCorruptBatches && IsCorruptionError(err.Err)` branches (in the outer `select` and the inner `for`-loop `select`) that return the `errSkipCorruptBatch` sentinel.
- `skipAndAdvance` — closes the old partition consumer, calls `FindNextHealthyOffset`, resolves `newOffset < 0` to the high-water mark, persists the resume position via `c.offsetManager.MarkOffset(ctx, c.topic, partition, newOffset)` BEFORE `CreatePartitionConsumer(...)`, then increments the metric and logs the skipped range.
- `recoverFromConsumeError` — the `c.consumerOptions.SkipCorruptBatches && errors.Is(err, errSkipCorruptBatch)` branch that invokes `skipAndAdvance`.

Read (read-only, do NOT change) `kafka_consumer-offset_internal_test.go` to confirm the persistence behaviour is already tested:
- `TestSkipAndAdvance_PersistsHealthyOffsetThroughOffsetManager`
- `TestSkipAndAdvance_PersistsHighWaterMarkWhenCorruptionExtendsToEnd`
- `TestSkipAndAdvance_MarkOffsetError_PropagatesWithoutRecreating`
Do NOT add new tests; these already cover the `NUKE-DEV-GN` contract.

Read (read-only, do NOT change) the offset-manager constructors you will reference in the README note, so the prose is accurate:
- `kafka_offset-manager-simple.go` — `NewSimpleOffsetManager(initalOffset Offset, fallbackOffset Offset) OffsetManager`, an in-memory manager that does not persist.
- `kafka_offset-manager-store.go` — `NewStoreOffsetManager(offsetStore OffsetStore, initalOffset Offset, fallbackOffset Offset) OffsetManager`, durable via `OffsetStore`.
- `kafka_offset-manager-sarama.go` — `NewSaramaOffsetManager(saramaClient SaramaClient, group Group, initalOffset Offset, fallbackOffset Offset) OffsetManager`, durable via the Kafka group offset store.
- `kafka_consumer-simple.go` — `NewSimpleConsumer` / `NewSimpleConsumerBatch` build `NewSimpleOffsetManager(initalOffset, initalOffset)` (in-memory).
- `kafka_consumer-offset.go` — `NewOffsetConsumerBatch(... offsetManager OffsetManager ...)` accepts any `OffsetManager`, and every constructor forwards `options ...func(*ConsumerOptions)`.

Read `README.md`:
- The `## Installation` section has a `### Minimum version for closed-partition-consumer recovery` subsection (added for `NUKE-PROD-AJ`). The new subsection goes directly after that subsection and before `## Quick Start`.
- The `## Table of Contents` does not list the existing `### Minimum version...` subsection, so do NOT add the new subsection to the Table of Contents either.

Read `CHANGELOG.md`:
- `## v1.23.0` records `feat(consumer): add opt-in CRC corruption skip mode ... WithSkipCorruptBatches(bool)`.
- `## v1.30.1` records `fix: Persist the resume offset through the OffsetManager when skipping corrupt batches ...`.
- Both behavioural changes are already documented. Do NOT add a new changelog entry.

Read `docs/dod.md` for repo conventions. Note its Ginkgo/Counterfeiter line does not apply here: this prompt adds no tests.

Reference docs (available inside the execution container):
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-library-guide.md`
</context>

<requirements>
1. Do NOT change any `.go` file, `go.mod`, or `go.sum`. Before editing anything, confirm the following anchors exist exactly as described, using read-only inspection:
   - `SkipCorruptBatches bool` is a field of `ConsumerOptions` in `kafka_consumer-offset.go`.
   - `func WithSkipCorruptBatches(skip bool) func(*ConsumerOptions)` is declared in `kafka_consumer-offset.go`.
   - `func IsCorruptionError(err error) bool` is declared in `kafka_consumer-offset.go`.
   - Both `if c.consumerOptions.SkipCorruptBatches && IsCorruptionError(err.Err)` branches exist in `consumeMessages`.
   - `skipAndAdvance` calls `c.offsetManager.MarkOffset(ctx, c.topic, partition, newOffset)` before its `CreatePartitionConsumer(...)` call, and that call is guarded by an error return (no replacement consumer is created after a failed write).
   - `recoverFromConsumeError` invokes `skipAndAdvance` on `errSkipCorruptBatch`.
   - The three tests named in `<context>` exist in `kafka_consumer-offset_internal_test.go`.
   If any anchor is missing, STOP and report the discrepancy instead of re-implementing the fix — the canonical implementation is prompts/completed/004-crc-corruption-skip.md, prompts/completed/005-crc-skip-bugfix.md, and prompts/completed/016-spec-bug-nuke-dev-gn-persist-skipped-offset.md.

2. Add exactly one new subsection to `README.md`, immediately after the existing `### Minimum version for closed-partition-consumer recovery` subsection and before `## Quick Start`. Use this exact heading text so the anchor is stable:

```
### Corrupt-batch skip (CRC corruption) and offset persistence
```

   The body must be prose (optionally one short code snippet showing the option), accurate to the code you verified in requirement 1, and must state all of the following:
   - A CRC/decode corruption in a record batch is fatal by default; the consumer surfaces the error and stops. `SkipCorruptBatches` defaults to `false`.
   - Corruption skipping is opt-in per consumer via the `kafka.WithSkipCorruptBatches(true)` functional option, passed as an option to a consumer constructor (for example `kafka.NewSimpleConsumer(..., kafka.WithSkipCorruptBatches(true))` or `kafka.NewOffsetConsumerBatch(..., kafka.WithSkipCorruptBatches(true))`).
   - When enabled, on a corruption error the consumer closes the partition consumer, searches forward for the next healthy offset (falling back to the partition high-water mark when corruption runs to the end), and resumes there. It increments `kafka_consumer_corrupt_batch_skipped_total`.
   - The opt-in skip mode is available since `github.com/bborbe/kafka` v1.23.0.
   - From v1.30.1 the skip path also persists the resume offset through the consumer's `OffsetManager` before recreating the partition consumer, so a consumer backed by a durable offset manager does not re-walk the corrupt range after a restart.
   - The skip only survives a restart when the consumer is backed by a durable offset manager. `NewSimpleConsumer` and `NewSimpleConsumerBatch` use the in-memory `NewSimpleOffsetManager`, which does not persist: after a restart such a consumer resumes at its configured initial offset and, with the flag enabled, re-skips the corrupt range instead of crash-looping. For durable progress, use `NewOffsetConsumerBatch` with `NewStoreOffsetManager(offsetStore, initialOffset, fallbackOffset)` or `NewSaramaOffsetManager(saramaClient, group, initialOffset, fallbackOffset)`.
   - Keep the prose accurate to the code; do not claim behaviour the code does not implement. Do not mention the deployment flag, broker repair, or Sentry — those are operational and not part of the library.

3. Do NOT modify the `## Table of Contents` (the existing `### Minimum version...` subsection is not listed there, and the new subsection should not be either).

4. Do NOT add or modify a `CHANGELOG.md` entry. The behavioural changes are already recorded in `## v1.23.0` (opt-in skip mode) and `## v1.30.1` (offset persistence). This is a README documentation-only change; do not create a `## Unreleased` section or touch the frozen changelog preamble.

5. Out of scope, and must not be attempted here: setting `SKIP_CRC_CORRUPTION=true` on any deployment, broker-side partition repair or rewrite, wiring a durable `OffsetManager` into `CreateTaskView` (agent-task-executor repo), and resolving the issue in Sentry. These are named in the spec's open questions and must be tracked outside this repository.

6. Before finishing, re-run `<verification>` and confirm every command passes; walk each requirement above against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Do NOT change any `.go` file, `go.mod`, or `go.sum` (copied from the spec's scope: the library fix is already present and released)
- Do NOT change the `SkipCorruptBatches` default; it stays `false` (opt-in), and it must not be enabled anywhere in the library
- Do NOT change `skipAndAdvance`, `recoverFromConsumeError`, `consumeMessages`, `IsCorruptionError`, or any `OffsetManager` implementation
- Do NOT add new tests — the persistence contract is already covered by the three existing tests
- Do NOT add a CHANGELOG entry — the changes are already documented in v1.23.0 and v1.30.1
- Do NOT modify the README Table of Contents
- Do NOT add a metric, log line, config flag, tunable threshold, or disable switch
- Do NOT edit deployment manifests, environment variables, broker state, or the downstream agent-task-executor repo
- Existing tests must still pass
</constraints>

<verification>
`make precommit` — must pass from the project root.

Also confirm by inspection / grep (all must succeed):
- `grep -q 'Corrupt-batch skip (CRC corruption) and offset persistence' README.md` — the new subsection heading exists.
- `grep -q 'WithSkipCorruptBatches' README.md` — the opt-in option is documented.
- `grep -q 'v1.23.0' README.md` and `grep -q 'v1.30.1' README.md` — the version floors are documented.
- `grep -q 'NewStoreOffsetManager' README.md` and `grep -q 'NewSaramaOffsetManager' README.md` — the durable offset-manager guidance is documented.
- `grep -q 'NewSimpleOffsetManager' README.md` — the in-memory limitation is documented.
- Only `README.md` is modified: no `.go`, `go.mod`, or `go.sum` change is present in the diff.
- The new subsection is positioned after `### Minimum version for closed-partition-consumer recovery` and before `## Quick Start`.
</verification>
