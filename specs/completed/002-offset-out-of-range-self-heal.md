---
status: completed
approved: "2026-09-08T19:44:14Z"
verifying: "2026-09-08T20:10:52Z"
completed: "2026-09-08T20:18:05Z"
branch: dark-factory/offset-out-of-range-self-heal
---

## Summary

- Add an opt-in mode to the shared bborbe/kafka offset consumer that makes it self-heal when the consumer's stored offset falls outside the broker's retained range, by resetting to the oldest offset and resuming, instead of propagating the error and going down.
- The mode is OFF by default: every consumer behaves exactly as today (error propagates → consumer run fails → reported to Sentry upstream) unless its owner explicitly turns the mode on.
- When ON, the consumer logs the reset at warn level with topic + partition + old/new offset, resets the stored offset backward, recreates the partition consumer at the fallback offset (broker-resolved to oldest), and continues consuming.
- The reset MUST advance the read position by recreating the partition consumer — it MUST NOT swallow the error in the consumer error handler, which would silently stall the partition forever (the exact silent-data-loss the bug causes today).
- Scope is the bborbe/kafka library only; the trading-side flag that enables the mode (tick consumers) and the runtime offset-advance verification are a separate follow-up (owning task: "Self-Heal Kafka Consumer on OffsetOutOfRange Instead of Erroring", vault task 97caec10, deploy subtasks).

## Problem

When a `bborbe/kafka` offset consumer's stored offset falls outside the broker's retained range for its topic/partition (a normal, expected condition after a consumption gap or a retention prune), sarama surfaces `sarama.ErrOffsetOutOfRange` and the consumer's `consumeMessages` loop propagates it. The consumer run fails, the partition stops being consumed — silent data loss on tick ingestion — and the error is reported to Sentry. This is a routine condition, not a defect worth an alert, and it recurs: live Sentry issues **NUKE-PROD-9B** (`master-raw-mt5-tick-ftmo-520167206-input`, prod) and **NUKE-DEV-9A** (`develop-raw-mt5-tick-dwx-3000062117-input`) have each produced ~46-117 events over the last month. Note: Sentry ingestion has been dead org-wide since 2026-09-06T07:20Z (blocker: Sentry Has Received Zero Events Across All Four Projects Since 2026-09-06, SEV-2), so post-fix Sentry silence is NOT proof — the offset-advance check is the real proof.

## Reproduction

The bug is reproducible in the unit layer without a broker, using the existing in-package fake. The runnable test that asserts this is the new in-package test in `kafka_consumer-offset_internal_test.go` (to be added by this fix; the fix must include a regression test named for this repro, e.g. `TestOffsetConsumerAutoResetOffsetOutOfRange`) — run with `go test -run TestOffsetConsumerAutoResetOffsetOutOfRange ./...`:

1. Construct an offset consumer with `newOffsetConsumerForTest` and a `fakePartitionConsumer` whose error channel delivers `sarama.ErrOffsetOutOfRange` (wrapped in `*sarama.ConsumerError` with topic/partition set) before any message.
2. Call `Consume(ctx)`.
3. **Observed (today):** `Consume` returns a non-nil error wrapping the offset error (the error handler propagates it) and no further messages are consumed — the partition stalls. The error handler contract (`kafka_consumer-error-handler.go` `ConsumerErrorHandler.HandleError`) returns `err`, which the consume loop wraps and returns.
4. **Observed (after fix, option ON):** `Consume` does not return the offset error; the consumer logs a warn reset line, resets the stored offset backward, recreates the partition consumer at the fallback offset, and the subsequent good messages are delivered.

**Expected vs Actual**

- **Expected (documented):** per the offset consumer's error contract (see `ConsumerErrorHandler` interface in `kafka_consumer-error-handler.go` and the `OffsetManager` interface doc in `kafka_offset-manager.go`, which documents `ResetOffset` as the backward-movement counterpart to `MarkOffset`), an out-of-range stored offset is a recoverable condition the consumer can self-heal from when configured.
- **Actual:** the consumer errors out and the partition stops being consumed (silent data loss), with the error reported to Sentry as noise.

## Goal

The `bborbe/kafka` offset consumer can be configured, per consumer instance, to detect `OffsetOutOfRange` on a partition and reset to the oldest offset instead of erroring. Consumers that do not opt in keep the current error-and-fail behavior. When the reset path runs, it records the old and new offset in a warn log line, resets the stored offset backward so a restart does not recreate the stuck condition, recreates the partition consumer at the oldest offset, and resumes consuming.

## Non-goals

- Do NOT change default behavior. Without the opt-in, `OffsetOutOfRange` propagates and the consumer fails exactly as today.
- Do NOT blanket-suppress consumer errors. Only the `OffsetOutOfRange` error class is intercepted; all other consume errors still propagate to the error handler and to Sentry upstream.
- Do NOT wire any trading-repo flag (e.g. a main.go argument in the tick command handler). That is a separate follow-up task in the trading repo, done after this library is tagged.
- Do NOT reset to newest. Decision recorded: **oldest** — replays retained tick data rather than dropping it; no backfill work lost.
- Do NOT add configurable jump sizes, retry counts, or reset policies beyond a single on/off option — invariant; if a future consumer demands variation, that is a separate spec.
- Do NOT implement the reset by returning nil from the consumer error handler (`NewConsumerErrorHandler`) — invariant anti-pattern (see Constraints).
- Do NOT add a new metric for reset events. The warn log line is the reset's observability for this fix; a counter can be a separate follow-up if operators find the log insufficient. (Sibling corruption-skip increments `kafka_consumer_corrupt_batch_skipped_total` because it irreversibly drops batches — its skipped range is not re-read. A reset replays retained data, so the warn log recording the reset point is sufficient observability here; the replay itself is visible in the consumer-group lag.)

## Acceptance Criteria

- [ ] A `WithAutoResetOffsetOutOfRange(bool)` functional option exists on the offset consumer and defaults to OFF when not supplied — evidence: `grep -n 'WithAutoResetOffsetOutOfRange' kafka_consumer-offset.go` returns line ≥1.
- [ ] With the option OFF, an `OffsetOutOfRange` consumer error is returned/propagated as today — evidence: in-package unit test asserts `Consume` returns a non-nil error wrapping/equal to the offset error with the option absent; `go test` for that test passes (exit 0).
- [ ] With the option ON, the consumer detects `OffsetOutOfRange` via the `IsOffsetOutOfRange` helper, resets, and resumes consuming — evidence: in-package unit test using `fakePartitionConsumer` injects an `OffsetOutOfRange` error followed by good messages, and asserts (i) the consume loop's detection path calls `IsOffsetOutOfRange` (the helper is exercised, not dead code), (ii) `Consume` does not return the offset error, (iii) the good messages after the reset are delivered/processed, (iv) the partition consumer was recreated at the fallback offset; `go test` passes (exit 0).
- [ ] On a reset, a warn log line records topic, partition, old offset, and new offset — evidence: unit test captures the logger output and asserts it contains the topic, partition, and both offsets.
- [ ] On a reset, the stored offset is moved backward via `OffsetManager.ResetOffset` so a restart does not recreate the stuck condition — evidence: unit test asserts `ResetOffset` was invoked with the fallback offset (mock `KafkaOffsetManager` records the call); `grep -n 'ResetOffset' kafka_consumer-offset.go` returns ≥1 match.
- [ ] Only `OffsetOutOfRange` is intercepted; an unrelated consume error (e.g. rebalance timeout or a synthetic non-offset error) with the option ON still propagates via the error handler — evidence: unit test injects a non-offset consumer error and asserts `Consume` returns it (non-nil); `go test` passes (exit 0).
- [ ] The consumer error handler is NOT modified to return nil — evidence: `grep -n 'return nil' kafka_consumer-error-handler.go` returns 0 lines, AND a unit test asserts `NewConsumerErrorHandler(...).HandleError(<offset error>)` returns a non-nil error; the reset logic lives in the consume loop / partition-consumer recreation path, not the error handler.
- [ ] `make precommit` exits 0 in the kafka package — evidence: exit code 0.

## Verification

**Rung 1 — unit-replayable repro (in this library spec):**

```
make precommit
```

Expected: exit code 0. The new in-package unit tests (option OFF propagates; option ON resets and resumes; warn log records offsets; unrelated error still propagates; error handler unchanged) pass — the Reproduction steps above are asserted by these tests. `grep -n 'WithAutoResetOffsetOutOfRange' kafka_consumer-offset.go` and `grep -n 'IsOffsetOutOfRange' kafka_consumer-offset.go` each return ≥1 match.

**Rung 2 — runtime offset-advance check (deferred, explicitly owned, completion-gated):** this library ships as a tag; the runtime proof runs in the trading follow-up after the consumer bump + deploy. Owning task: **Self-Heal Kafka Consumer on OffsetOutOfRange Instead of Erroring** (vault task `97caec10-2917-480c-afe0-74f44b6b6f79`, category trading), deploy subtasks ("Deploy to dev, verify with a real offset-out-of-range occurrence or a forced reproduction — confirm the consumer resumes and offset/lag advances past the stuck point"). The operator check: force an out-of-range condition (prune retention or reset the consumer group offset past retention), then confirm `kafka-consumer-groups --describe --group <group> --topic <topic>` shows the consumer's current offset/lag advancing past the previously-stuck offset within 15 min. **Completion semantics: this spec does NOT reach `completed` on Rung 1 alone.** Per bug-workflow's runtime-symptom rule, this spec stays `verifying` until Rung 2 passes in the owning task; the owning task's completion gate (`kafka-consumer-groups` offset-advance confirmed) is the condition that releases this spec to `completed`. This rung is NOT satisfiable inside the library spec's `verify-spec` (no broker, no deployed consumer) and is declared deferred with a named check + named owning task rather than claimed here.

## Desired Behavior

1. A new functional option, `WithAutoResetOffsetOutOfRange(bool)`, is available on the offset consumer, following the existing `ConsumerOptions` / `func(*ConsumerOptions)` option pattern. Its default (when not supplied) is `false` (OFF).
2. With the option OFF (default): when the consume loop receives an error where `IsOffsetOutOfRange(err)` is true, the error propagates through `errorHandler.HandleError` and the consumer fails — identical to current behavior, no observable change.
3. With the option ON: when the consume loop receives an error where `IsOffsetOutOfRange(err)` is true, the consumer does NOT propagate the error. Instead it:
   a. logs at warn level: topic, partition, the old (stuck) offset, and the fallback offset (the offset being reset to);
   b. resets the stored offset backward via `OffsetManager.ResetOffset(ctx, topic, partition, fallbackOffset)` so a restart does not recreate the stuck condition;
   c. closes the current partition consumer and recreates it at `offsetManager.FallbackOffset()` — the same fallback value `CreatePartitionConsumer` already uses on creation-time out-of-range — then continues consuming.
4. The reset is observable: the partition consumer is recreated at the fallback offset (below the stuck offset), and the read position advances past the stuck offset as messages flow.
5. An `IsOffsetOutOfRange(err error) bool` helper exists in the kafka package for reusable offset-error detection, matching the error by type (`sarama.ErrOffsetOutOfRange` / `sarama.KError`) with the string-match fallback pinned to `OutOfRangeErrorMessage` (the existing constant in `kafka_consumer-partition.go`) for backwards compatibility — mirroring the draft on branch `improve-kafka-offset-out-of-range-error` (commit cdef9053).

## Assumptions

- `offsetManager.FallbackOffset()` returns a configured fallback offset constant (typically the `sarama.OffsetOldest` sentinel −2, per `kafka_offset-manager-simple.go`), and sarama resolves that sentinel to the actual oldest available offset when the partition consumer is created at it. The logged "new offset" is therefore the fallback value (possibly the −2 sentinel), NOT a resolved absolute offset — the log records what the consumer was reset to, which is the fallback.
- The in-package fake pattern (`fakePartitionConsumer`, `newOffsetConsumerForTest`) used by the corruption-skip feature can inject an `OffsetOutOfRange` consumer error and verify the reset + resume.
- `ResetOffset` already exists on the `OffsetManager` interface as the backward-movement counterpart to `MarkOffset`.
- Consumers that opt in accept that data between the retained oldest offset and the stuck offset is re-read (replayed) rather than skipped — the opt-in is the operator's explicit acceptance of the replay, which is the point of resetting to oldest.

## Constraints

- **Frozen anti-pattern (MUST NOT):** Do NOT implement the reset by having the consumer error handler (`NewConsumerErrorHandler`) return nil to swallow the `ErrOffsetOutOfRange`. The offset is only advanced via `MarkOffset` after a successful message, so swallowing the error leaves the partition consumer stuck at the same invalid offset forever: a silent stall that looks healthy, does zero work, and never alerts. The reset MUST instead recreate the partition consumer at the fallback offset so the read position advances.
- Default behavior is frozen: with the option OFF, the error-propagate-and-fail path must be unchanged.
- The new option must follow the existing functional-option style in `kafka_consumer-offset.go`; existing options and their defaults must not change.
- Only the `OffsetOutOfRange` error class is intercepted. All other consume errors (rebalance timeouts, auth failures, connection errors) still reach `errorHandler.HandleError` and propagate — the error-class filter must be narrow.
- Tests must use the existing in-package fake pattern with plain `testing.T`, NOT Ginkgo (one-`RunSpecs`-per-binary constraint).
- `IsOffsetOutOfRange` must live in the kafka package (home file: `kafka_consumer-offset.go`, next to `IsCorruptionError`), not the trading repo.
- The reset must be idempotent across restarts: after `ResetOffset`, a restart reads the fallback offset and recreates the same valid consumer — no partial-state trap.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility | Concurrency |
|---------|-------------------|----------|-----------|---------------|-------------|
| `ErrOffsetOutOfRange` with option OFF | Error propagates; consumer run fails; error reaches error handler / Sentry upstream | Operator investigates (today's flow) | Propagated error in logs / Sentry | N/A (no state advanced) | Single partition consumer; fail is clean |
| `ErrOffsetOutOfRange` with option ON | Reset-and-resume: log warn with old/fallback offset, `ResetOffset` backward, recreate consumer at fallback offset | Automatic — consumer resumes | Warn log line with old+fallback offset; offset/lag advances past stuck point | Irreversible only in the sense that the reset point is not re-chosen; retained data between oldest and stuck offset IS replayed | Per-partition consumer recreated; only that partition is affected |
| Unrelated consume error (rebalance, auth, connection) with option ON | NOT intercepted — propagates via error handler, consumer fails as today | Operator/retry per existing handling | Propagated error in logs / Sentry | N/A | No interception; error path unchanged |
| Broker/partition unavailable while recreating consumer at fallback offset | Recreate surfaces the underlying connection error (not an offset error); error propagates | Operator/retry per existing consumer error handling | Propagated non-offset error in logs | N/A | Recreate failure does not silently advance offset |
| `ResetOffset` fails (e.g. offset manager closed) | Reset path errors; propagates | Operator | Propagated error | N/A | No partial offset commit |
| Mid-reset crash (process dies after detection, before recreate) | On restart, reads the offset the manager holds; if `ResetOffset` already ran, resumes at fallback; if not, re-detects and re-resets | Automatic on restart | Warn log re-emitted on restart | Idempotent — re-detection produces same reset | No torn state |

## Suggested Decomposition

Single-layer, single-behavior change in one package — a single prompt covers all Desired Behaviors and Acceptance Criteria.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Implement `WithAutoResetOffsetOutOfRange` option, `IsOffsetOutOfRange` helper, reset-in-consume-loop path, and in-package tests | 1-5 | 1-8 | — |

## Do-Nothing Option

If we do nothing, every consumer keeps failing on `OffsetOutOfRange`, the affected partition silently stops being consumed, and the error is reported to Sentry as noise. The error-and-fail behavior is safe (no data corruption), but it turns a routine retention condition into a production incident and — because the partition stalls — causes silent data loss on tick ingestion. Doing nothing is acceptable only while no consumer needs self-healing; this spec makes the opt-in available without changing the safe default.
