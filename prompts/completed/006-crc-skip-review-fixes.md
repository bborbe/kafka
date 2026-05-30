---
status: completed
summary: 'Fixed CRC skip PR #13 review findings: binary search confirmation, close-order in skipAndAdvance, consumeMessages message retention, log levels, probe timeout constant, and added tests'
container: kafka-crc-skip-exec-006-crc-skip-review-fixes
dark-factory-version: v0.173.0
created: "2026-05-30T20:25:53Z"
queued: "2026-05-30T20:25:53Z"
started: "2026-05-30T20:26:16Z"
completed: "2026-05-30T20:42:32Z"
---

# Fix CRC Skip — PR #13 Review Findings

## Goal

Address all fixable findings from the pr-reviewer bot's CHANGES_REQUESTED review on PR #13 (the CRC-skip feature). Two critical correctness bugs, five should-fix items, two nits. Every finding is accepted (all "fix it"). Read `kafka_consumer-offset.go` (`skipAndAdvance` ~lines 485-557, `binarySearchEndOfCorruption` ~105-126, `isOffsetGood` ~128-165, `consumeMessages` ~417-471, the `Consume` loop skip branch ~343-360) and `kafka_consumer-offset_internal_test.go` in full before changing anything.

## Requirements

### 1. CRITICAL — binary search must confirm the returned offset is good
`binarySearchEndOfCorruption` (line ~105) returns `low` without ever confirming `low` itself reads cleanly. The exponential probe proves *some* offset ≥ corruptOffset+jump is good, and the search narrows toward it, but the returned `low` can land on an offset that was never directly probed good (e.g. corruption 100, probe 110 good → can return 101 unverified). If 101 is still corrupt, the consumer recreates there and re-hits corruption.

Fix: after the loop, confirm the result with a final `isOffsetGood(ctx, ...)` on `low`. If `low` is NOT good (or the probe errors), the search did not actually find a clean offset — return a sentinel/`-1`-style "not found" so the caller falls back (the `skipAndAdvance` `newOffset < 0` → high-water-mark path). Keep the existing `err` semantics: a genuine broker/probe error must still propagate, not be treated as "not found". Adjust `binarySearchEndOfCorruption`'s signature to return `(int64, error)` if needed so the confirmation error can propagate, and thread that through `FindNextHealthyOffset`.

### 2. CRITICAL — do not close the old partition consumer until the new one is created
`skipAndAdvance` (line ~513) calls `oldPartitionConsumer.Close()` BEFORE `CreatePartitionConsumer` (line ~529). If `CreatePartitionConsumer` fails, it returns `(nil, Offset(0), err)`; the `Consume` caller sets `consumePartition = nil` and the next `consumeMessages(ctx, nil)` panics on a nil partition consumer.

Fix: create the new partition consumer FIRST; on success, close the old one and return the new one; on `CreatePartitionConsumer` failure return `(nil, 0, err)` WITHOUT closing the old consumer. The `Consume` loop already returns on `skipErr != nil` before assigning `consumePartition` (verify lines ~346-358: `newPC, newOff, skipErr := ...`; `if skipErr != nil { return ... }`; then `consumePartition = newPC`), so on failure no nil consumer is ever used. Invariant: **after a failed skip, the loop never calls `consumeMessages` with a nil consumer, and the old consumer is not closed/leaked.**

### 3. consumeMessages must not drop already-enqueued messages on corruption
When a corruption error arrives on `Errors()`, `consumeMessages` (lines ~430, ~455) returns `errSkipCorruptBatch` immediately, discarding any messages already accumulated in `result` (or deliverable) from this call. Those good messages before the corrupt batch are silently lost.

Fix: when skip is ON and a corruption error is detected, if `result` already holds messages, return them normally first (so they're processed + offset-marked), and let the NEXT `consumeMessages` call surface the corruption sentinel. I.e. only return `errSkipCorruptBatch` when `result` is empty; otherwise return the accumulated `result` with nil error. (Check both the outer select ~430 and the inner loop ~455.)

### 4. Test skipAndAdvance error/fallback paths
Add in-package tests (plain `testing.T`, fake pattern) for the three uncovered branches:
  - (a) `skipper.FindNextHealthyOffset` returns an error → `skipAndAdvance` returns that error, old consumer handled per requirement 2, no nil-consumer leak.
  - (b) `CreatePartitionConsumer` fails at the healthy offset → returns error, no panic, old consumer not left dangling.
  - (c) `newOffset < 0` (corruption to end) → recreates at high-water mark; assert the new consumer is created at the HWM offset.
Use a fake `sarama.Consumer` whose `ConsumePartition` can be scripted to fail on demand.

### 5. Test the real defaultCorruptionSkipper algorithm
`FindNextHealthyOffset`, `binarySearchEndOfCorruption`, `isOffsetGood` currently have ZERO direct tests (only `fakeSkipper` is exercised). Add in-package tests driving the real `defaultCorruptionSkipper` with a fake `sarama.Consumer`/`PartitionConsumer` that simulates corruption at a known offset range:
  - exponential probe finds a good offset past the range
  - binary search converges to the first good offset (and, per requirement 1, confirms it)
  - corruption-to-end → returns not-found/`-1`
  - `isOffsetGood`: good message → true; corruption error → false; timeout → false; non-corruption error → propagates

### 6. Use V(1).Infof for expected skip logs, not Warningf
The skip log lines in `skipAndAdvance` (the "detected corrupt batch", "skipped corrupt batch", "corrupt range extends to end" messages at lines ~494, ~520, ~549) use `glog.Warningf`. Corruption skip is expected opt-in behavior — change these to `glog.V(1).Infof` so operators who enable the feature aren't alarmed by normal skip events. (Keep genuine error logs as-is.)

### 7. newOffsetConsumerForTest must set a non-nil metrics field
`newOffsetConsumerForTest` (line ~438) initializes `errorHandler` with `NewMetrics()` but leaves `metrics` nil. Any test reaching `skipAndAdvance` → `c.metrics.CorruptBatchSkippedCounterInc` panics. Set `metrics: NewMetrics()` (or a counting fake) in the factory.

### 8. Nit — named constant for probe timeout
Replace the hardcoded `time.NewTimer(5 * time.Second)` (line ~144) with a package-level `const probeTimeout = 5 * time.Second` (or a named var), used by `isOffsetGood`.

### 9. Nit — copyright year consistency
`kafka_consumer-offset_internal_test.go` line 1 uses `2026`. Change it to **`2023`** to match its sibling `kafka_consumer-offset.go` header. (If `make precommit`'s `addlicense` normalizes the year, accept whatever it produces.)

## Constraints

- Default behavior (skip OFF) unchanged — crash on `PacketDecodingError`. Error handler untouched (`grep -n 'return nil' kafka_consumer-error-handler.go` == 0).
- No `context.Background()` in `kafka_consumer-offset.go`.
- No `fmt.Errorf` (use `github.com/bborbe/errors` `Wrapf`, or `stderrors.New` for sentinels).
- Tests in-package `testing.T` + fakes, NOT Ginkgo.
- Skip must advance by recreating the partition consumer — never swallow-and-reread, never return a nil consumer the caller will use.

## Acceptance Criteria

- [ ] `binarySearchEndOfCorruption` confirms the returned offset with a final `isOffsetGood`; an unverifiable result yields not-found (caller falls back to HWM), and genuine probe errors propagate.
- [ ] `skipAndAdvance` creates the new partition consumer BEFORE closing the old one; on create failure the old consumer is not leaked and the caller never uses a nil consumer (no nil-deref panic).
- [ ] `consumeMessages` returns already-accumulated good messages before surfacing `errSkipCorruptBatch` (no silent message loss); empty-result case still returns the sentinel.
- [ ] Tests cover skipAndAdvance paths (a) FindNextHealthyOffset error, (b) CreatePartitionConsumer failure, (c) newOffset<0 → HWM.
- [ ] Tests cover the real `defaultCorruptionSkipper` (probe + binary search + isOffsetGood branches).
- [ ] Skip log lines use `glog.V(1).Infof`, not `Warningf`.
- [ ] `newOffsetConsumerForTest` sets a non-nil `metrics`.
- [ ] Probe timeout is a named constant.
- [ ] Copyright year consistent with sibling files.
- [ ] `make precommit` exits 0.

## Validation

```
make precommit
```
Expected: exit 0; all new and existing tests pass; `grep -n 'context.Background()' kafka_consumer-offset.go` == 0; `grep -n 'fmt.Errorf' kafka_consumer-offset.go` == 0; `grep -n 'glog.Warningf' kafka_consumer-offset.go` shows only genuine-error logs (skip-path logs are V(1).Infof).

## Out of Scope

- The optional compaction-gap branch in `FindNextHealthyOffset` (intentionally omitted).
- Throughput/lag; trading-side `SKIP_CRC_CORRUPTION` arg.
