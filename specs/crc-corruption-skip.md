---
tags:
  - dark-factory
  - spec
status: draft
---

## Summary

- Add an opt-in mode to the shared `bborbe/kafka` offset consumer that lets it skip past a corrupt Kafka record batch instead of crashing.
- The mode is OFF by default: every consumer behaves exactly as today (crash on CRC corruption) unless its owner explicitly turns the mode on.
- When ON, the consumer logs the skipped offset range, increments a new corruption metric, and resumes consuming at the first healthy offset past the corruption.
- The skip MUST advance the read position by recreating the partition consumer past the corrupt batch — it MUST NOT swallow the decode error, which would silently re-read the same batch forever.
- Scope is the `bborbe/kafka` library only; the trading-side flag that enables the mode is a separate follow-up after this library is tagged.

## Problem

When the `bborbe/kafka` offset consumer encounters a record batch whose CRC fails to decode, sarama surfaces a `PacketDecodingError` and the consumer crashes. For high-value topics this is the desired behavior: a human investigates corruption rather than letting the system quietly discard data. But for some topics the operator would rather lose the corrupt batch and keep consuming than have the consumer stay down. Today there is no way to choose: corruption always means a crash. The naive fix — swallowing the decode error in the consumer error handler — is worse than the crash, because the offset is only advanced after a *successful* message, so swallowing the error makes the consumer re-read the same corrupt batch forever: it looks healthy, does zero work, and never alerts.

## Goal

The `bborbe/kafka` offset consumer can be configured, per consumer instance, to skip past a corrupt record batch and continue consuming at the next healthy offset. Consumers that do not opt in keep crashing on corruption, byte-for-byte identical to today. When the skip path runs, it advances the read position (so the corrupt batch is never re-read), records the skipped offset range in the log, and increments a Prometheus metric so the skip is observable.

## Non-goals

- Do NOT change default behavior. Without the opt-in, corruption crashes the consumer exactly as today.
- Do NOT improve consumer throughput or lag. Lag is a separate speed concern and out of scope here.
- Do NOT wire any trading-repo flag (e.g. a `SKIP_CRC_CORRUPTION` main.go argument in core-tick-command-handler). That is a separate follow-up task in the trading repo, done after this library is tagged.
- Do NOT add a `github.com/bborbe/kafka/metric` subpackage — it does not exist; metrics live in the root kafka package (kafka_metrics.go). If a future consumer wants a separate metrics package, that is a separate spec.
- Do NOT make the skip behavior tunable beyond a single on/off option (no configurable jump sizes, probe limits, or thresholds) — invariant; if a future consumer demands variation, that is a separate spec.
- Do NOT implement skip by returning nil from the consumer error handler — invariant anti-pattern (see Constraints).

## Desired Behavior

1. A new functional option, `WithSkipCorruptBatches(bool)`, is available on the offset consumer, following the existing `ConsumerOptions` / `func(*ConsumerOptions)` option pattern. Its default (when the option is not supplied) is `false` (OFF).
2. With the option OFF (default): when the consume loop hits a `sarama.PacketDecodingError`, the consumer propagates the error and crashes — identical to current behavior, no observable change.
3. With the option ON: when the consume loop hits a `sarama.PacketDecodingError`, the consumer does NOT propagate the error. Instead it:
   a. logs the corrupt offset and the resolved skipped offset range (corrupt-start offset → first-healthy offset);
   b. increments the Prometheus metric `kafka_consumer_corrupt_batch_skipped_total` (namespace `kafka`, subsystem `consumer`, name `corrupt_batch_skipped_total`, labels `topic` and `partition`);
   c. closes the current partition consumer and recreates it at the first healthy offset past the corrupt batch, then continues consuming.
4. The "first healthy offset past the corrupt batch" is found by probing forward from the corrupt offset (exponential jumps) and then binary-searching for the exact end of the corruption, where an offset is judged healthy by opening a short-lived partition consumer at the candidate offset and confirming a readable message rather than a CRC decode error. The behavior is ported from the proven trading scanner logic (`FindNextHealthyOffset` / `isOffsetGood`) and adapted to the local kafka package types (`Topic`, `Partition`, `Offset`).
5. The skip path always advances the read position past the corrupt batch before resuming; the corrupt offset range is never re-read by the same consumer.

## Assumptions

- The trading scanner's probe-forward + binary-search logic (`FindNextHealthyOffset` / `isOffsetGood` in `~/Documents/workspaces/trading/strimzi/topic-backuper/pkg/corruption-skipper.go`) is proven in production and is a sound algorithm to port.
- Metrics live in the root `kafka` package (`kafka_metrics.go`); there is no `metric` subpackage.
- Skipped (corrupt) records are acceptable to lose for any consumer that opts in — the opt-in is the operator's explicit acceptance of that trade-off.

## Constraints

- **Frozen anti-pattern (MUST NOT):** Do NOT implement the skip by having the consumer error handler (`NewConsumerErrorHandler`) return nil to swallow the `PacketDecodingError`. In the offset consumer the offset is only advanced via `MarkOffset` after a *successful* message, so on the error path the offset never advances. Swallowing the error causes the consumer to re-read the same corrupt batch indefinitely: a silent infinite spin-loop that appears healthy, does zero work, and never alerts. The skip MUST instead advance the offset by recreating the partition consumer past the corrupt batch.
- Default behavior is frozen: with the option OFF, the crash-on-corruption path must be unchanged.
- The new metric must be added to the consumer Metrics interface, implemented in the `metrics` impl, and registered in the `init()` in kafka_metrics.go, following the existing GaugeVec/CounterVec metric pattern exactly (namespace `kafka`, subsystem `consumer`).
- The new option must follow the existing functional-option style in kafka_consumer-offset.go; existing options and their defaults must not change.
- Tests must use the existing in-package fake pattern (`fakePartitionConsumer`, `newOffsetConsumerForTest`) with plain `testing.T`, NOT Ginkgo, to respect the one-`RunSpecs`-per-binary constraint already noted in that test file's history.
- Ported algorithm must reference the local kafka package types (`Topic`, `Partition`, `Offset`), not the trading repo's libkafka types.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility | Concurrency |
|---------|-------------------|----------|-----------|---------------|-------------|
| `PacketDecodingError` with skip OFF | Error propagates; consumer crashes | Operator investigates corruption manually (today's flow) | Process exit / propagated error in logs | N/A (no state advanced) | Single partition consumer; crash is clean |
| `PacketDecodingError` with skip ON | Skip-and-advance: log range, increment metric, recreate consumer at first healthy offset | Automatic — consumer resumes | Metric `corrupt_batch_skipped_total` increments; skipped-range log line | Irreversible — skipped records are not re-read | Per-partition consumer recreated; only that partition is affected |
| Probe/binary-search opens short-lived consumer that itself hits CRC error at candidate | Candidate offset treated as still-corrupt; probe continues forward | Automatic — search advances past it | Search continues (no premature resume) | N/A | Short-lived probe consumers are independent of the main consumer |
| Corruption extends to (or past) the high-water mark / end of partition | Search resolves first healthy offset to the partition end; consumer resumes at the tail and waits for new messages | Automatic — resumes at tail | Skipped-range log shows range ending at partition end | Irreversible (corrupt tail skipped) | Single partition consumer |
| Broker/partition unavailable while probing for healthy offset | Probe surfaces the underlying connection error (not a CRC skip); error propagates | Operator/retry per existing consumer error handling | Propagated non-CRC error in logs | N/A | Probe failure does not silently advance offset |
| Mid-skip crash (process dies after detecting corruption, before resuming) | On restart, consumer re-reads from last committed offset; if skip still ON, it re-detects and re-skips the corruption | Automatic on restart | Metric increments again on restart | Idempotent — re-detection produces same skip | No partial offset commit during skip |

## Security / Abuse Cases

- The corrupt batch content is attacker-or-disk-controlled bytes that fail CRC. The skip path must not parse/trust the corrupt bytes beyond detecting the decode failure; it only uses offsets to advance.
- The forward-probe must not loop forever: it is bounded by the partition's high-water mark (end of partition), so a fully-corrupt tail resolves to the partition end rather than spinning.
- The skip never re-reads the same corrupt batch (the explicit guard against the spin-loop anti-pattern), so a single corrupt batch cannot cause unbounded CPU or unbounded metric increments within one consume pass.

## Acceptance Criteria

- [ ] A `WithSkipCorruptBatches(bool)` functional option exists on the offset consumer and defaults to OFF when not supplied — evidence: source contains the option following the existing `func(*ConsumerOptions)` pattern; `grep -n 'WithSkipCorruptBatches' kafka_consumer-offset.go` returns line ≥1.
- [ ] With skip OFF, a `sarama.PacketDecodingError` in the consume loop is returned as an error — evidence: in-package unit test asserts `Consume` returns a non-nil error wrapping/equal to the decode error; `go test` for that test passes (exit 0).
- [ ] With skip ON, the consumer advances past the corrupt offset and resumes consuming good messages — evidence: in-package unit test using `fakePartitionConsumer` injects a `PacketDecodingError` at a corrupt offset followed by good messages, and asserts (i) `Consume` does not return the decode error, (ii) the good messages after the corrupt batch are delivered/processed, (iii) the consumer was recreated at an offset past the corrupt one; `go test` passes (exit 0).
- [ ] The metric `kafka_consumer_corrupt_batch_skipped_total` is defined with namespace `kafka`, subsystem `consumer`, name `corrupt_batch_skipped_total`, labels `topic` and `partition`, added to the Metrics interface, implemented in the metrics impl, and registered in `init()` — evidence: `grep -n 'corrupt_batch_skipped_total' kafka_metrics.go` returns the metric definition and registration lines (≥2 matches); the metric exposes `topic` and `partition` labels.
- [ ] On a skip, the metric increments by exactly one per corrupt batch skipped within a single consume pass (re-detection after a process restart may increment again — see Failure Modes) — evidence: unit test (or test using a fake/registry metrics impl) asserts the counter for the topic/partition increments by 1 after one skip.
- [ ] On a skip, a log line records the corrupt offset and the resolved skipped range (corrupt-start → first-healthy) — evidence: unit test captures the logger output and asserts it contains both the corrupt offset and the resolved healthy offset.
- [ ] The consumer error handler is NOT modified to return nil for `PacketDecodingError` — evidence: `grep -n 'return nil' kafka_consumer-error-handler.go` returns 0 lines (the handler still propagates via `return err`), AND a unit test asserts `NewConsumerErrorHandler(...).HandleError(<PacketDecodingError>)` returns a non-nil error; the skip logic lives in the consume loop / partition-consumer recreation path, not the error handler.
- [ ] The first-healthy-offset search is implemented in the kafka package using local `Topic`/`Partition`/`Offset` types (ported from the trading scanner's probe-forward + binary-search) — evidence: source contains the probe-forward + binary-search function referencing local kafka types; `grep -n` finds the search function and shows no import of the trading libkafka package.
- [ ] `make precommit` exits 0 in the kafka package — evidence: exit code 0.

Scenario coverage: NO new scenario. The skip-and-advance behavior is reachable with the existing in-package `fakePartitionConsumer` (no real broker, no Docker, no cluster needed), so unit tests fully cover it.

## Verification

```
make precommit
```

Expected: exit code 0. The new in-package unit tests (skip OFF propagates error; skip ON advances and resumes; metric increments; log records range) pass. `grep -n 'WithSkipCorruptBatches' kafka_consumer-offset.go` and `grep -n 'corrupt_batch_skipped_total' kafka_metrics.go` each return ≥1 match.

## Do-Nothing Option

If we do nothing, every consumer keeps crashing on CRC corruption, and any topic whose owner would rather skip-and-continue has no option but a manual restart loop or an unsafe local hack (e.g. swallowing the error, which causes the silent spin-loop described above). The current crash-by-default behavior is acceptable and intentional for important topics, so doing nothing is safe — but it blocks the opt-in skip needed for topics where availability matters more than a single corrupt batch. Doing nothing is acceptable only until a consumer concretely needs the skip; this spec makes that opt-in available without changing the safe default.
