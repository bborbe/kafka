---
status: approved
created: "2026-05-30T11:11:48Z"
queued: "2026-05-30T11:11:48Z"
---

# Opt-in CRC Corruption Skip in Offset Consumer

## Goal

Add an opt-in mode to the `bborbe/kafka` offset consumer that lets it skip past a corrupt Kafka record batch (CRC decode failure) and resume consuming at the next healthy offset, instead of crashing. The mode is OFF by default — every existing consumer keeps crashing on corruption, byte-for-byte identical to today.

Derived from spec `specs/in-progress/001-crc-corruption-skip.md` (read it in full before starting).

## Context

When the offset consumer hits a record batch whose CRC fails to decode, sarama surfaces a `sarama.PacketDecodingError` on the partition consumer's `Errors()` channel. Today the error handler returns it, it propagates up `consumeMessages` → `Consume` → `service.Run` → `os.Exit(1)`, and the pod crashloops. For high-value topics this loud failure is desired (a human reviews corruption). For replaceable data (ticks, candles) the operator would rather skip the corrupt batch and keep consuming. This prompt makes that an opt-in choice.

### 🚨 CRITICAL anti-pattern — do NOT do this

Do **NOT** implement the skip by making `NewConsumerErrorHandler` (in `kafka_consumer-error-handler.go`) return `nil` to swallow the `PacketDecodingError`.

In the offset consumer the offset is only advanced via `MarkOffset` AFTER a *successful* message (`kafka_consumer-offset.go`, the `nextOffset := msg.Offset + 1; MarkOffset(...)` block around line 216). On the error path no offset is marked. If you swallow the error and `continue`, sarama re-delivers the SAME corrupt batch forever → a **silent infinite spin-loop**: the consumer looks healthy, does zero work, never advances, never alerts. That is strictly worse than the crash.

The skip MUST advance the read position by **closing and recreating the partition consumer at the first healthy offset past the corrupt batch**.

## Requirements

### 1. New functional option (default OFF)
Add `WithSkipCorruptBatches(bool)` following the existing functional-option pattern in `kafka_consumer-offset.go` (the `ConsumerOptions` struct + `func(*ConsumerOptions)` options, applied in `NewOffsetConsumerBatchWithProvider` around lines 96-117). Add a `SkipCorruptBatches bool` field to `ConsumerOptions` (zero value `false` = OFF). Thread it onto the `offsetConsumer` struct. Existing options and their defaults MUST NOT change.

### 2. OFF behavior unchanged (default)
With the option OFF: a `sarama.PacketDecodingError` in the consume loop propagates and the consumer crashes — identical to current behavior. No observable change. The error handler (`kafka_consumer-error-handler.go`) is NOT modified.

### 3. ON behavior — skip and advance
With the option ON: when the consume loop encounters a `sarama.PacketDecodingError` (detect via `errors.As` for `sarama.PacketDecodingError`; also treat a generic error whose message contains `"CRC"` as corruption — mirror the helper in requirement 5), the consumer must:
  a. log the corrupt offset AND the resolved skipped range (`corrupt-start → first-healthy`);
  b. increment a new Prometheus metric (requirement 4);
  c. close the current partition consumer and recreate it (via the existing `CreatePartitionConsumer`, `kafka_consumer-partition.go`) at the first healthy offset past the corrupt batch, then continue consuming.

**Where the skip lives — and the injectable seam (read carefully).** The skip-and-advance path MUST live at the `Consume()` for-loop level in `kafka_consumer-offset.go` (the `for { messages, err := c.consumeMessages(...) ... }` loop, lines 193-250), because only that level owns the partition-consumer lifecycle (`CreatePartitionConsumer` + `consumePartition.Close()`). Do NOT push the skip into `consumeMessages` and do NOT swallow in the error handler — both lack the lifecycle and recreate the spin-loop.

Today `Consume()` builds its partition consumer from a real `sarama.NewConsumerFromClient(saramaClient)` (line ~142) and the existing in-package fake (`fakePartitionConsumer`) only reaches the unexported `consumeMessages`, never `Consume()` or recreation. So a test cannot currently observe "recreated at an offset past the corrupt one" without a real broker. **You MUST introduce a test seam:** extract the skip-and-advance step (detect corruption → find first-healthy-offset → close → recreate partition consumer at that offset) into an injectable dependency on `offsetConsumer` — e.g. a `corruptionSkipper` interface field (with `FindNextHealthyOffset(...) Offset`) and/or a partition-consumer factory func — defaulted in the constructor to the real implementation, overridable in tests. The skip-ON unit test drives recreation through a fake/in-memory partition consumer + a fake skipper, with no real broker. Keep the default (constructor-wired) behavior production-correct.

`consumeMessages` currently wraps the handler error as `"parition consumer returns error"`; when skip is ON and the underlying error is a decode/CRC error, the `Consume()` loop must detect it (unwrap and test with `IsCorruptionError`) and invoke the skip-and-advance seam instead of returning.

### 4. New metric
Add `kafka_consumer_corrupt_batch_skipped_total` to `kafka_metrics.go`, following the existing pattern EXACTLY (the other consumer metrics are `prometheus.NewGaugeVec` with `.Inc()`):
  - namespace `kafka`, subsystem `consumer`, name `corrupt_batch_skipped_total`, help text describing skipped corrupt batches, labels `["topic","partition"]`
  - add a method to the `MetricsConsumer` interface (e.g. `CorruptBatchSkippedCounterInc(topic Topic, partition Partition)`)
  - implement it on the `metrics` struct
  - register the GaugeVec in the `init()` `prometheus.MustRegister(...)` block (keep the alphabetical-ish ordering already present)
  - the metric is incremented once per corrupt batch skipped (within a single consume pass)

### 5. First-healthy-offset search (algorithm inlined below)
Implement a first-healthy-offset search in the `kafka` package using LOCAL types `Topic` (`kafka_topic.go`), `Partition` (`kafka_partition.go`), `Offset` (`kafka_offset.go`). Do NOT import the trading `libkafka` package.

This algorithm is ported from a proven production implementation (the trading scanner). The full algorithm is specified here — you do NOT need to (and cannot, inside the container) read any external file:

**`FindNextHealthyOffset(ctx, consumer sarama.Consumer, topic, partition, corruptOffset Offset, maxOffset int64) (int64, error)`** — returns the first good offset after the corrupt range, or `-1` if corruption extends to the end of the partition. Returns a non-nil error only if a probe hits a genuine broker/partition failure (non-corruption), which the caller must propagate (not silently advance):
  1. Exponential probe: for `jump` in `[10, 100, 1000, 10000, 100000]`, test `candidate = corruptOffset + jump`. If `candidate >= maxOffset`, corruption runs to the end → return `-1`. If `isOffsetGood(candidate)` is true, corruption ends before `candidate` → binary-search between `corruptOffset` and `candidate` for the exact first-good offset and return it.
  2. (OPTIONAL refinement — you MAY skip this branch.) If all jumps still corrupt, probe near the end (`maxOffset - 10000`) to distinguish a long corrupt run from a massive compaction gap; if that reads cleanly, search forward a bounded window (e.g. 1000 offsets) from `corruptOffset` for the actual end, else jump to near-end. This handles massive compaction gaps; the exponential+binary-search path in step 1 is the primary case and is sufficient for the tick/candle topics this targets. If you omit this branch, step 1 returning `-1` (corruption-to-end) is the acceptable fallback — do NOT block on implementing it.

`maxOffset` is the partition high-water mark — the caller passes `consumePartition.HighWaterMarkOffset()` (available in the `Consume()` loop at line ~202).
  3. If nothing reads, return `-1` (corruption to end).

**`binarySearchEndOfCorruption(corruptOffset, goodOffset)`** — standard binary search: while `corruptOffset+1 < goodOffset`, test `mid`; if good, `goodOffset = mid`, else `corruptOffset = mid`. Return `goodOffset` (first good).

**`isOffsetGood(ctx, consumer, topic, partition, offset) (bool, error)`** — open a short-lived partition consumer at `offset` via `consumer.ConsumePartition(...)`; `defer pc.Close()`. Then `select` on:
  - `ctx.Done()` → return `ctx.Err()`;
  - a message on `pc.Messages()` → `(true, nil)` (a higher returned offset due to compaction is still "good");
  - an error on `pc.Errors()` → if `IsCorruptionError(err)` then `(false, nil)` (still corrupt → keep probing); **otherwise return `(false, err)` so a genuine broker/partition failure propagates** rather than being mistaken for corruption (per the spec Failure Mode "Broker/partition unavailable while probing → error propagates");
  - a **bounded read timeout** (~5s `time.NewTimer`) → `(false, nil)` (end of partition / massive gap).

The timeout / `ctx` cancellation MUST be honored so a probe never blocks forever. Propagate the non-corruption error up through `FindNextHealthyOffset` so the `Consume()` loop surfaces it (the skip path must NOT silently advance the offset on a broker error).

Bound the forward probe by the partition high-water mark (`maxOffset`) so a fully-corrupt tail resolves to the partition end rather than looping forever.

> Host-only reference for the human reviewer (NOT readable by the executing agent — do not depend on it): `~/Documents/workspaces/trading/strimzi/topic-backuper/pkg/corruption-skipper.go` and `corruption-detector.go`.

Also add a corruption-detection helper in the kafka package (the lib currently has none — `grep` confirms no `PacketDecodingError` reference exists yet). `errors.As` is the primary path; the string match is a deliberate fallback for wrapped/opaque errors:
```go
// IsCorruptionError reports whether err indicates Kafka message corruption (CRC mismatch / decode failure).
func IsCorruptionError(err error) bool {
    if err == nil { return false }
    var packetErr sarama.PacketDecodingError
    if errors.As(err, &packetErr) { return true }
    msg := err.Error()
    return strings.Contains(msg, "CRC") ||
        strings.Contains(msg, "CorruptRecord") ||
        strings.Contains(msg, "message contents does not match")
}
```

### 6. Tests
Add unit tests using the existing in-package fake pattern in `kafka_consumer-offset_internal_test.go` (`fakePartitionConsumer`, `newOffsetConsumerForTest`) — plain `testing.T`, NOT Ginkgo (respect the one-`RunSpecs`-per-binary constraint noted in that file's history). Prove:
  - skip OFF: a `PacketDecodingError` from the partition consumer propagates as an error (crash path);
  - skip ON: the consumer advances past the corrupt offset and delivers the good messages that follow, and the partition consumer is recreated at an offset past the corrupt one;
  - the metric increments by exactly one after one skip (use a fake/registry metrics impl, or assert via `testutil.ToFloat64` on the GaugeVec);
  - on a skip, a log line records both the corrupt offset and the resolved healthy offset;
  - `NewConsumerErrorHandler(...).HandleError(<PacketDecodingError>)` still returns a non-nil error (handler not weakened).

## Constraints

- Default behavior FROZEN: with the option OFF, the crash-on-corruption path is unchanged. The error handler is not modified to swallow `PacketDecodingError` (`grep -n 'return nil' kafka_consumer-error-handler.go` must return 0 lines).
- The skip MUST advance the offset by recreating the partition consumer past the corrupt batch — never swallow-and-continue (silent spin-loop).
- New metric follows the existing GaugeVec `.Inc()` pattern in `kafka_metrics.go` exactly (namespace `kafka`, subsystem `consumer`).
- Ported algorithm uses local `Topic`/`Partition`/`Offset` types; no import of the trading `libkafka` package.
- Tests use the existing in-package `fakePartitionConsumer` / `testing.T` pattern, not Ginkgo.
- No new tunability beyond the single on/off option (no configurable jump sizes / probe limits / thresholds).
- Scope is the **offset consumer** (`offsetConsumer` in `kafka_consumer-offset.go`) ONLY. Sibling consumers (`kafka_consumer-group-offset.go`, `kafka_consumer-simple.go`) are intentionally out of scope — do not modify them.
- The offset probe (`isOffsetGood`) must honor a bounded read timeout and `ctx` cancellation — never block forever.
- The skip-and-advance step must be behind an injectable seam (interface field / factory on `offsetConsumer`, defaulted in the constructor) so the skip-ON path is unit-testable without a real broker. Production default must be wired in the constructor and behave correctly.

## Acceptance Criteria

- [ ] `WithSkipCorruptBatches(bool)` functional option exists and defaults to OFF — `grep -n 'WithSkipCorruptBatches' kafka_consumer-offset.go` returns ≥1 line.
- [ ] Skip OFF: a `sarama.PacketDecodingError` in the consume loop propagates as an error — in-package unit test asserts the consume path returns non-nil at the `Consume()`-loop seam (the same seam the skip-ON test drives); `go test` exit 0.
- [ ] Skip ON: consumer advances past the corrupt offset and resumes consuming — unit test (driving the injectable seam from requirement 3 with a fake partition consumer + fake skipper, no real broker) injects a `PacketDecodingError` at a corrupt offset followed by good messages and asserts (i) no decode error returned, (ii) the following good messages are delivered/processed, (iii) the partition consumer is recreated at an offset past the corrupt one; `go test` exit 0.
- [ ] Metric `kafka_consumer_corrupt_batch_skipped_total` defined (namespace `kafka`, subsystem `consumer`, name `corrupt_batch_skipped_total`, labels `topic`,`partition`), added to the Metrics interface, implemented, and registered in `init()` — `grep -n 'corrupt_batch_skipped_total' kafka_metrics.go` returns ≥2 matches.
- [ ] On a skip the metric increments by exactly one per corrupt batch within a single consume pass (restart re-detection may increment again — see spec Failure Modes) — unit test asserts +1 after one skip.
- [ ] On a skip a log line records the corrupt offset and the resolved skipped range — unit test captures logger output and asserts both offsets present.
- [ ] Error handler NOT modified to return nil for `PacketDecodingError` — `grep -n 'return nil' kafka_consumer-error-handler.go` returns 0 lines AND a unit test asserts `NewConsumerErrorHandler(...).HandleError(<PacketDecodingError>)` returns non-nil.
- [ ] First-healthy-offset search implemented in the kafka package using local `Topic`/`Partition`/`Offset` (probe-forward + binary-search per requirement 5), behind an injectable seam on `offsetConsumer` — `grep -n` finds the search function; no import of the trading libkafka package; the offset probe honors a timeout/`ctx`.
- [ ] `make precommit` exits 0.

## Validation

```
make precommit
```
Expected: exit 0. New in-package unit tests pass. `grep -n 'WithSkipCorruptBatches' kafka_consumer-offset.go` and `grep -n 'corrupt_batch_skipped_total' kafka_metrics.go` each return ≥1 match; `grep -n 'return nil' kafka_consumer-error-handler.go` returns 0 lines.

## Out of Scope

- Throughput / consumer-lag optimization (separate concern; today's core-tick lag is a speed problem, not corruption).
- Trading-side wiring: the `SKIP_CRC_CORRUPTION` main.go arg in `core-tick-command-handler` is a SEPARATE follow-up task in the trading repo, done after this library is tagged.
- No `github.com/bborbe/kafka/metric` subpackage — metrics live in `kafka_metrics.go` in the root package.
