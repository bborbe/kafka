---
status: completed
summary: Closed partition consumer channels now return a recoverable sentinel error; recovery recreates consumer at last good offset with bounded consecutive attempts and a single WARN log line
execution_id: 16a0bd60-8d3a-50c8-8fdb-e3538828184d-exec-012-bug-partition-consumer-closed-sentinel
dark-factory-version: dev
created: "2026-10-20T18:36:24Z"
queued: "2026-10-20T18:52:36Z"
started: "2026-10-20T18:56:05Z"
completed: "2026-10-20T18:59:23Z"
branch: dark-factory/bug-partition-consumer-closed-sentinel
---

# Bug Fix: Partition Consumer Closed Sentinel

## Goal

A partition consumer whose channels close mid-run is replaced and the run continues from the last successfully consumed offset, so a routine rebalance or connection drop no longer fails the consumer or reaches Sentry as an error — for every consumer built from this library, with no call-site change.

Derived from spec `specs/in-progress/003-bug-partition-consumer-closed-sentinel.md` (read it in full before starting).

## Context

`NUKE-PROD-AJ` has fired 226 times since 2026-05-16 and 18 times in the last 30 days: `*errors.dataError: run failed: consume failed: partition consumer messages channel closed`. A single Kafka connection drop closes the partition consumer's `Errors()` and `Messages()` channels. `consumeMessages` reads the closed channel and returns a freshly formatted error, which `recoverFromConsumeError` does not recognise, so the partition run returns and `run.CancelOnFirstError` aborts every other partition in the same consumer. The outage is self-inflicted: the condition is a normal teardown event that the client already recovers from internally, and the consumer converts it into a whole-service failure.

Frozen symbols: `consumeMessages`, `recoverFromConsumeError`, `errPartitionConsumerClosed`.

## Constraints

- No new exported API; the change is internal to the consumer.
- Reuse the existing recreation primitive; do not build a second one.
- No disable switch — recovery is unconditional.
- Do NOT treat an offset-out-of-range or corruption error as this class.
- Do NOT swallow the error permanently.
- Do NOT add a metric. Exactly one warning log line is permitted on the new path.
- Do NOT change `run.CancelOnFirstError` or the per-partition run wiring.
- Do NOT touch the Kafka producer or the non-offset consumer.

## Implementation

### 1. Sentinel error `errPartitionConsumerClosed`
Package-level sentinel in `kafka_consumer-offset.go`:
```go
// errPartitionConsumerClosed is a sentinel error indicating the partition consumer's channels
// closed, which is normal sarama teardown (rebalance, connection drop, client close).
var errPartitionConsumerClosed = stderrors.New("partition consumer closed")
```

### 2. Four closed-channel branches in `consumeMessages`
Each closed `Errors()` or `Messages()` channel read returns `errPartitionConsumerClosed` instead of a formatted error:

- **Outer select, `Errors()` branch** (line ~552): `if !ok { return nil, errPartitionConsumerClosed }`
- **Outer select, `Messages()` branch** (line ~571): `if !ok { return nil, errPartitionConsumerClosed }`
- **Inner select, `Errors()` branch** (line ~586): `if !ok { return nil, errPartitionConsumerClosed }`
- **Inner select, `Messages()` branch** (line ~605): `if !ok { return nil, errPartitionConsumerClosed }`

### 3. Bounded consecutive recovery constant
```go
// maxConsecutivePartitionConsumerRecoveries bounds how many times a partition consumer is
// recreated after a closed channel without a successful batch in between.
const maxConsecutivePartitionConsumerRecoveries = 3
```

### 4. Recovery counter in `Consume` partition run loop
- Local counter `consecutiveClosedRecoveries := 0` initialised at the start of each partition run (per-partition, not shared).
- On `errors.Is(err, errPartitionConsumerClosed)` in the consume loop: `consecutiveClosedRecoveries++`.
- On successful batch: `consecutiveClosedRecoveries = 0`.
- Counter passed to `recoverFromConsumeError` as the `attempt` argument.

### 5. Recovery branch in `recoverFromConsumeError`
```go
if errors.Is(err, errPartitionConsumerClosed) {
    if client.Closed() || attempt > maxConsecutivePartitionConsumerRecoveries {
        return nil, Offset(0), false, nil
    }
    glog.Warningf(
        "partition consumer closed in topic(%s) partition(%d) at offset %s, recreating consumer (attempt %d)",
        c.topic,
        partition,
        nextOffset,
        attempt,
    )
    // ... close old, create new at nextOffset, return handled=true
}
```

The `client.Closed()` guard short-circuits recovery when the client is permanently closed — no recreation attempt. `CreatePartitionConsumer` errors propagate immediately (not counted against the consecutive recovery limit).

### 6. One WARN log line on recovery
Exactly one `glog.Warningf` line naming the topic, partition, offset, and attempt number. No other log changes.

## Tests

All tests live in `kafka_consumer-offset_internal_test.go` using the existing in-package fake pattern (`fakePartitionConsumer`, `testSaramaClient`, `testSaramaConsumer`, `fakeOffsetManager`, `fakeMessageHandlerBatch`).

### Four sentinel-return tests (AC2)
Each asserts `errors.Is(err, errPartitionConsumerClosed)` from `consumeMessages` for its branch:
- `TestConsumeMessages_OuterErrorsChannelClosed` — closed `Errors()` in outer select
- `TestConsumeMessages_OuterMessagesChannelClosed` — closed `Messages()` in outer select
- `TestConsumeMessages_InnerErrorsChannelClosed` — closed `Errors()` in inner select with message in buffer
- `TestConsumeMessages_InnerMessagesChannelClosed` — closed `Messages()` in inner select with message in buffer

### Full recovery integration test (AC1)
`TestConsume_PartitionConsumerClosed_RecoversAndContinues`: drives a partition whose first consumer has a closed `Errors()` channel. Asserts:
- `errors.Is(err, errPartitionConsumerClosed)` on the first call
- Exactly one recreation (`calls == 2`)
- Recreation at the recorded `nextOffset`
- The dead partition consumer is closed before recreation
- The replacement delivers its message to the batch handler
- `Consume` ends with `context.Canceled` (not an error)

### Warn log test (AC3)
`TestRecoverFromConsumeError_PartitionConsumerClosed_RecreatesAtNextOffsetAndLogsWarn`: drives `recoverFromConsumeError` directly with `errPartitionConsumerClosed` and `attempt=2`. Asserts exactly one WARN line containing the topic, offset, and `attempt 2`.

### Client-closed guard test (AC4)
`TestConsume_PartitionConsumerClosed_ClientClosedFailsWithoutRecreation`: drives a `testSaramaClient{closed: true}`. Asserts the error propagates AND `createCalls == 0` (no recreation attempted).

### Three-recovery bound test (AC5)
`TestConsume_PartitionConsumerClosed_BoundedAfterThreeRecoveries`: drives four consecutive closed channels with no successful batch. Asserts `calls == 4` (1 initial + 3 recoveries) and the error is `errPartitionConsumerClosed`.

### Counter-reset test (AC5)
`TestConsume_PartitionConsumerClosed_SuccessfulBatchResetsAttemptCounter`: drives `recovery, recovery, successful batch, recovery, recovery`. A never-resetting counter would error on the third recovery; this sequence asserts no error (ends with `context.Canceled`).

## Acceptance Criteria

- [ ] **AC1** — Closed `Errors()` channel is recoverable: sentinel returned, partition recreated, run continues, batch delivered.
- [ ] **AC2** — All four closed-channel branches return the sentinel: `errors.Is(err, errPartitionConsumerClosed)` in each of the four test cases.
- [ ] **AC3** — Recovery resumes at `nextOffset`; one WARN line names partition, offset, and attempt.
- [ ] **AC4** — Closed client (`client.Closed() == true`) propagates the error without attempting recreation (`createCalls == 0`).
- [ ] **AC5** — Three consecutive sentinel-triggered recoveries with no successful batch end the run; a successful batch resets the counter.
- [ ] **AC6** — `make precommit` exits 0.

## Verification

```
make precommit
```

Run from the project root. Expected: exit 0.

## Out of Scope

- Changes to the Kafka producer.
- Changes to non-offset consumers (`kafka_consumer-group-offset.go`, `kafka_consumer-simple.go`).
- New metrics beyond the single WARN log line.
- Any disable/switch mechanism.
