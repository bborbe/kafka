---
status: prompted
approved: "2026-10-19T17:01:19Z"
generating: "2026-10-20T18:36:24Z"
prompted: "2026-10-20T18:51:18Z"
branch: dark-factory/bug-partition-consumer-closed-sentinel
---

## Summary

- A partition consumer whose channels close is normal Sarama teardown — rebalance, connection drop, client close — not a fatal error.
- Today the offset consumer turns that into a fresh, unrecoverable error and aborts the whole consumer, so every partition dies with it.
- A closed channel on an active partition consumer becomes a recoverable class, and the existing recovery path recreates the consumer at the last good offset.
- The recovery is unconditional — this is a bug fix, and a switch nobody turns on would leave the reported error firing.
- Genuine failures still surface: a closed client, or three consecutive failed recreations, still ends the run with an error.

## Problem

`NUKE-PROD-AJ` has fired 226 times since 2026-05-16 and 18 times in the last 30 days: `*errors.dataError: run failed: consume failed: partition consumer messages channel closed`. A single Kafka connection drop closes the partition consumer's `Errors()` and `Messages()` channels. `consumeMessages` reads the closed channel and returns a freshly formatted error, which `recoverFromConsumeError` does not recognise, so the partition run returns and `run.CancelOnFirstError` aborts every other partition in the same consumer. The outage is self-inflicted: the condition is a normal teardown event that the client already recovers from internally, and the consumer converts it into a whole-service failure.

## Goal

A partition consumer whose channels close mid-run is replaced and the run continues from the last successfully consumed offset, so a routine rebalance or connection drop no longer fails the consumer or reaches Sentry as an error — for every consumer built from this library, with no call-site change.

## Non-goals

- **No disable switch.** The escape hatch is the regression: revert the commit. A switch nobody enables would leave the reported error firing, which is the bug this spec exists to fix.
- Do NOT treat an offset-out-of-range or corruption error as this class. Those have their own recovery paths and stay distinct.
- Do NOT swallow the error permanently. A closed client, or recovery that exhausts its consecutive attempts, must still end the run with an error.
- Do NOT add a metric. Exactly one warning log line is permitted on the new path (see AC3).
- Do NOT change `run.CancelOnFirstError` or the per-partition run wiring.
- Do NOT touch the Kafka producer or the non-offset consumer.

## Assumptions

- A closed `Errors()`/`Messages()` channel on an active partition consumer means teardown, not a genuine failure. Evidence: `NUKE-PROD-AJ`'s verdict (`Analyze Sentry issue 24cf245f… - 2026-10-10`, understanding High) and Sarama's contract that `Close()` closes both channels.
- The existing `recoverFromConsumeError` primitive is reusable — it already returns `(newPC, newOff, handled, recoverErr)` and the run loop swaps both.
- Recovery on the closed-channel path is safe unconditionally because it reuses the same recreation primitive the out-of-range path already exercises in production.
- Bounded attempts are **consecutive**, and the counter resets after a successful batch.
- The "client permanently closed" signal is `sarama.Client.Closed()`. `sarama.Consumer` exposes no such method; the run already holds the client.

## Alternatives considered

- **Opt-in flag** — rejected: no consumer would enable it, so the Goal would not be delivered and the reported error would keep firing.
- **Silently ignore the closed channel** — rejected: it hides genuine failures and stalls the partition at an invalid offset (the frozen anti-pattern from spec 002).
- **Retry at the client level** — out of scope: the client already reconnects; the defect is that the consumer does not survive the teardown.
- **Do nothing** — see the last section.

## Acceptance Criteria

- [ ] **A closed `Errors()` channel is recoverable.** `consumeMessages` returns the sentinel, and the partition run recreates the consumer and continues.
  - evidence: a unit test drives a fake partition consumer with a closed channel; asserts `errors.Is(err, errPartitionConsumerClosed)`, that the fake's create-call count is 1, **and that the run continues** — the recreated fake delivers a message that reaches the batch handler. Recreation alone is not the claim.
- [ ] **All four closed-channel branches return the sentinel.** Both `Messages()` branches and both `Errors()` branches, each covered by its own test case.
  - evidence: four unit cases, each asserting `errors.Is(err, errPartitionConsumerClosed)` from `consumeMessages` for its branch. No source-text grep — the assertion is on behaviour.
- [ ] **Recovery resumes at the recorded `nextOffset`** (last handled offset + 1), and one warning line names the partition, that offset and the attempt number.
  - evidence: a unit test asserts the recreation call receives that `nextOffset`; the warning line is asserted at WARN level with its fields.
- [ ] **A closed client still fails, without attempting recreation.**
  - evidence: a unit test drives a fake whose `Closed()` reports true; asserts a non-nil error **and** a create-call count of **0**.
- [ ] **Three consecutive sentinel-triggered recoveries with no successful batch between them end the run; a successful batch resets the counter.** A `CreatePartitionConsumer` error is **not** one of these attempts — it propagates immediately, exactly as today.
  - evidence: case A drives three consecutive sentinel-triggered recoveries, none delivering a message, and asserts the error on the third; case B drives `recovery, recovery, successful batch, recovery, recovery` and asserts **no** error — a never-reset counter would error on the third recovery, so the sequence discriminates.
- [ ] **`make precommit` exits 0.**

## Verification

```bash
make precommit
```

Run from the project root. Never from a sibling checkout — a green run there tests code that does not contain the change.

## Desired Behavior

1. A closed `Errors()` or `Messages()` channel on an active partition consumer is a recoverable teardown, not a failure.
2. The four closed-channel branches in `consumeMessages` return a package-level sentinel `errPartitionConsumerClosed` instead of a formatted error.
3. `recoverFromConsumeError` recognises the sentinel and returns a recreated partition consumer at the last good `nextOffset` with `handled=true`.
4. Recovery is bounded: **3 consecutive sentinel-triggered recoveries** with no successful batch between them, then the error propagates. A `CreatePartitionConsumer` error propagates immediately, unchanged. There is no delay between attempts — replay from `nextOffset` is safe, so failing fast beats a backoff.
5. When `sarama.Client.Closed()` reports true, recreation is not attempted; the error propagates immediately.
6. A genuine consume error that is not this class propagates exactly as it does today.

## Suggested Decomposition

One prompt. The change is a single file plus its two existing internal test files; splitting it across prompts would separate each behaviour from the test that proves it.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Sentinel + four branches + recovery branch with bounded consecutive attempts + all tests | 1-6 | 1-6 | — |

## Constraints

- No new exported API; the change is internal to the consumer.
- Reuse the existing recreation primitive; do not build a second one.
- Existing tests pass, with one deliberate exception: the four closed-channel tests at `kafka_consumer-offset_internal_test.go:474-550` change their assertion from `strings.Contains(err.Error(), "… channel closed")` to `errors.Is(err, errPartitionConsumerClosed)`. They assert the old message text today, which the sentinel replaces by construction.
- Frozen symbols for this change: `consumeMessages`, `recoverFromConsumeError`, `errPartitionConsumerClosed`.

## Failure Modes

| Trigger | Expected | Observable recovery |
|---|---|---|
| Rebalance closes the partition consumer mid-run | Sentinel → recreation at the last good offset → consume continues | One WARN line naming partition, offset, attempt; no Sentry event |
| Broker connection drop | Same as above | Same WARN line; no Sentry event |
| Client permanently closed | Error propagates, `createCalls == 0` | `Consume` returns non-nil; process exits non-zero with `run failed: consume failed: …` |
| 3 consecutive recreation failures | Error propagates after the third | Same non-zero exit; the third WARN line is the last |
| Two partitions close simultaneously | Each partition run recovers independently; no shared counter | One WARN line per partition |
| Offset manager unavailable during recreation | Error propagates rather than spinning | `Consume` returns non-nil |

## Reproduction

Observed on `nuke-prod`: `NUKE-PROD-AJ` (`7484357732`), 226 lifetime events, 18 in the trailing 30 days, `last_seen 2026-10-18T09:29:15Z`. Verdict `real bug`, understanding High, fix certainty Medium (`Analyze Sentry issue 24cf245f… - 2026-10-10`). Root cause pinned to `kafka_consumer-offset.go` — the closed `PartitionConsumer.Errors()` channel is escalated to a fatal consumer abort. Regression from commit `6891889` (2026-05-13), first released `v1.22.15`.

## Do-Nothing Option

The error is recoverable at the transport layer but not at the consumer layer, so every rebalance costs a consumer restart and a Sentry event — 18 in the last 30 days, one per affected pod per event. It also costs quota on a plan the fleet already exhausts. Doing nothing keeps a routine condition indistinguishable from a real outage.
