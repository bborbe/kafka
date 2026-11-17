---
status: verifying
approved: "2026-11-16T11:09:29Z"
generating: "2026-11-16T11:29:26Z"
prompted: "2026-11-16T11:29:26Z"
verifying: "2026-11-17T01:10:50Z"
branch: dark-factory/kafka-decode-error-attribution
---

## Summary

- A consumer decode failure names the topic and partition but not the record, so an operator cannot act on it without a manual binary search through the log segment.
- The consumer-error metric cannot distinguish a corrupt batch from a broker-side leader or coordination error, so both land in one series and one gets misread as the cause of the other.
- The error handler both logs and returns the same error, so one failure is reported twice at two layers.

## Problem

When `develop-agent-task-v1-event/0` returned a corrupted record batch, the consumer crashlooped for five and a half hours and the operator-facing error read `kafka: error while consuming develop-agent-task-v1-event/0: kafka: error decoding packet: CRC didn't match expected 0x70ee976c got 0xa4df6a67`. It names the topic and the partition. It does not name the offset, which is the one value the recovery procedure needs to start its scan. In the same period a separate burst of broker leader and coordination failures tripped the same metric, so the corruption appeared to cause an outage it had nothing to do with.

## Goal

A decode failure is attributable to the record that caused it and is classifiable against every other consumer error, using the metric and error surface the library already exposes.

## Non-goals

- Changing crash-versus-skip behaviour. The consumer still returns on a decode failure; `WithSkipCorruptBatches` remains off by default and is not enabled here.
- Recovering, repairing or rewriting any Kafka topic or log segment.
- Adding a log line at any site that has none today.
- Migrating `consumerErrorCounter` from a gauge to a counter, or renaming it. That is a separate change with its own breaking blast radius and its own do-nothing argument; this spec adds a label to the metric as it stands.
- `consumerCorruptBatchSkippedCounter`, which has the same gauge-named-like-a-counter defect. It belongs to sibling spec `001-crc-corruption-skip`.
- Any change to `agent-task-executor` — it consumes this library through its pin, and the pin bump is a separate repository's work.

## Assumptions

- `github.com/bborbe/errors`' `AddToContext(ctx, key, value)` returns a **context**, not an error. The value reaches the error only through a later `Wrapf(ctx, …)`, which calls `AddContextDataToError`. So the implementation attaches `offset` to the per-iteration context and lets the existing `Wrapf` carry it — one line, no signature change.
- Context data never enters the error **message**: `dataError.Error()` delegates to the cause. So the offset is machine-readable through `errors.DataFromError(err)` and cannot disturb the message substrings other tooling greps for.
- The offset is operator-visible without a log line because `bborbe/sentry`'s `addErrorTags` converts `errors.DataFromError` into Sentry event tags. The offset therefore becomes a Sentry **tag**, which is high cardinality — acceptable for rare error events, and the reason the Prometheus label set is kept closed instead.
- `NewMetrics()` returns `&metrics{}` whose methods write to package-level vectors registered on the **default** Prometheus registry in `init()`. A fresh instance is not an unscraped instance; the vectors are shared. This spec does not require a metrics injection point.
- `IsCorruptionError` is the established predicate for this library and it does `errors.As` on `PacketDecodingError` first, then falls back to substring matching. This spec reuses it as-is and introduces no new substring matching.
- The second report the handler duplicates belongs to the consuming application, not this library: nothing in `bborbe/kafka` logs the error `Consume` returns. Removing the handler's `glog.Warningf` is safe only because the caller reports the returned error, and `agent-task-executor` does. That dependency is stated here rather than assumed.
- The internal test package cannot use Ginkgo — `kafka_suite_test.go` holds the only `RunSpecs`. New tests in `package kafka` use plain `testing.T`; sibling spec 003 settled that convention and this spec does not re-derive it.

## Desired Behavior

1. When the offset consumer receives an error from a partition consumer, the returned error's context carries the **offset of the failing record** — the offset the partition consumer will read next: the last delivered offset plus one when a message was delivered, otherwise the offset the partition consumer was created at. That is where the recovery scan starts, and it is defined in the error-before-any-message case that produced the incident.
2. The error class is derived from the error type: `corruption` through the existing `IsCorruptionError`, `broker_side` through a type assertion on the sarama error, `other` for everything else. No new substring matching is introduced.
3. The consumer-error metric carries `error_class` alongside `topic` and `partition`, and the class values are exactly the three enumerated above.
4. The metric path and the classifier are the same function — the class a caller can test and the class the metric emits cannot diverge.
5. The handler returns the error without logging it, so the failure is reported once rather than twice.
6. The error message substrings other tooling depends on are unchanged.

## Acceptance Criteria

- [ ] A decode failure's error carries the offset of the failing record. Evidence: a test drives the **real `Consume(ctx)`** — not `consumeMessages` directly, which would let the test attach the context value itself and pass without the shipped path doing anything — in **two** cases. First, an error arriving **before any message is delivered**, asserting `errors.DataFromError(err)["offset"]` equals the partition consumer's creation offset; this is the incident case, and an implementation that returns `0` here fails. Second, an error after a delivered message, asserting the last delivered offset plus one. A constant, a zero, or the last-delivered offset in place of the next-expected one all fail.
- [ ] The consumer-error metric classifies a broker-side failure, not merely a corrupt one. Evidence: `prometheus/testutil` reads the real metrics implementation and asserts `kafka_consumer_error_counter{error_class="broker_side"}` **increments** for an `ErrNotLeaderForPartition` while `error_class="corruption"` does not, and that a `PacketDecodingError` increments `corruption` while `broker_side` does not. Both directions are positive assertions, so a metric that never emits `broker_side` fails. Each case uses a unique `topic`/`partition` pair, because the vectors are package-level and shared and a reused pair makes the negative assertion flaky.
- [ ] Every classified error lands in exactly one of the three classes. Evidence: a table test drives a `PacketDecodingError`, an `ErrNotLeaderForPartition`, and an unrelated error through the classifier and asserts three distinct values — including `other` — with no two sharing a value.
- [ ] Classification comes from the error type, not its text. Evidence: an error whose message contains `not a leader` but whose type is not a sarama error classifies as `other`, not `broker_side`.
- [ ] No label carries an unbounded value. Evidence: the registered label set is exactly `topic`, `partition`, `error_class`, and a test asserts every classifier output is a member of the exported enumeration.
- [ ] The handler does not both log and return the same error. Evidence: `grep -n 'glog\.' kafka_consumer-error-handler.go` returns zero matches, and the error returned still carries the offset from the first criterion.
- [ ] The error message substrings other tooling depends on are unchanged. Evidence: the wrapped error's `Error()` still contains `parition consumer returns error`, `consume failed` and `run failed`.
- [ ] `make precommit` exits 0.

## Verification

### Container-executable

- `make precommit` — format, generate, lint, test and security checks clean (this already runs `make generate`)
- `grep -n 'glog\.' kafka_consumer-error-handler.go` — returns zero matches
- `grep -n 'error_class' kafka_metrics.go` — returns the label declaration

### Operator-executable

- `grep -rn 'kafka_consumer_error_counter' ~/Documents/Obsidian/private-personal/65\ Runbooks ~/Documents/workspaces/nuke/alerts` — **zero hits is the expected pre-state** (verified 2026-11-16); any future hit must be updated, because the series now splits on `error_class` and a query that aggregates across classes will silently average them

## Constraints

- The error must be built with `github.com/bborbe/errors` — `AddToContext` on the context, `Wrapf` to wrap. No `fmt.Errorf`, no bare `return err` at a new site.
- The `error_class` values are exactly `corruption`, `broker_side`, `other`. Adding one is a code change, not a config change.
- No label may carry an offset, a message key, a timestamp or any other unbounded value.
- `MetricsConsumer.ErrorCounterInc(topic, partition)` gains a class parameter. That is a public interface change: it touches `metrics`, the generated `mocks/kafka-metrics.go`, and the in-package `fakeMetricsConsumer`. Counterfeiter regeneration is mandatory.
- The error message substrings `parition consumer returns error`, `consume failed` and `run failed` are preserved — sibling spec `003-transient-consumer-error-marker` freezes them and existing runbooks grep for them. Context data never enters the message, so they cannot be disturbed.
- Sibling spec `003-transient-consumer-error-marker` (`approved`) adds a transient/broker marker on the same error chain. This spec's `error_class` is a metric label, not a second marker — `003`'s `Transient()` keeps its current semantics. `003` also documents the `=> skip:` log line at `kafka_consumer-error-handler.go:36`, which this spec deletes; `003`'s text becomes stale on that one line and this is the note that says so.
- Sibling spec `002-offset-out-of-range-self-heal` is **completed and shipped** — `WithAutoResetOffsetOutOfRange` is live. The classifier must not change its behaviour. `001-crc-corruption-skip` (`prompted`) owns the skip path.
- With `WithSkipCorruptBatches` on, a corrupt batch short-circuits before the error handler, so `error_class="corruption"` stays at zero in exactly the mode where corruption is being skipped. A zero corruption count is therefore not evidence of zero corruption, and the metric must be read alongside the skip counter.
- Test conventions follow `docs/dod.md`: new functions have tests, Counterfeiter mocks are regenerated, error handling follows `github.com/bborbe/errors`.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility |
|---|---|---|---|---|
| Offset not in scope where the error is built | It is attached in `Consume`'s per-partition closure, where `nextOffset` lives — not in `consumeMessages`, which sees no position — and carried by the existing `Wrapf` | `make precommit` fails on a missing context value | Test failure | Reversible |
| The class is derived from `err.Error()` substring matching | The not-a-sarama-error case classifies as `broker_side` and the AC fails | Re-derive from the type predicates | Test failure | Reversible |
| The metric emits only `corruption` and `other` | The positive `broker_side` assertion fails | Wire the metric to the same classifier function | Test failure | Reversible |
| The handler log is removed and the application does not report the returned error | The failure would become silent | The Sentry path reports it as an event with the offset as a tag; confirm one event arrives on a driven failure | Sentry event and tag | Reversible |
| A new class value is added without enumeration | The closed-set assertion fails | Add it to the enumeration | Test failure | Reversible |
| `003`'s error-text invariant is broken | The substring assertion fails | Context never enters the message, so this cannot occur without an explicit message edit | Test failure | Reversible |

## Security / Abuse

No network, file or user input is introduced. The change touches only in-process error construction and metric labelling. The one Prometheus abuse vector is label cardinality: an `error_class` derived from arbitrary error text would grow the series set without bound, which is why the class is a closed enumeration rather than a pass-through string. The offset becomes a Sentry tag and is deliberately kept out of Prometheus labels for the same reason.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Classifier plus offset in error context | 1, 2, 5, 6 | 1, 3, 4, 6, 7 | — |
| 2 | `error_class` label on the metric, wired to the same classifier | 3, 4 | 2, 5 | prompt 1 (reuses the classifier) |

`make precommit` (AC 8) is an implicit gate for both prompts.

Rationale: prompt 1 establishes the classifier and the context attachment; prompt 2 builds the metric surface on top of the same classifier, so it follows rather than runs in parallel.

## Do-Nothing Option

The defect is dormant, not fixed: the corrupt batch is gone, demand is back to about one event per day, and nothing forces this work. What remains is that the next corruption produces the same unattributable error and the same crashloop, and an operator again binary-searches a log segment to find the offset — the cost that made the original incident expensive. Doing nothing also leaves the metric unable to separate corruption from a broker-side burst, so the next such pair will again be misdiagnosed as cause and effect.
