---
status: approved
approved: "2026-10-18T10:25:26Z"
generating: "2026-10-18T10:32:27Z"
branch: dark-factory/transient-consumer-error-marker
---

## Summary

- Mark the shared offset consumer's **transient** broker-unreachable errors — end-of-stream and connection-failure causes — with a structural marker method, so a downstream reporter can recognise the class without importing this library.
- The marker is **additive and behaviour-neutral**: the consumer still returns the same error, still tears down, still propagates exactly as today. Nothing about consuming changes. Only the error's *shape* gains a method.
- Classification is a **narrow positive list**. An offset-out-of-range error, a corruption-class error, an auth failure, and anything unclassified are **not** marked, so they keep reaching the reporter.
- The marker is a structural interface — implicit satisfaction means this library imports neither the reporter nor any shared marker package. The consuming change is the sibling reporter spec in the service repository.
- Scope is this library only. The reporter-side predicate, the consumer bump, and the runtime broker-loss verification are separate: the last two are owned by the vault task **Suppress the Trading Fleet's Kafka Consumer-Error Storm on Broker Outage**.

## Problem

A single Kafka broker outage makes every consuming pod report its own Sentry error for the same event. On 2026-10-12 a broker became unreachable and **158 distinct trading-service pods** each emitted an error for it, producing 251 distinct `event_id`s in one hour. The burst exhausted `sentry-proxy`'s global rate-limit window, so a large block of alerts was rejected before Sentry ever stored it.

The fan-out is **per-pod by construction, not per-retry**: the consumer builds one run-function per partition, `run.CancelOnFirstError` collapses however many partition errors occur into a single returned error, and the reporter captures exactly once per run. The event count therefore equals the replica count, and no per-process limiter can reduce it.

The loss is that a routine, self-resolving network condition is reported as an error once per pod. The consumer's error handler already *intends* to treat it as skippable — it logs a `=> skip:` line — but then returns the error unconditionally. That naming lie is **not** this spec's target: returning `nil` there is a frozen anti-pattern (it leaves the partition stalled at an invalid offset). This spec makes the class *identifiable*; deciding what to do about it belongs to the reporter.

## Goal

An error returned by the offset consumer whose cause is a transient broker-unreachable condition satisfies the structural interface `interface{ Transient() bool }`, and calling `Transient()` on it returns `true`. Every other error the consumer returns — offset-out-of-range, corruption, auth, and anything unclassified — either does not satisfy that interface or returns `false`. The consumer's runtime behaviour, error values, and error text are otherwise unchanged.

## Non-goals

- Do NOT change consumer behaviour. The error is still returned, the consumer still tears down, the partition function still fails — identical to today. This spec adds a method to an error, nothing more.
- Do NOT make the consumer error handler return `nil` for transient errors. Frozen anti-pattern, carried from spec 002: swallowing the error in the handler stalls the partition. The marker does not suppress anything; it only labels.
- Do NOT mark an offset-out-of-range error. It is self-healed by spec 002 and is a distinct class with its own recovery path.
- Do NOT mark corruption / CRC errors. Those are owned by the CRC-recovery workstream and must stay loud.
- Do NOT import the reporter library or add a shared "errors" package. The marker is structural precisely so no dependency is created.
- Do NOT add retry, backoff, reconnect, or coalescing logic. This spec classifies; it does not recover.
- Do NOT change the error's message text, and do NOT add a metric or log line. The marker is the whole observable.
- Do NOT add an opt-in flag or a configurable cause list. The classification is fixed; a future consumer needing variation is a separate spec.

## Acceptance Criteria

- [ ] **A transient error returned by the real consume path satisfies the marker.** Evidence: a named in-package test (`TestConsume_TransientBrokerErrorSatisfiesMarker`) that drives the **real** `Consume(ctx)` — not `consumeMessages` directly, and not a constructor-only fixture — delivers an `EOF`-caused `*sarama.ConsumerError` on the fake partition consumer's error channel, and asserts on the **returned error value** that `var t interface{ Transient() bool }; errors.As(err, &t) && t.Transient()` is true; `go test -run TestConsume_TransientBrokerErrorSatisfiesMarker ./...` exits 0.

  ⚠️ **This AC states the rule, not a field list — deliberately.** Two ways to fail it, both hit during review: **(a)** asserting on a value built by a constructor **instead of** the returned one; **(b)** constructing the consumer with a field left nil that `Consume` dereferences before the assertion is reached — **every field on the path to the error handler must be non-nil**. Derive that path from `Consume`'s own source; do **not** copy an existing helper's field list, because the helpers differ and each is safe only for the case it was written for:

  - `newOffsetConsumerForTest` (`kafka_consumer-offset_internal_test.go:459`) sets only `batchSize`, `errorHandler` and `metrics` — `saramaClientProvider` is nil, so `Consume` panics on its **first statement** and the fake is never reached. It is a `consumeMessages`-level helper only.
  - `buildSkipTestConsumer` (`:343`) sets `saramaClientProvider`, `saramaConsumerFunc`, `offsetManager`, `topic`, `messageHandlerBatch`, `metrics`, `batchSize`, `logSampler`, `consumerOptions`, `skipper` and `waiter` — but **not `errorHandler`**. It is safe for its own test only because that test feeds a *corruption* error, which short-circuits at the corruption branch before the handler call. A **transient** error does not short-circuit: it falls through to `c.errorHandler.HandleError` (`kafka_consumer-offset.go:491`), so a construction omitting `errorHandler` panics there.
  - Conversely `c.skipper.FindNextHealthyOffset` (`:557`) is reached only on the corruption-skip path, so it must be non-nil for AC 7's corruption-negative case **if** `consumerOptions.SkipCorruptBatches` is true — pin that option's value in the test rather than leaving it unset.
- [ ] **Every cause on the classified list is marked, driven through the real path.** Evidence: the same test is table-driven over all four causes — `io.EOF`, `io.ErrUnexpectedEOF`, a `connection refused` dial error, a `connection reset` error — and asserts the marker on the value `Consume` returns for each; `go test` exits 0.
- [ ] **The marker is found on the full wrapped chain, not only the innermost cause.** Evidence: the test asserts `errors.As` finds the marker on the returned error whose message contains `run failed` and `consume failed` (the outer wraps), not merely on the unwrapped cause; `go test` exits 0. (This is the load-bearing requirement — the reporter matches the error the consumer returns.)
- [ ] **A `broker not available` cause is marked.** Evidence: the same table-driven test includes the sarama broker-not-available cause and asserts the marker; `go test` exits 0. If implementation shows this cause never reaches the consumer through this path, remove it from the classification and from this AC together — do not leave the two out of step.
- [ ] **An unrelated consumer error is NOT marked.** Evidence: the test delivers a rebalance-timeout / auth-failure `*sarama.ConsumerError` and asserts the returned error does **not** satisfy the marker (or satisfies it and returns `false`); `go test` exits 0. (Negative AC.)
- [ ] **An offset-out-of-range consumer error is NOT marked.** Evidence: the test delivers the offset error and asserts the returned value fails the same assertion; `go test` exits 0. (Negative AC — this is the class spec 002 owns.)
- [ ] **A corruption-class error is NOT marked.** Evidence: the test delivers an error for which the existing corruption predicate is true and asserts the returned value fails the marker assertion; `go test` exits 0. (Negative AC.)
- [ ] **The returned error text is unchanged.** Evidence: a unit test asserts the returned error's message still contains `parition consumer returns error` and `consume failed` for the transient case, AND `grep -n 'return nil' kafka_consumer-error-handler.go` returns 0 lines; `go test` exits 0. (Negative AC.)
- [ ] **No new module dependency.** Evidence: `git diff origin/master..HEAD -- go.mod go.sum` returns empty; `go list -m all | grep -c .` unchanged from `origin/master`. (Not a bare `git diff` — the container commits, so an unstaged diff reads empty regardless.)
- [ ] **`make precommit` exits 0** in the kafka package — evidence: exit code 0.

## Verification

**Rung 1 — container-executable (runs inside the YOLO container at prompt time):**

```
make precommit
```

Expected: exit code 0. The new in-package tests pass — a transient cause delivered through `fakePartitionConsumer` makes `Consume` return an error satisfying the marker, for every cause on the classified list and through the full wrapped chain; an unrelated consumer error, an offset-out-of-range error, and a corruption-class error each fail that assertion; the returned error text is unchanged; `grep -n 'return nil' kafka_consumer-error-handler.go` returns 0 lines; `git diff origin/master..HEAD -- go.mod go.sum` is empty.

**Rung 2 — operator-executable, deferred to a named owning task (NOT satisfiable in this spec):**

The runtime proof — that a forced broker loss against the bumped consumers produces no per-pod Sentry fan-out — runs on the cluster, after the sibling reporter spec lands and consumers are bumped to both libraries. Owning task: **Suppress the Trading Fleet's Kafka Consumer-Error Storm on Broker Outage** (vault task `c6d987db-384a-4961-8e4e-affc60ef95a6`), subtasks *nuke dev* and *nuke prod*, whose expected result is ≤ 1 distinct `server_name` per incident against the 158-`server_name` baseline. **Completion semantics: this spec does NOT reach `completed` on Rung 1 alone** — it stays `verifying` until that task's forced-loss readings pass, the same deferred-rung pattern spec 002 used. This rung is not satisfiable inside `verify-spec`: it needs a deployed consumer and a real broker loss.

## Desired Behavior

1. The package classifies an error as transient when the cause at the end of its wrapped chain is one of: `io.EOF`, `io.ErrUnexpectedEOF`, a connection-refused dial error, a connection-reset error, or sarama's broker-not-available cause.
2. An error the consumer returns whose chain contains a transient cause satisfies the structural interface `interface{ Transient() bool }`, and that method returns `true` on it.
3. The classification walks the **whole wrapped chain**, so the marker is reachable from the error the consumer actually returns — not only from the innermost cause.
4. The consumer's runtime behaviour is unchanged: the same error value, with the same wrapped message, is returned from the same call sites on the same conditions. Only the error's satisfaction of the marker interface is new.

## Assumptions

- The storm's errors reach the consumer as `*sarama.ConsumerError` values whose cause is a transient network condition. The observed strings from the 2026-10-12 evidence — `kafka: error while consuming master-core-candle-v1-event-latest/0: EOF` and `dial tcp 192.168.178.42:30479: connect: connection refused` — are read from that record, not re-derived here.
- The consumer's error-wrapping library preserves enough structure for a chain walk to reach the cause. If it does not, the classification must be applied where the cause is still visible (the `consumeMessages` error branch) — agent decides at impl time, since it is local and reversible.
- A structural interface is the right contract rather than an exported sentinel: it lets the reporter match without importing this library, and lets a later library implement the same marker with no change to the reporter.
- The sibling reporter spec — `bborbe/service` *transient-error-exclusion*, at `~/Documents/workspaces/service/specs/transient-error-exclusion.md` — may land before or after this one. Until it lands this marker has no consumer, which is safe: the marker changes no behaviour.
- **Line references move with the revision.** `run.CancelOnFirstError` is at `kafka_consumer-offset.go:456` on `origin/master` and `:429` at the deployed tag `v1.25.10`; the `=> skip:` log is at `kafka_consumer-error-handler.go:36` on **both** revisions. Read the symbol, not the number.

## Constraints

- **Frozen anti-pattern (MUST NOT):** do NOT implement transient handling by returning `nil` from the consumer error handler. Carried verbatim from spec 002: the offset only advances after a successful message, so swallowing the error leaves the partition consumer stuck at the same invalid offset — a silent stall that looks healthy, does zero work, and never alerts.
- The marker is **structural**: `interface{ Transient() bool }`. This library must not import the reporter library, and no shared marker package may be introduced.
- The classification must be **narrow**. Marking an error class that is not transient silences a real defect at the reporter. When a cause is not clearly transient, it is not marked.
- An offset-out-of-range error and corruption-class errors are explicitly outside the transient class and must remain unmarked.
- The returned error value and message text must not change — existing runbooks and log greps depend on `parition consumer returns error`, `consume failed`, and `run failed`.
- ⚠️ **Test-framework collision, deliberate and named.** The repo's configured validation prompt (`docs/dod.md`, referenced by `.dark-factory.yaml`) says "Tests use Ginkgo/Gomega conventions", but the **internal** test package cannot use Ginkgo: `package kafka` and `package kafka_test` compile into one test binary and `kafka_suite_test.go:21` already holds the single `RunSpecs` the binary permits. The internal test file therefore has 29 plain `func Test` and no suite. New tests in the internal package **must** use plain `testing.T`; this spec's constraint overrides the stale DoD line, and the DoD line should be corrected separately (it outlives this spec).
- Counterfeiter is the mock generator (`mocks/` dir); the existing in-package fakes are the preferred test doubles here.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility | Concurrency |
|---------|-------------------|----------|-----------|---------------|-------------|
| Broker unreachable (EOF / refused / reset) | Error returned as today, now satisfying the marker | None needed here — consumer behaves exactly as before; the reporter's predicate decides whether to report | Marker assertion on the value `Consume` returns | N/A — no state changed | Unchanged: `CancelOnFirstError` still returns one error per `Consume` |
| Cause is transient but not on the classified list | Error is NOT marked; it reports as today | Widen the list deliberately in a follow-up | Missing marker in the table-driven test | Reversible — the list is code | N/A |
| Cause is on the list but is actually persistent (a broker down for hours) | Error is marked transient; once the reporter lands it will not be reported | The replacement signal must carry it — the owning task's forced-loss readings must show the `KafkaConsumerLag` alert firing, or the suppression is unsafe | Owning task's dev/prod forced-loss readings | Reversible — remove the cause from the list | N/A |
| Marker implemented only in a constructor the real path never calls | The Goal is not delivered while every AC could pass | This is the failure C1 in the audit named; AC 1 closes it by asserting on the **returned** value | AC 1's assertion on `Consume`'s return | Reversible | N/A |
| Offset-out-of-range arrives | NOT marked — unchanged, and still self-healed when spec 002's option is on | Spec 002's reset path | Negative AC on the returned value | N/A | Per-partition, unchanged |
| Corruption-class error arrives | NOT marked — unchanged and still loud | CRC recovery runbook | Negative AC on the returned value | N/A | Unchanged |
| Chain walk fails to reach the cause | Error is not marked; behaviour falls back to today's | Apply classification where the cause is visible | AC 3 asserting the marker through the outer wraps | Reversible | N/A |
| A future library needs the same marker | Implements the method itself; no change to this library | — | — | N/A | N/A |

## Suggested Decomposition

Single-layer, single-behaviour change in one package — a single prompt covers every Desired Behavior and Acceptance Criterion.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Add the transient classification and the structural marker on the value the offset consumer returns, with a table-driven in-package test covering marked and unmarked classes through the real `Consume` path | 1-4 | 1-10 | — |

## Do-Nothing Option

Doing nothing leaves the class unidentifiable, so no reporter can distinguish a transient broker blip from a real defect — which is why the 2026-10-12 storm cost 158 pods' worth of alert volume and a rejected block of proxy forwarding. The cost of acting is small and fully reversible: a method on an error type, changing no runtime behaviour, adding no dependency, and suppressible by removing a cause from the list. Doing nothing is acceptable only if the org decides a per-pod fan-out on every broker blip is the intended signal — which the 2026-10-17 scope decision (fix the shared library) already rejected.
