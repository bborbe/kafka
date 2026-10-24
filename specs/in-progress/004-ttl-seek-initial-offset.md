---
status: verifying
approved: "2026-10-24T16:35:43Z"
generating: "2026-10-24T17:01:01Z"
prompted: "2026-10-24T17:01:01Z"
verifying: "2026-10-24T17:06:23Z"
branch: dark-factory/ttl-seek-initial-offset
---

## Summary

- Add an opt-in `OffsetManager` that, when a partition has **no committed offset**, starts at the first offset whose message is younger than a caller-supplied TTL, instead of at the earliest retained offset.
- The TTL seek is what keeps a restart from replaying history while still honouring a message published just before the restart: it skips only what is already older than the TTL.
- The manager never resolves a missing offset to the oldest sentinel, including through `InitialOffset()` and `FallbackOffset()` — both report `OffsetNewest`, so an out-of-range recovery cannot silently reinstate a full replay.
- Opt-in addition to `bborbe/kafka`; no existing constructor, default, or behaviour changes.
- Scope is the `bborbe/kafka` library only. Wiring it into the task controller (and its vendor bump) is a follow-up prompt in `bborbe/agent-task-controller` after this library is tagged, and runtime proof belongs to that follow-up.

## Problem

`bborbe/kafka`'s offset consumers start from the `OffsetManager`'s initial offset when a partition has no committed offset. Every offset manager in the library is constructed with an explicit initial offset, and the value callers pass today is `OffsetOldest`. On a topic that holds roughly 68,000 messages spanning six days, a consumer with no committed offset reads all of them on every boot; at the measured replay rate of 17.85 messages/second that is about 63 minutes before the first live message is reached, and every replayed message older than the consumer's TTL is discarded on arrival anyway. (Figures measured 2026-10-24 on the `agent-task-controller-openclaw` consumer: lag 63,382 → 57,123 over 351 s, and offsets 355,910 → 424,026 across the retained window.)

The library offers only two sentinels for this choice: replay everything retained, or skip everything published before the consumer started. Neither expresses "start where the data is still relevant".

## Goal

`bborbe/kafka` offers an `OffsetManager` that resolves a missing offset by asking the broker for the first offset at or after `now − TTL`, persists that resolution, and returns it — so a consumer with no committed offset starts at the oldest still-relevant message rather than at the oldest retained one, and a restart resumes from what it last consumed.

A consumer that does not construct this manager behaves exactly as it does today.

## Non-goals

- Do NOT change `CreateSaramaConfig`, `config.Consumer.Offsets.Initial`, or any default. The sarama field is not consulted by this library's consumers — they pass an explicit offset to `ConsumePartition` — so editing it would change nothing here while risking unrelated callers.
- Do NOT change `NewStoreOffsetManager`, `NewSaramaOffsetManager`, or `NewSimpleOffsetManager`, or their initial-offset semantics.
- Do NOT change the consumer's recovery paths (`skipAndAdvance`, `resetOnOffsetOutOfRange`, the creation-time out-of-range fallback). This manager only supplies different values to them.
- Do NOT add a batch-level message filter here. That is a separate spec (`pre-tx-message-filter.md`).
- Do NOT wire this into any consuming service, and do NOT add a runtime or cluster verification rung. The benefit is consumer-visible, and the proof belongs to the follow-up wiring prompt in `bborbe/agent-task-controller`.
- Do NOT add a scenario. The behaviour is reachable by unit tests with an injected client and store.

## Acceptance Criteria

- [ ] `NextOffset` returns the stored offset without contacting the broker when the store has one, and asks the broker for the first offset at or after `now − TTL` when it does not — evidence: Ginkgo test with `mocks.KafkaSaramaClient` asserts the stored value is returned with `GetOffsetCallCount()` 0, then, with an empty store and `GetOffsetReturns(5000, nil)`, asserts the result is `5000` and the recorded `GetOffsetArgsForCall(0)` timestamp equals the fixed getter's time minus the TTL in Unix milliseconds exactly; `go test` exits 0.
- [ ] The resolved offset is persisted before it is returned, so a later `NextOffset` on a fresh manager sharing the same store returns it without a second broker seek — evidence: Ginkgo test constructs a second manager over the same store and asserts the result matches and `GetOffsetCallCount()` is still 1.
- [ ] When persisting the resolved offset fails, `NextOffset` returns an error and does not return the unpersisted offset — evidence: Ginkgo test with `GetOffsetReturns(5000, nil)` and `KafkaOffsetStore.SetReturns(err)` asserts a non-nil error and `Offset(0)`, and a second manager over the same store still seeks (`GetOffsetCallCount()` 2).
- [ ] A broker seek failure is returned as an error and is NOT downgraded to a sentinel — evidence: Ginkgo test with `GetOffsetReturns(0, err)` asserts `NextOffset` returns a non-nil error and the offset value `Offset(0)`.
- [ ] A store error that is neither `libkv.KeyNotFoundError` nor `libkv.BucketNotFoundError` is returned as an error and does NOT trigger a broker seek — evidence: Ginkgo test with `mocks.KafkaOffsetStore.GetReturns(0, errors.New("disk full"))` asserts a non-nil error, `Offset(0)`, and `GetOffsetCallCount()` 0.
- [ ] A broker result below zero resolves to `OffsetNewest` — evidence: Ginkgo test with `GetOffsetReturns(-2, nil)` (a value that is not itself a valid sentinel) asserts `NextOffset` returns `OffsetNewest`, no error, and `KafkaOffsetStore.SetCallCount()` is 0 so no sentinel is persisted.
- [ ] `InitialOffset()` and `FallbackOffset()` both return `OffsetNewest`, so no recovery path can resolve a missing or out-of-range offset to the oldest sentinel — evidence: Ginkgo test asserts both methods return `OffsetNewest`; `grep -n 'OffsetOldest' kafka_offset-manager-ttl-seek.go` returns 0 lines.
- [ ] `MarkOffset`, `ResetOffset`, and `Close` delegate to the internally constructed store manager, and `NextOffset` after `Close` returns `ErrClosed` — evidence: Ginkgo test calls `MarkOffset` then reads the value back through a second manager over the same store, asserts `ResetOffset` moves it backwards, asserts `Close` returns nil, and asserts `NextOffset` afterwards returns `ErrClosed`.
- [ ] Existing behaviour is untouched — evidence: `git diff --stat HEAD --` and `git status --porcelain --` for `kafka_offset-manager-store.go`, `kafka_offset-manager-sarama.go`, `kafka_offset-manager-simple.go`, `kafka_offset-manager.go`, `kafka_consumer-offset.go`, and `kafka_sarama-config.go` are both empty; `go test ./...` exits 0.
- [ ] `make precommit` exits 0.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — format, vet, lint, and the full test suite; expect exit code 0.
- `go test ./...` — expect exit code 0.
- `grep -n 'NewTTLSeekOffsetManager' kafka_offset-manager-ttl-seek.go` — expect at least one match.
- `grep -n 'OffsetOldest' kafka_offset-manager-ttl-seek.go` — expect 0 matches.

## Desired Behavior

1. `NewTTLSeekOffsetManager(saramaClient SaramaClient, offsetStore OffsetStore, ttl libtime.Duration, currentDateTime libtime.CurrentDateTimeGetter) OffsetManager` is available and returns a manager satisfying the existing `OffsetManager` interface.
2. `NextOffset` consults the store first. On a hit it returns the stored offset unchanged and does not contact the broker. On a miss — and only for `libkv.KeyNotFoundError` or `libkv.BucketNotFoundError` — it asks the broker for the first offset at or after `now − TTL`, expressed in Unix milliseconds, and returns that offset. Any other store error is returned as an error; a store outage must never be mistaken for a cache miss.
3. A non-negative broker result is persisted through the store before being returned, so the resolution survives a crash before the first message is marked and is not repeated on the next call.
4. A broker result below zero resolves to `OffsetNewest`. A broker error is returned to the caller; the manager never substitutes a sentinel for a failed lookup.
5. `MarkOffset`, `ResetOffset`, and `Close` delegate to the internally constructed store manager, and `NextOffset` after `Close` returns `ErrClosed`, matching every sibling manager. `InitialOffset()` and `FallbackOffset()` both return `OffsetNewest` — the true initial offset is time-dependent and cannot be expressed as a constant, and `OffsetOldest` in the fallback path would reinstate the replay this manager exists to remove. `InitialOffset()` is not read by any consumer; `FallbackOffset()` **is** read by the consumer's out-of-range recovery paths (`kafka_consumer-offset.go:425,667,727,787`), which is exactly why it must report `OffsetNewest`.

## Assumptions

- The broker's `ListOffsets` by timestamp is enabled and message timestamps reflect production time closely enough for a TTL window. Both hold for the topics this library serves; a topic configured with `LogAppendTime` is if anything more accurate.
- `sarama.Client.GetOffset` returns the most recent available offset at the requested timestamp, per the doc comment and signature at `github.com/IBM/sarama@v1.61.1/client.go:85-89`. The no-match case is not relied on: a negative result is handled explicitly by Desired Behavior 4.
- The offset store tolerates being written from `NextOffset`: it is the same operation the consumer performs after every batch.
- A caller that opts in accepts that messages older than the TTL are never read — that is the point of the option, and it is the caller's TTL, not a library default.

## Constraints

- Default behaviour is frozen. No existing constructor, default, or exported signature changes. A consumer that does not construct this type must be observably identical.
- `CreateSaramaConfig` must not be modified.
- The manager must not import or depend on `bborbe/cqrs`, a command schema, or any expiry concept. It knows only a duration.
- Tests follow the repo's `docs/dod.md`: Ginkgo/Gomega in `package kafka_test`, using the existing Counterfeiter mocks in `mocks/` (`KafkaSaramaClient`, `KafkaOffsetStore`). Do not add a second `RunSpecs` bootstrap — reuse `kafka_suite_test.go`.
- The new file follows the repo's flat naming: `kafka_offset-manager-ttl-seek.go`, BSD license header, GoDoc on every exported symbol.
- The GoDoc on the constructor carries the durable rationale: the replay arithmetic, the "store miss only on `KeyNotFoundError`/`BucketNotFoundError`" rule, and the `FallbackOffset → OffsetNewest` trap with its four recovery call sites. A short `docs/offset-managers.md` records the same three points, because the spec is archived after implementation and the follow-up wiring work lives in another repository.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility | Concurrency |
|---------|-------------------|----------|-----------|---------------|-------------|
| Broker seek fails (`ListOffsets` error, broker unreachable) | Error returned from `NextOffset`; `Consume` returns it and `run.CancelOnFirstError` aborts the consumer — the library has no internal retry | The caller's own retry loop reconnects; an operator confirms recovery when the consumer's first `NextOffset` line reports a concrete offset instead of `oldest` | The caller logs the returned error; the library itself emits no log line on this path | N/A — nothing persisted | No partial write: the store is written only after a successful seek |
| Store write fails after a successful seek | Error returned from `NextOffset`; the resolved offset is NOT returned | Next attempt re-seeks to the same TTL window; an operator confirms recovery by seeing a persisted offset in the store on the following start | The caller logs the returned error, which wraps the store error | Reversible — a re-seek resolves to the same window | No torn state: the store write is one transaction |
| Broker rejects `ListOffsets` by timestamp (broker older than Kafka 0.10.1, or the topic's timestamps are producer-set and skewed) | Error returned from `NextOffset`; no silent fallback to a sentinel | Operator checks the broker version and the topic's `message.timestamp.type`, then either upgrades or accepts that this manager cannot be used on that topic | The returned error naming the topic and partition; the library emits no log line | N/A — nothing persisted | N/A |
| Broker returns a negative offset | `OffsetNewest` returned, no error, nothing negative persisted | None needed; the operator confirms the first consume starts at the head | First consume log line reports a high offset, not `oldest` | N/A | Same |
| Two processes share one store | Both may seek once; the later write wins with an equal or newer offset | None needed | Both processes' first `NextOffset` lines report a concrete offset within the same TTL window | Reversible | Store writes are single transactions |
| TTL longer than the topic's retention | Seek returns the earliest retained offset — a replay, but bounded by retention rather than by the TTL | Operator shortens the TTL and confirms the next boot starts at a later offset | Consumer lag at boot | Reversible | N/A |
| Consumer clock ahead of producer/broker by more than the TTL | The seek resolves past live messages and skips them permanently, with no error and no metric | Operator checks NTP on the node, restores the clock, and re-reads the skipped range only by resetting the stored offset backwards | Absent messages with a `NextOffset` that resolved to the log end offset | Irreversible for the skipped messages once the offset advances | N/A |
| Stored offset falls outside retention (`OffsetOutOfRange`) | Existing recovery resets to `FallbackOffset()` — `OffsetNewest` here, so the partition resumes at the head instead of replaying | Automatic; the operator confirms the reset log line names `newest` | Existing reset warn log line | Reversible | Unchanged consumer path |

## Do-Nothing Option

Doing nothing leaves every consumer with the choice between two sentinels. A consumer with no committed offset either replays everything retained or skips everything published before it started, with no way to express "start where the data is still relevant". Both are safe — nothing corrupts — but the first turns a routine restart into about an hour of work whose entire output is discarded, and the second makes a restart silently lossy in proportion to how long the consumer was down.
