---
status: completed
spec: [004-ttl-seek-initial-offset]
summary: Added opt-in NewTTLSeekOffsetManager that seeks to now-minus-TTL on a store miss, persists the resolved offset, reports newest for both sentinels, with 10 Ginkgo specs, a docs page, and a CHANGELOG entry
execution_id: kafka-offset-ttl-exec-014-spec-004-ttl-seek-initial-offset
dark-factory-version: v0.198.7
created: "2026-10-24T16:53:53Z"
queued: "2026-10-24T17:01:01Z"
started: "2026-10-24T17:04:34Z"
completed: "2026-10-24T17:06:23Z"
---

# Add an opt-in TTL-seek offset manager

<summary>
- A new opt-in offset manager resolves a partition that has no committed offset by starting at the oldest message younger than a caller-supplied TTL, instead of at the oldest retained message
- A partition that already has a stored offset is served straight from the store and never contacts the broker
- A store outage is reported as an error and is never mistaken for a cache miss, so a broken database cannot silently turn into a full replay
- The resolved offset is written to the store before it is returned, so the seek happens once and a restart resumes where it left off
- A failed broker lookup is reported as an error and is never quietly replaced with a default position marker
- A broker answer that comes back negative resolves to the newest position marker and persists nothing
- The two default-position accessors report the newest offset, so the consumer's existing out-of-range recovery resumes at the head rather than replaying everything
- Every existing constructor, default, and consumer code path is untouched; a consumer that does not construct this type behaves exactly as it does today
- A short reference page records why the manager exists, since the spec is archived and the wiring work happens in another repository
</summary>

<objective>
Add `NewTTLSeekOffsetManager` to `bborbe/kafka` so a consumer with no committed offset starts at the oldest still-relevant message (`now − TTL`) rather than at the oldest retained one — turning a routine restart from roughly an hour of replay-into-the-bin into a seek that begins where the data is still useful, while leaving every existing offset manager and consumer path observably unchanged.
</objective>

<context>
This repo has no `CLAUDE.md`. Read `README.md` and `docs/dod.md` for conventions. The spec is authoritative and must be read in full before writing anything: `specs/in-progress/004-ttl-seek-initial-offset.md`.

Read these files before writing code — the new type mirrors their shapes exactly:

- `kafka_offset-manager.go` — the `OffsetManager` interface (`InitialOffset`, `FallbackOffset`, `NextOffset`, `MarkOffset`, `ResetOffset`, embedded `io.Closer`) and `TopicPartition`.
- `kafka_offset-manager-store.go` — `NewStoreOffsetManager(offsetStore OffsetStore, initalOffset Offset, fallbackOffset Offset) OffsetManager`, the `storeOffsetManager` struct (`mux sync.Mutex` + `closed bool`), and the two idioms this prompt copies verbatim: `errors.Wrapf(ctx, ErrClosed, "offsetManager is closed")` on a closed manager, and the store-miss discrimination `if errors.Is(err, libkv.KeyNotFoundError) || errors.Is(err, libkv.BucketNotFoundError)`. `ErrClosed` is declared here.
- `kafka_offset-store.go` — the `OffsetStore` interface (`Get(ctx, topic, partition) (Offset, error)`, `Set(ctx, topic, partition, offset) error`). Note `offsetStore.Get` returns `libkv.KeyNotFoundError` when the value is empty and the db layer returns `libkv.BucketNotFoundError` when the bucket is absent.
- `kafka_offset.go` — `Offset` (`type Offset int64`), `OffsetNewest`, `OffsetOldest`, `Offset.Int64()`.
- `kafka_topic.go` (`Topic.String()`) and `kafka_partition.go` (`type Partition int32`, `Partition.Int32()`).
- `kafka_sarama-client-creator.go` — `type SaramaClient interface { sarama.Client }`. The method used here is `sarama.Client.GetOffset(topic string, partitionID int32, time int64) (int64, error)`; the third argument is a Unix millisecond timestamp.
- `kafka_consumer-offset.go` — the four places the consumer reads `FallbackOffset()`: the per-partition run closure inside `Consume` (the `CreatePartitionConsumer` call at partition start), `skipAndAdvance`, `recoverFromConsumeError`, and `resetOnOffsetOutOfRange`. These four are the reason `FallbackOffset()` must not resolve to the oldest sentinel.
- `kafka_offset-manager-store_test.go` — the Ginkgo test style for offset managers (`package kafka_test`, `Describe`/`Context`/`It`, `libkafka "github.com/bborbe/kafka"`, `"github.com/bborbe/kafka/mocks"`).
- `kafka_suite_test.go` — the single `RunSpecs` bootstrap. Reuse it; do not add a second one.
- `mocks/kafka-offset-store.go` and `mocks/kafka-sarama-client.go` — the counterfeiter fakes. `KafkaOffsetStore` exposes `GetCalls`, `SetCalls`, `GetReturns`, `GetReturnsOnCall`, `SetReturns`, `SetReturnsOnCall`, `GetCallCount`, `SetCallCount`, `SetArgsForCall`. `KafkaSaramaClient` exposes `GetOffsetReturns`, `GetOffsetReturnsOnCall`, `GetOffsetCallCount`, `GetOffsetArgsForCall(i) (string, int32, int64)`.

`github.com/bborbe/time` (imported as `libtime`, already a direct dependency at v1.27.18) provides the clock type:
- `libtime.CurrentDateTimeGetter` — interface with `Now() DateTime`.
- `libtime.CurrentDateTimeGetterFunc` — `func() libtime.DateTime`, satisfies the getter; use it in tests, no mock needed.
- `libtime.DateTime` — `type DateTime stdtime.Time`; `DateTime.Time() stdtime.Time`, `DateTime.Add(d libtime.HasDuration) DateTime`.
- `libtime.Duration` — `type Duration stdtime.Duration`; `Duration.Duration() stdtime.Duration`. A `libtime.Duration` value therefore satisfies `libtime.HasDuration`, so `DateTime.Add` accepts `-s.ttl` directly.

Coding-plugin guides (in-container paths) for the conventions this file follows: `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-library-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-time-injection.md`.
</context>

<requirements>

### 1. New file `kafka_offset-manager-ttl-seek.go`

Flat repo naming, same package `kafka`, BSD header matching the siblings (`// Copyright (c) 2026 Benjamin Borbe All rights reserved.` / `// Use of this source code is governed by a BSD-style` / `// license that can be found in the LICENSE file.`).

Imports (exactly these; `make format` will group them):

```go
import (
	"context"
	"sync"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	libtime "github.com/bborbe/time"
)
```

Do not import `github.com/IBM/sarama` (the client is abstracted by `SaramaClient`), `github.com/golang/glog` (this path emits no log line), or any metrics package.

### 2. Struct and constructor

```go
// ttlSeekOffsetManager resolves a missing offset by asking the broker for the first offset at or
// after now minus a caller-supplied TTL, persisting that resolution, and returning it.
type ttlSeekOffsetManager struct {
	saramaClient    SaramaClient
	offsetStore     OffsetStore
	ttl             libtime.Duration
	currentDateTime libtime.CurrentDateTimeGetter
	offsetManager   OffsetManager

	mux    sync.Mutex
	closed bool
}
```

```go
// NewTTLSeekOffsetManager creates an offset manager that, when a partition has no committed
// offset, starts at the oldest message younger than ttl instead of at the oldest retained message.
func NewTTLSeekOffsetManager(
	saramaClient SaramaClient,
	offsetStore OffsetStore,
	ttl libtime.Duration,
	currentDateTime libtime.CurrentDateTimeGetter,
) OffsetManager {
	return &ttlSeekOffsetManager{
		saramaClient:    saramaClient,
		offsetStore:     offsetStore,
		ttl:             ttl,
		currentDateTime: currentDateTime,
		offsetManager:   NewStoreOffsetManager(offsetStore, OffsetNewest, OffsetNewest),
	}
}
```

The internally constructed store manager exists only so `MarkOffset`, `ResetOffset`, and `Close` have a sibling-shaped delegate. Its initial and fallback values are never read through this type's own accessors.

### 3. `NextOffset` — store hit, store miss, seek, persist

`NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error)`. Lock `s.mux` for the whole method and defer the unlock (same shape as `storeOffsetManager.NextOffset`). Order of operations, each a distinct branch:

1. **Closed check first.** If `s.closed`, return `0, errors.Wrapf(ctx, ErrClosed, "offsetManager is closed")` — the exact sibling idiom.
2. **Store read.** `offset, err := s.offsetStore.Get(ctx, topic, partition)`. If `err == nil`, return `offset, nil` immediately. The broker must not be contacted on a hit — no `GetOffset` call on this path at all.
3. **Miss discrimination.** Only `errors.Is(err, libkv.KeyNotFoundError) || errors.Is(err, libkv.BucketNotFoundError)` counts as a miss. Any other error returns `0, errors.Wrapf(ctx, err, "get offset failed")` and must NOT reach the broker. A store outage is never a cache miss.
4. **Seek.** Compute the timestamp exactly as:

   ```go
   seekTimestamp := s.currentDateTime.Now().Add(-s.ttl).Time().UnixMilli()
   ```

   `-s.ttl` is a `libtime.Duration`, which satisfies `libtime.HasDuration` — that is what `DateTime.Add` takes. Do not pass `s.ttl.Duration()` (a `stdtime.Duration` does not satisfy `HasDuration`), and do not call `stdtime.Now()` — the clock is injected. Then `brokerOffset, err := s.saramaClient.GetOffset(topic.String(), partition.Int32(), seekTimestamp)`. `sarama.Client.GetOffset` has no `context.Context` parameter, so this call cannot be cancelled — do not wrap it; the sibling `GetRealOffset` in `kafka_offset.go` calls it the same way. Two processes sharing one store need no extra handling: the seek is idempotent and the `Set` is a single transaction, so the later write wins with an equal or newer offset — do not add cross-process locking.
5. **Seek error.** If `err != nil`, return `0, errors.Wrapf(ctx, err, "get offset for topic(%s) partition(%d) at timestamp(%d) failed", topic, partition, seekTimestamp)`. Never substitute a sentinel for a failed lookup.
6. **Negative result.** If `brokerOffset < 0`, return `OffsetNewest, nil` and do NOT call `Set` — a negative value is never persisted.
7. **Persist, then return.** `if err := s.offsetStore.Set(ctx, topic, partition, Offset(brokerOffset)); err != nil { return 0, errors.Wrapf(ctx, err, "set offset failed") }`, then `return Offset(brokerOffset), nil`. The offset is written before it is returned; a failed write must not return the unpersisted value.

### 4. `InitialOffset` and `FallbackOffset`

Both return `OffsetNewest`. Two methods, one line each, GoDoc on each:

- `InitialOffset` — the true initial offset is time-dependent and cannot be expressed as a constant. No consumer reads it.
- `FallbackOffset` — this one **is** read by the consumer's recovery paths, so returning the oldest sentinel here would reinstate the full replay this manager exists to remove.

**The implementation file must not contain the identifier `OffsetOldest` anywhere — not in code, not in a comment, not in GoDoc.** `<verification>` asserts its absence. Describe the trap in prose ("the oldest sentinel", "the replay sentinel") rather than naming the constant.

### 5. Delegation and `Close`

```go
// MarkOffset persists the given offset as consumed for the specified topic and partition.
func (s *ttlSeekOffsetManager) MarkOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error {
	return s.offsetManager.MarkOffset(ctx, topic, partition, nextOffset)
}

// ResetOffset resets to the provided offset, allowing backward movement.
func (s *ttlSeekOffsetManager) ResetOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error {
	return s.offsetManager.ResetOffset(ctx, topic, partition, nextOffset)
}

// Close marks the offset manager as closed.
func (s *ttlSeekOffsetManager) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	return s.offsetManager.Close()
}
```

`Close` sets the local `closed` flag (so `NextOffset` returns `ErrClosed` afterwards) *and* closes the delegate. Lock ordering is safe: this type's `mux` and the store manager's `mux` are distinct and never acquired in opposite orders.

### 6. GoDoc on the constructor carries the durable rationale

The constructor's GoDoc must state, in prose:

- the replay arithmetic — a topic holding tens of thousands of messages across several days costs roughly an hour of replay at the measured rate before the first live message is reached, and every message older than the TTL is discarded on arrival anyway;
- the store-miss rule — only `libkv.KeyNotFoundError` and `libkv.BucketNotFoundError` count as a miss, because treating a store outage as a miss would silently turn a broken database into a full replay;
- the `FallbackOffset` trap — the sentinel accessors report the newest offset because the consumer reads `FallbackOffset()` in four recovery paths (`Consume`'s per-partition start, `skipAndAdvance`, `recoverFromConsumeError`, `resetOnOffsetOutOfRange`), and resolving any of them to the oldest sentinel would restart the replay.

Keep it under ~20 lines. Every exported symbol gets GoDoc (`revive` enforces it); the type is unexported and still gets a comment for consistency with the siblings.

### 7. Tests — `kafka_offset-manager-ttl-seek_test.go`

`package kafka_test`, Ginkgo/Gomega, reusing `kafka_suite_test.go`'s single `RunSpecs`. Do not add a `TestXxx` function or a second `RunSpecs`.

Imports the test file needs: `context`, `stderrors "errors"`, `stdtime "time"`, `github.com/bborbe/errors`, `libkv "github.com/bborbe/kv"`, `libtime "github.com/bborbe/time"`, `. "github.com/onsi/ginkgo/v2"`, `. "github.com/onsi/gomega"`, `libkafka "github.com/bborbe/kafka"`, `"github.com/bborbe/kafka/mocks"`.

Top-level `Describe("TTLSeekOffsetManager", ...)` so `-ginkgo.focus='TTLSeekOffsetManager'` selects the whole set. Shared setup: `ctx = context.Background()`, `topic = libkafka.Topic("my-topic")`, `partition = libkafka.Partition(0)`, a fixed clock

```go
fixedNow := stdtime.Date(2026, 10, 24, 12, 0, 0, 0, stdtime.UTC)
currentDateTime := libtime.CurrentDateTimeGetterFunc(func() libtime.DateTime {
	return libtime.DateTime(fixedNow)
})
```

and `ttl := libtime.Duration(libtime.Hour)`.

Use `mocks.KafkaSaramaClient` and `mocks.KafkaOffsetStore`. For the specs that need `Set` to actually persist across two managers, back the fake with a local map installed through the generated stub setters (set them once in `BeforeEach`, before any call):

```go
offsets := map[libkafka.TopicPartition]libkafka.Offset{}
offsetStore.GetCalls(func(
	ctx context.Context, topic libkafka.Topic, partition libkafka.Partition,
) (libkafka.Offset, error) {
	offset, ok := offsets[libkafka.TopicPartition{Topic: topic, Partition: partition}]
	if !ok {
		return 0, libkv.KeyNotFoundError
	}
	return offset, nil
})
offsetStore.SetCalls(func(
	ctx context.Context, topic libkafka.Topic, partition libkafka.Partition, offset libkafka.Offset,
) error {
	offsets[libkafka.TopicPartition{Topic: topic, Partition: partition}] = offset
	return nil
})
```

Do not mix `GetReturns`/`SetReturns` with `GetCalls`/`SetCalls` on the same fake — the generated setters null the stub, so the later call wins silently.

Write one `It` per assertion below (ten specs total, grouped under `Context`s):

1. **Store hit is served without the broker.** `GetReturns(libkafka.Offset(1234), nil)`; `NextOffset` returns `1234`, `err` nil, and `saramaClient.GetOffsetCallCount()` is `0`.
2. **Store miss seeks at `now − TTL` in Unix milliseconds.** `GetReturns(0, libkv.KeyNotFoundError)`, `GetOffsetReturns(5000, nil)`; `NextOffset` returns `5000`; `GetOffsetCallCount()` is `1`; and from `GetOffsetArgsForCall(0)`: the topic equals `topic.String()`, the partition equals `partition.Int32()`, and the timestamp equals `fixedNow.Add(-stdtime.Hour).UnixMilli()`. Compute the expectation from the `stdtime.Time` literal, not through the `libtime` path, so a sign flip in the implementation cannot be mirrored by the assertion.
3. **Bucket miss also seeks.** Same as 2 but `GetReturns(0, libkv.BucketNotFoundError)` — `GetOffsetCallCount()` is `1`.
4. **The resolved offset is persisted before it is returned.** Map-backed store, `GetOffsetReturns(5000, nil)`; first manager's `NextOffset` returns `5000`; then a **second** `NewTTLSeekOffsetManager` over the same store returns `5000` with `GetOffsetCallCount()` still `1`. Also assert `offsetStore.SetArgsForCall(0)` carries `Offset(5000)`.
5. **A failed persist returns an error and does not return the offset.** Map-backed store, `GetOffsetReturns(5000, nil)`, and in the `It` (after the `BeforeEach` stubs) `offsetStore.SetReturns(stderrors.New("disk full"))` — this is the one place where mixing is intended: `SetReturns` nulls `SetStub`, so the write fails and persists nothing while `Get` keeps reading the map: `NextOffset` returns a non-nil error and `libkafka.Offset(0)`. Then a second manager over the same store seeks again — `GetOffsetCallCount()` is `2`.
6. **A broker seek failure is an error, not a sentinel.** `GetReturns(0, libkv.KeyNotFoundError)`, `GetOffsetReturns(0, stderrors.New("broker unreachable"))`: `NextOffset` returns a non-nil error and `libkafka.Offset(0)`. Assert the returned offset is not `libkafka.OffsetNewest` and not `libkafka.OffsetOldest`.
7. **A non-miss store error is an error and never reaches the broker.** `GetReturns(0, stderrors.New("disk full"))`: non-nil error, `libkafka.Offset(0)`, `GetOffsetCallCount()` is `0`.
8. **A negative broker result resolves to the newest sentinel and persists nothing.** `GetReturns(0, libkv.KeyNotFoundError)`, `GetOffsetReturns(-2, nil)`: returns `libkafka.OffsetNewest`, no error, and `offsetStore.SetCallCount()` is `0`.
9. **Both sentinels report newest.** `InitialOffset()` and `FallbackOffset()` both equal `libkafka.OffsetNewest`.
10. **Delegation and close.** Map-backed store: `MarkOffset(ctx, topic, partition, libkafka.Offset(700))` then a second manager over the same store returns `700`; `ResetOffset(ctx, topic, partition, libkafka.Offset(500))` then a third manager returns `500` (backward movement works); `Close()` returns nil; a subsequent `NextOffset` returns a non-nil error with `stderrors.Is(err, libkafka.ErrClosed)` true — `stderrors` is already imported for `stderrors.New`, and `github.com/bborbe/errors.Is` is a thin wrapper over it, so do not also import `github.com/bborbe/errors` unless you actually use it.

Existing test files must keep compiling and passing unchanged.

### 8. `docs/offset-managers.md`

A short reference page (roughly 40–60 lines). Start it with `# Offset Managers` — the same heading level `docs/dod.md` uses. It exists because the spec is archived after implementation and the follow-up wiring lives in `bborbe/agent-task-controller`. Contents:

- **What it does** — one paragraph: `NewTTLSeekOffsetManager` starts a partition with no committed offset at the oldest message younger than the TTL; a partition with a stored offset is unaffected.
- **The replay arithmetic** — the measured numbers from the spec (about 68,000 messages over six days, 17.85 messages/second, about 63 minutes before the first live message) and why discarding them on arrival is pure cost.
- **Store miss vs store outage** — only `libkv.KeyNotFoundError` and `libkv.BucketNotFoundError` mean "no offset yet"; every other store error is returned. Naming the identifiers is fine here (the grep restriction applies only to the Go implementation file).
- **The `FallbackOffset` trap** — `InitialOffset()` and `FallbackOffset()` both return `OffsetNewest` because `FallbackOffset()` is read by four recovery paths in `kafka_consumer-offset.go`: the per-partition start in `Consume`, `skipAndAdvance`, `recoverFromConsumeError`, and `resetOnOffsetOutOfRange`. Returning `OffsetOldest` there would make an out-of-range reset replay the whole retained window.
- **Operational notes** — two bullets: a TTL longer than the topic's retention degrades to a bounded replay of the retained window (shorten the TTL); a consumer clock ahead of the broker by more than the TTL skips live messages permanently and silently (check NTP on the node).

Link the existing sibling constructors (`NewStoreOffsetManager`, `NewSaramaOffsetManager`, `NewSimpleOffsetManager`) by name so the page reads as the offset-manager overview, not a page about one type.

### 9. `CHANGELOG.md`

Add the bullet to the existing `## Unreleased` section if `CHANGELOG.md` already has one — an approved sibling prompt for spec 005 adds it first, so do NOT create a second `## Unreleased` heading. Only when no `## Unreleased` section exists, create one after the SemVer preamble bullets and directly above the highest existing `## vX.Y.Z` heading. Never move, delete, or insert anything inside the `# Changelog` header block. One bullet, conventional prefix:

```
- feat: Add opt-in TTL-seek offset manager that starts a partition with no committed offset at the oldest message younger than a caller-supplied TTL
```

### 10. Self-check

Before finishing: re-run `<verification>` and confirm every command passes; then walk each Acceptance Criterion in `specs/in-progress/004-ttl-seek-initial-offset.md` against the change and name, for each, the code branch plus the test spec that proves it. Skip the "files unmodified" AC — this container has no git; it is verified at spec acceptance on the operator side.
</requirements>

<constraints>
- Default behaviour is frozen: no existing constructor, default, or exported signature changes. A consumer that does not construct this type must be observably identical.
- Do NOT change `CreateSaramaConfig`, `config.Consumer.Offsets.Initial`, or any sarama default.
- Do NOT change `NewStoreOffsetManager`, `NewSaramaOffsetManager`, or `NewSimpleOffsetManager`, or their initial-offset semantics.
- Do NOT change `kafka_consumer-offset.go` — not `skipAndAdvance`, not `resetOnOffsetOutOfRange`, not `recoverFromConsumeError`, not the creation-time out-of-range fallback. This manager only supplies different values to them.
- Do NOT touch `kafka_offset-manager-store.go`, `kafka_offset-manager-sarama.go`, `kafka_offset-manager-simple.go`, `kafka_offset-manager.go`, `kafka_consumer-offset.go`, or `kafka_sarama-config.go`. The "files unmodified" acceptance criterion is checked at spec acceptance on the operator side; this container has no git.
- Do NOT add a batch-level message filter — that is spec `005-pre-tx-message-filter.md`.
- Do NOT wire this into any consuming service, and do not add a runtime or cluster verification step. Wiring belongs to a follow-up prompt in `bborbe/agent-task-controller` after this library is tagged.
- Do NOT add a scenario. The behaviour is reachable by unit tests with an injected client and store.
- The manager must not import or depend on `bborbe/cqrs`, a command schema, or any expiry concept. It knows only a `libtime.Duration`.
- Do NOT add a log line (`glog`) and do NOT add a Prometheus metric on any path. Every failure mode is reported through the returned error; the caller logs it.
- Do NOT add a configurable knob beyond the four constructor parameters — no retry count, no fallback sentinel option, no disable switch.
- Tests follow `docs/dod.md`: Ginkgo/Gomega in `package kafka_test`, using the existing counterfeiter mocks in `mocks/`. Do not add a second `RunSpecs` bootstrap — reuse `kafka_suite_test.go`.
- The new file follows the repo's flat naming (`kafka_offset-manager-ttl-seek.go`), carries the BSD license header, and has GoDoc on every exported symbol.
- `kafka_offset-manager-ttl-seek.go` must contain zero occurrences of the identifier `OffsetOldest` — including inside comments and GoDoc.
- Do NOT commit — dark-factory handles git.
- Repo-relative paths only; the container mounts the repo at `/workspace`.
- Existing tests must still pass; `make precommit` must pass.
</constraints>

<verification>
`.git` is masked in the container (`hideGit=true`), so no `git` or `docker` command may appear here. `ROOTDIR=/workspace` is passed for consistency with other repos; this repo's `Makefile` includes only `tools.env` and does not read `ROOTDIR`, so it is a harmless no-op.

- `ROOTDIR=/workspace make precommit` — must pass (runs ensure, format, generate, test, check, addlicense).
- `go test -mod=mod -count=1 ./...` — must pass (full suite, exit 0).
- `go test -mod=mod -count=1 . -ginkgo.focus='TTLSeekOffsetManager'` — must pass, and must report at least 10 specs ran.
- `grep -n 'NewTTLSeekOffsetManager' kafka_offset-manager-ttl-seek.go` — must print at least one line.
- `! grep -q 'OffsetOldest' kafka_offset-manager-ttl-seek.go` — must succeed (zero occurrences in the implementation file).
- `grep -n 'GetOffset' kafka_offset-manager-ttl-seek.go` — must print at least one line (the broker seek is really there).
- `grep -n 'UnixMilli' kafka_offset-manager-ttl-seek.go` — must print at least one line (the timestamp conversion is really there).
- `! grep -q 'glog' kafka_offset-manager-ttl-seek.go` — must succeed (no logging on this path).
- `test -f docs/offset-managers.md` — must succeed.
- `grep -n 'FallbackOffset' docs/offset-managers.md` — must print at least one line (the trap section is really present).
- `test "$(grep -c '^## Unreleased' CHANGELOG.md)" -eq 1` — must succeed (exactly one `## Unreleased` heading; `grep -n` exits 0 even when a duplicate exists, so the count is the real assertion).
- `awk '/^## /{sec=$0} /ttl-seek offset manager/{print sec}' CHANGELOG.md` — must print `## Unreleased` (the new bullet sits under that section, not folded into a released one).
</verification>
