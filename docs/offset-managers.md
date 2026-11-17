# Offset Managers

`NewTTLSeekOffsetManager` starts a partition that has no committed offset at the oldest message
younger than a caller-supplied TTL, instead of at the oldest retained message. A partition that
already has a stored offset is unaffected: the stored value is returned straight from the store
and the broker is never contacted.

The existing constructors — `NewStoreOffsetManager`, `NewSaramaOffsetManager`, and
`NewSimpleOffsetManager` — are unchanged and remain the default choice. This page describes the
opt-in TTL-seek manager alongside them.

## The replay arithmetic

A topic holding roughly 68,000 messages across six days costs about 63 minutes of replay at the
measured 17.85 messages/second before the first live message is reached. Every replayed message
older than the consumer's TTL is discarded on arrival, so that hour of work produces nothing. The
TTL-seek manager seeks past that window: it asks the broker for the first offset at or after
`now − TTL` and starts there.

## Store miss vs store outage

Only `libkv.KeyNotFoundError` and `libkv.BucketNotFoundError` mean "this partition has no offset
yet". Every other store error is returned to the caller. Treating a store outage as a miss would
silently turn a broken database into a full replay, so the distinction is deliberate.

The resolved offset is persisted through the store before it is returned. A restart therefore
resumes where the previous process left off rather than seeking again.

## The `FallbackOffset` trap

`InitialOffset()` and `FallbackOffset()` both return `OffsetNewest`. `FallbackOffset()` is read by
four recovery paths in `kafka_consumer-offset.go`: the per-partition start in `Consume`,
`skipAndAdvance`, `recoverFromConsumeError`, and `resetOnOffsetOutOfRange`. Returning
`OffsetOldest` there would make an out-of-range reset replay the whole retained window, which is
exactly the replay this manager exists to remove. `InitialOffset()` is not read by any consumer;
it reports the newest offset because the true initial offset is time-dependent and cannot be
expressed as a constant.

## Operational notes

- A TTL longer than the topic's retention degrades to a bounded replay of the retained window.
  Shorten the TTL to move the start offset forward.
- A consumer clock ahead of the broker by more than the TTL skips live messages permanently and
  silently. Check NTP on the node if messages appear to be missing.
