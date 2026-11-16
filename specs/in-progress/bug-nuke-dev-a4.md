---
status: approved
approved: 2026-11-16T16:15:18.513845876Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-A4` is a real bug in `bborbe/kafka` at `kafka_highwatermarks.go:50`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

createTriggerOffsets/HighWaterMarks loops every partition with no retry, so a single sarama ErrNotLeaderForPartition during a broker restart (topics use default.replication.factor 1) aborts startup and the core-news pod exits.

## Reproduction

The implicated source location from the deep verdict is `kafka_highwatermarks.go:50` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-DEV-A4`.

## Recommended Fix

Retry the startup high-water-mark/trigger-offset computation with bounded backoff on transient broker errors (add sarama.ErrNotLeaderForPartition to IsTransientBrokerError and reuse it on this path), or return a Transient()-marked error so the runner retries; optionally raise replication factor for core-* topics as a follow-up.
