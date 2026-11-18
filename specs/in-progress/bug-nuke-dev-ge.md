---
status: approved
approved: 2026-11-16T15:48:47.030691249Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-GE` is a real bug in `bborbe/kafka` at `bborbe/kafka/kafka_consumer-offset-highwater-marks.go:106`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

createTriggerOffsets -> HighWaterMarks -> sarama GetOffset returns KError 6 (ErrNotLeader) during a partition leader election, and the startup path has no retry/backoff and does not classify leader-election errors as transient, so one transient broker condition becomes a fatal service start failure.

## Reproduction

The implicated source location from the deep verdict is `bborbe/kafka/kafka_consumer-offset-highwater-marks.go:106` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-DEV-GE`.

## Recommended Fix

In bborbe/kafka, add ErrNotLeader/ErrLeaderNotAvailable to IsTransientBrokerError and wrap the HighWaterMarks call in createTriggerOffsets with a bounded, context-aware retry/backoff, keeping non-transient errors fatal.
