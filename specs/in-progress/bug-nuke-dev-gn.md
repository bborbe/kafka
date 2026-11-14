---
status: approved
approved: 2026-11-14T16:27:19.136531334Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-GN` is a real bug in `bborbe/kafka` at `bborbe/kafka/kafka_consumer-offset.go:568`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

Broker-side CRC corruption of record batches in partition 0 of develop-agent-task-v1-event; bborbe/kafka treats a sarama.PacketDecodingError as fatal unless SkipCorruptBatches is set (default false), and CreateTaskView's non-group simple consumer replays from offset 0 on every restart, so the executor re-reads offsets 980,929-985,739 and crash-loops.

## Reproduction

The implicated source location from the deep verdict is `bborbe/kafka/kafka_consumer-offset.go:568` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-DEV-GN`.

## Recommended Fix

Set SKIP_CRC_CORRUPTION=true on the nuke-dev agent-task-executor deployment to step over the corrupt range, then repair/rewrite partition 0 broker-side or give the task-view reader a durable committed position so it stops re-walking from offset 0.
