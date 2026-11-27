---
status: approved
approved: 2026-11-27T05:54:15.091160451Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-GN` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go:569`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

Broker-side CRC corruption of record batches in partition 0 of develop-agent-task-v1-event is fatal by default (SkipCorruptBatches=false at kafka_consumer-offset.go:569/:603), and agent-task-executor's non-group simple consumer replays from offset 0 without committing, so every restart re-reads the corrupt range and crash-loops.

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go:569` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-DEV-GN`.

## Recommended Fix

Set SKIP_CRC_CORRUPTION=true on the nuke-dev agent-task-executor deployment (verified opt-in path at kafka_consumer-offset.go:569/:603, persisted skip via skipAndAdvance), repair/rewrite partition 0 broker-side or wire a durable OffsetManager into CreateTaskView, then mark NUKE-DEV-GN resolved in Sentry.
