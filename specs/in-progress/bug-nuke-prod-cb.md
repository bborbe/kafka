---
status: verifying
approved: "2026-10-19T15:58:59.763060582Z"
generating: "2026-10-20T18:33:11Z"
prompted: "2026-10-20T18:35:42Z"
verifying: "2026-10-20T18:56:03Z"
---

## Summary

Sentry issue `NUKE-PROD-CB` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go (skipAndAdvance, resetOnOffsetOutOfRange)`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

skipAndAdvance and resetOnOffsetOutOfRange in bborbe/kafka call ConsumePartition/CreatePartitionConsumer before closing the old partition consumer, violating Sarama's one-consumer-per-partition rule

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go (skipAndAdvance, resetOnOffsetOutOfRange)` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-PROD-CB`.

## Recommended Fix

Deploy bborbe/kafka v1.28.1 to production — update bborbe/trading or bborbe/agent-task-executor dependency to v1.28.1 (or latest)
