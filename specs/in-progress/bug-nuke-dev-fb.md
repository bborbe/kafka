---
status: prompted
approved: "2026-11-19T01:28:36.663994323Z"
generating: "2026-11-19T02:08:29Z"
prompted: "2026-11-19T02:12:52Z"
---

## Summary

Sentry issue `NUKE-DEV-FB` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go:79`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

bborbe/kafka's IsTransientBrokerError (kafka_consumer-offset.go:79 in pinned v1.29.10) omits syscall.EPIPE, so a broken-pipe Sarama ConsumerError is not wrapped as transientError, escapes bborbe/service's Sentry exclusion, and aborts the partition ("parition consumer returns error -> consume failed -> run failed").

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go:79` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-DEV-FB`.

## Recommended Fix

Bump github.com/bborbe/kafka from v1.29.10 to v1.30.1 (or latest v1.30.x) in every bborbe/trading Kafka-consuming module, run go mod tidy per module, and redeploy the candle consumers.
