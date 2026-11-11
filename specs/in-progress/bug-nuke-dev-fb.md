---
status: approved
approved: 2026-11-11T22:52:30.092061285Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-FB` is a real bug in `bborbe/kafka` at `bborbe/kafka/kafka_consumer-offset.go:79`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

IsTransientBrokerError (bborbe/kafka/kafka_consumer-offset.go:79) omits syscall.EPIPE, so a sarama ConsumerError caused by a broken-pipe broker write is never marked transient, bypassing bborbe/service's default Transient() Sentry exclusion and aborting the partition (consume failed -> run failed -> pod restart).

## Reproduction

The implicated source location from the deep verdict is `bborbe/kafka/kafka_consumer-offset.go:79` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-DEV-FB`.

## Recommended Fix

Add errors.Is(cause, syscall.EPIPE) to IsTransientBrokerError with EPIPE test rows in bborbe/kafka, then bump github.com/bborbe/kafka in the trading Kafka modules; handle bounded partition-consumer recovery as a separate spec.
