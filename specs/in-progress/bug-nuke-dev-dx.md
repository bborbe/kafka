---
status: approved
approved: 2026-11-25T01:56:53.323666904Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-DX` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go:79`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

In bborbe/kafka, IsTransientBrokerError (kafka_consumer-offset.go:79) omits net.Error.Timeout()/os.IsTimeout, so a broker TCP read timeout is classified fatal and escalates "consume failed" -> "run failed", aborting the consumer and restarting the pod; confirmed absent from master HEAD e7224f2.

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go:79` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-DEV-DX`.

## Recommended Fix

Add a net.Error.Timeout()/os.IsTimeout predicate to IsTransientBrokerError with table-test rows for poll.ErrTimeout/os.ErrDeadlineExceeded (keeping the rebalance-timeout negative row), then bump bborbe/kafka in the consuming services; partition-level recovery is a separate follow-up.
