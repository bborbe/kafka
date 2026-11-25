---
status: approved
approved: 2026-11-25T02:09:17.974555689Z
kind: bug
---

## Summary

Sentry issue `NUKE-PROD-D4` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go:79`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

bborbe/kafka IsTransientBrokerError (kafka_consumer-offset.go:79-90) omits network read timeouts (os.ErrDeadlineExceeded / ETIMEDOUT / net.Error.Timeout), so the transient Sarama ConsumerError is not marked transient, escapes bborbe/service's Sentry exclusion, and cancels the consumer run.

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go:79` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-PROD-D4`.

## Recommended Fix

Add timeout causes (os.ErrDeadlineExceeded, syscall.ETIMEDOUT, net.Error.Timeout()) to IsTransientBrokerError with table-test rows, release a bborbe/kafka patch (v1.30.3), and bump github.com/bborbe/kafka in bborbe/agent-task-executor and other consumers.
