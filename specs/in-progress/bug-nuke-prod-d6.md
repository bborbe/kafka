---
status: approved
approved: 2026-11-25T02:00:41.444937251Z
kind: bug
---

## Summary

Sentry issue `NUKE-PROD-D6` is a real bug in `bborbe/kafka` at `bborbe/kafka/kafka_consumer-offset.go:79`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

IsTransientBrokerError (bborbe/kafka kafka_consumer-offset.go:79) uses a narrow positive cause list (EOF/ECONNREFUSED/ECONNRESET/EPIPE/ErrBrokerNotAvailable) that omits TCP read-deadline timeouts, so a transient 'read tcp ...: i/o timeout' from the partition consumer is not wrapped as transient, aborts the run, and pages Sentry.

## Reproduction

The implicated source location from the deep verdict is `bborbe/kafka/kafka_consumer-offset.go:79` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-PROD-D6`.

## Recommended Fix

Add os.ErrDeadlineExceeded, syscall.ETIMEDOUT and/or a net.Error.Timeout() check to IsTransientBrokerError's positive list, extend TestConsume_TransientBrokerErrorSatisfiesMarker with a *net.OpError deadline row (keeping rebalance-timeout => false), then bump and roll the module into the trading consumers.
