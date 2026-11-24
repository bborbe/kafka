---
status: approved
approved: 2026-11-24T23:58:23.114134685Z
kind: bug
---

## Summary

Sentry issue `NUKE-PROD-AW` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go:79`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

IsTransientBrokerError (bborbe/kafka kafka_consumer-offset.go:79-90) omits timeout/deadline errors, so a broker read i/o timeout from 192.168.178.41:32706 is treated as fatal and aborts every partition via run.CancelOnFirstError instead of being suppressed as a transient Sentry event.

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go:79` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-PROD-AW`.

## Recommended Fix

Add net.Error.Timeout()/os.ErrDeadlineExceeded/ETIMEDOUT cases to IsTransientBrokerError with test rows, cut a patch release, and bump github.com/bborbe/kafka in the k8s-apply consumer; investigate broker slowness separately.
