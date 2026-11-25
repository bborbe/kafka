---
status: approved
approved: 2026-11-25T01:54:54.085543005Z
kind: bug
---

## Summary

Sentry issue `NUKE-PROD-D5` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go:79`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

IsTransientBrokerError (kafka_consumer-offset.go:79) omits os.ErrDeadlineExceeded and syscall.ETIMEDOUT, so a transient broker read i/o timeout is classified fatal, bypasses the Transient() Sentry exclusion, and aborts the consumer.

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go:79` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-PROD-D5`.

## Recommended Fix

Add errors.Is(cause, os.ErrDeadlineExceeded) and errors.Is(cause, syscall.ETIMEDOUT) to IsTransientBrokerError with test rows, release a patch, and bump the services consuming master-raw-forexfactory-detail-input.
