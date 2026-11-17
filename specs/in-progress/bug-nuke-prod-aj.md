---
status: approved
approved: 2026-10-28T13:05:19.818943386Z
kind: bug
---

## Summary

Sentry issue `NUKE-PROD-AJ` is a real bug in `bborbe/kafka` at `kafka_consumer-offset.go:572`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

offsetConsumer.consumeMessages in bborbe/kafka v1.28.1 (line 536) turns a normal closed sarama PartitionConsumer channel into a fresh errors.Errorf; Consume.func1 wraps it as "consume failed" and run.CancelOnFirstError escalates it into a whole-consumer abort — a regression introduced by 6891889 (first released v1.22.15), fixed upstream by 6d79267 in v1.28.2.

## Reproduction

The implicated source location from the deep verdict is `kafka_consumer-offset.go:572` in `bborbe/kafka` (resolution rule: `candidate position 2 (bborbe/kafka)`), Sentry issue `NUKE-PROD-AJ`.

## Recommended Fix

Bump the nuke-prod consumer service's github.com/bborbe/kafka from v1.28.1 to >= v1.28.2 (latest v1.29.2) in go.mod/go.sum; the fix is internal to offsetConsumer with no call-site or exported-API change.
