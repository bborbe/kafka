---
status: draft
---

# Mark Kafka network timeouts as transient broker errors

<summary>
- A Kafka consumer that loses its broker to a network timeout stops reporting that timeout to Sentry
- The timeout is recognised as the same class of condition as a refused or reset connection
- Existing error classifications are unchanged — only the timeout case is added
- No caller changes, and no configuration is introduced
- The behaviour is proven by extending the existing transient-marker test table
- Genuine failures such as rebalance timeouts and auth errors are still reported
</summary>

<objective>
Treat a broker network timeout (`i/o timeout`, `connection timed out`) as a transient broker-unreachable error, so that `bborbe/service`'s default Sentry exclusion suppresses it. Today an unreachable broker produces one captured Sentry event per consumer retry, so a single infrastructure fault floods the error budget that exists to carry real incidents.
</objective>

<context>
Read `docs/dod.md` for the definition of done; read `CLAUDE.md` if present for project conventions.

Read `kafka_consumer-offset.go`:
- `IsTransientBrokerError` — the predicate that classifies a wrapped cause as transient. It already computes `cause := errors.Cause(err)` and guards `err == nil`.
- `transientError` — the wrapper carrying `Transient() bool` for downstream reporters.
- `handleConsumerError` — the single choke point that calls `IsTransientBrokerError` and wraps on a match. Both consume paths go through it.

Read `kafka_consumer-offset_internal_test.go`:
- `TestConsume_TransientBrokerErrorSatisfiesMarker` — the table test covering every classification, positive and negative.
- `newTransientTestConsumer`, `newConsumerError` and `hasTransientMarker` — the harness the table drives.

Downstream contract, for understanding only — do not edit: `bborbe/service` excludes from Sentry any error whose chain contains a value implementing `Transient() bool` that returns true. This predicate is what marks it.
</context>

<requirements>
1. Add `net` to the import block of `kafka_consumer-offset.go` — it is not currently imported.
2. Extend `IsTransientBrokerError` so it also returns true when `cause` is a network timeout — a leading `if` before the existing `return` is fine, since `errors.As` needs a target variable. Match when `cause` satisfies `net.Error` and its `Timeout()` returns true. That single case covers both shapes the objective names: `read tcp <src>-><dst>: i/o timeout` (a `*net.OpError` whose `Err` is `os.ErrDeadlineExceeded`) and `connection timed out` (whose `Err` is `os.NewSyscallError("read", syscall.ETIMEDOUT)`).
   Keep the `nil` guard and all six existing cases exactly as they are. Extend the doc comment above `IsTransientBrokerError` to name network timeouts as a transient class, so the comment does not drift from the code.
3. In `kafka_consumer-offset_internal_test.go`, add two rows to the existing `tests` slice in `TestConsume_TransientBrokerErrorSatisfiesMarker`, both expecting `transient: true`:
   - name `io timeout`, err `&net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}`
   - name `connection timed out`, err `&net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ETIMEDOUT)}`
   Leave every existing row and expectation untouched.
4. Confirm the negative rows still return false. In particular the existing `rebalance timeout` row must remain non-transient — a rebalance timeout is an application-level condition with its own handling and must not be folded into the broker-unreachable class. If adding the timeout case breaks it, the case is too broad; narrow it rather than changing that row.
5. Before finishing, re-run `<verification>` and confirm it passes, then walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change `handleConsumerError`, `transientError`, or any file other than `kafka_consumer-offset.go` and its internal test.
- Do NOT alter the six existing transient cases, or any existing test row or expectation.
- Do NOT classify a rebalance timeout, auth failure, offset-out-of-range or corruption as transient.
- Do NOT edit `CHANGELOG.md` — the release entry is operator-owned on this repo (`autoRelease: false`), so the changelog rule in `CLAUDE.md` does not apply to this prompt.
- Existing tests must still pass.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` -- must pass.
</verification>
