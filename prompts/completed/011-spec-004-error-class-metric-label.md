---
status: completed
spec: [004-kafka-decode-error-attribution]
summary: Added the error_class label to kafka_consumer_error_counter, extended MetricsConsumer.ErrorCounterInc with a typed ErrorClass parameter fed by ClassifyError, regenerated the counterfeiter mock, updated the in-package fake, and added internal tests for both label directions and the registered label set.
execution_id: kafka-exec-011-spec-004-error-class-metric-label
dark-factory-version: v0.199.1
created: "2026-11-16T11:20:24Z"
queued: "2026-11-16T11:29:35Z"
started: "2026-11-17T01:08:03Z"
completed: "2026-11-17T01:10:43Z"
---

# Add the `error_class` label to the consumer error metric

<summary>
- The consumer error metric splits into three series instead of one, so a corrupt batch and a broker-side leader or coordination error are no longer summed into the same number
- The label values are exactly the three classes the classifier already produces, so the series set stays bounded
- The label the metric emits is derived by the same function a caller can test, so the two cannot drift apart
- An operator query can tell a corruption burst from a broker-side burst without reading error text
- The metric stays the gauge it is today, with its name unchanged — only a label is added
- No offset, message key, timestamp or other unbounded value reaches a Prometheus label
- Every in-repo implementer of the consumer metrics interface is updated, including the generated mock
</summary>

<objective>
Carry the error class into the consumer error metric: `MetricsConsumer.ErrorCounterInc` gains an `ErrorClass` parameter, `kafka_consumer_error_counter` gains an `error_class` label, and the consumer error handler feeds it from the classifier so the class a caller can test and the class the metric emits are the same function.
</objective>

<context>
Read `CLAUDE.md` for project conventions and `docs/dod.md` for the definition of done.

Read these files before changing anything:
- `kafka_metrics.go` — the `MetricsConsumer` interface (`ErrorCounterInc(topic Topic, partition Partition)`), the package-level `consumerErrorCounter` gauge vector with its `[]string{"topic", "partition"}` label set, the `init()` registration, and the `metrics` implementation's `With(prometheus.Labels{…})` style.
- `kafka_consumer-error-handler.go` — `NewConsumerErrorHandler`, which is the only caller of `ErrorCounterInc`.
- `kafka_consumer-error-class.go` — `ErrorClass`, `AvailableErrorClasses`, and `ClassifyError`, added by the sibling prompt `1-spec-004-error-classifier-and-offset.md`. This prompt consumes them; it does not re-derive them.
- `mocks/kafka-metrics.go` — the counterfeiter-generated fake for `Metrics`. It ends with `var _ kafka.Metrics = new(KafkaMetrics)`, so a stale mock breaks compilation.
- `kafka_consumer-offset_internal_test.go` — the hand-written `fakeMetricsConsumer`, which must gain the third parameter.
- `Makefile` — the `generate` target (`rm -rf mocks avro`, then `go generate -mod=mod ./...`); `make precommit` runs it.

Coding-plugin guides (in-container paths): `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-enum-type-pattern.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`.

Spec (read in full, it is authoritative): `specs/in-progress/004-kafka-decode-error-attribution.md`. This prompt covers its Desired Behavior 3 and 4, and Acceptance Criteria 2 and 5. Acceptance Criterion 5's membership half (every classifier output is in the exported enumeration) is already covered by the sibling prompt's classifier test; the registered-label-set half is covered here.

<requirements>

### 1. `error_class` label on the metric vector

In `kafka_metrics.go`, extend the `consumerErrorCounter` gauge vector's label set to exactly three names, in this order:

```go
	consumerErrorCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "consumer",
		Name:      "error_counter",
		Help:      "Kafka Consumer Error Counter",
	}, []string{"topic", "partition", "error_class"})
```

Keep it a `GaugeVec` with the same namespace, subsystem and name. The spec's Non-goals forbid migrating this metric to a counter, renaming it, or touching `consumerCorruptBatchSkippedCounter` — the `go-prometheus/no-gauge-for-monotonic` rule is deliberately not applied here and that decision is recorded in the spec.

### 2. Interface and implementation signature

Change the `MetricsConsumer` method:

```go
	ErrorCounterInc(topic Topic, partition Partition, errorClass ErrorClass)
```

and the `metrics` implementation to write the class as a third label:

```go
func (m *metrics) ErrorCounterInc(topic Topic, partition Partition, errorClass ErrorClass) {
	consumerErrorCounter.With(prometheus.Labels{
		"topic":       topic.String(),
		"partition":   partition.String(),
		"error_class": errorClass.String(),
	}).Inc()
}
```

The parameter is the typed `ErrorClass`, not a `string`: the type is what keeps an arbitrary value out of the label. Never pass an error message, an error text, or an offset into this label.

`MetricsConsumer` is public, so this is a breaking interface change. That is the accepted blast radius recorded in the spec. The in-repo implementers are exactly three: `metrics` in `kafka_metrics.go`, the generated `mocks/kafka-metrics.go`, and the hand-written `fakeMetricsConsumer` in `kafka_consumer-offset_internal_test.go`. If the compiler names a fourth, update it the same way rather than reverting the signature.

### 3. Regenerate the counterfeiter mock

Run `make generate`. Do NOT hand-edit `mocks/kafka-metrics.go` — the `generate` target deletes and regenerates the whole `mocks/` directory. After regeneration, `ErrorCounterInc` must take three arguments (`arg1 kafka.Topic, arg2 kafka.Partition, arg3 kafka.ErrorClass`) and the `var _ kafka.Metrics = new(KafkaMetrics)` assertion must still compile.

### 4. Update the in-package fake

In `kafka_consumer-offset_internal_test.go`, change `fakeMetricsConsumer.ErrorCounterInc` to accept the third parameter: `func (f *fakeMetricsConsumer) ErrorCounterInc(Topic, Partition, ErrorClass) {}`. This is a signature-only change; the fake records nothing for this method today and must keep recording nothing.

### 5. Wire the handler to the classifier

In `kafka_consumer-error-handler.go`, pass the class derived from the same function a caller can test:

```go
	return ConsumerErrorHandlerFunc(func(err *sarama.ConsumerError) error {
		metricsConsumer.ErrorCounterInc(
			Topic(err.Topic),
			Partition(err.Partition),
			ClassifyError(err),
		)
		return err
	})
```

`*sarama.ConsumerError` unwraps to its `Err` field, so `ClassifyError` sees the underlying cause. Do not add a second classifier, a switch on the error text, or a class parameter to `ConsumerErrorHandler` — the handler has no way to receive one from a caller.

### 6. Tests — new file `kafka_metrics_internal_test.go` (`package kafka`, plain `testing.T`)

The internal package is required: the assertions read the package-level `consumerErrorCounter` vector directly, which `package kafka_test` cannot reach. Do not use Ginkgo here (one `RunSpecs` per binary, already taken by `kafka_suite_test.go`).

a) `TestErrorCounterInc_ErrorClassLabel` — both directions, through the **real** metrics implementation and the real handler, using `prometheus/testutil`:

```go
	handler := NewConsumerErrorHandler(NewMetrics())
```

Drive two cases, each on its own `topic`/`partition` pair, because the vectors are package-level and shared and a reused pair makes the negative assertion flaky:

- Broker-side: `if err := handler.HandleError(&sarama.ConsumerError{Topic: "test-error-class-broker", Partition: 11, Err: sarama.ErrNotLeaderForPartition}); err != nil { t.Fatalf("unexpected handler error: %v", err) }` (`errcheck` runs in `make precommit`, so never discard the returned error), then assert `testutil.ToFloat64(consumerErrorCounter.With(prometheus.Labels{"topic": "test-error-class-broker", "partition": "11", "error_class": ErrorClassBrokerSide.String()}))` is `1`, and that the same `topic`/`partition` with `error_class: ErrorClassCorruption.String()` is `0`.
- Corruption: the same call with `&sarama.ConsumerError{Topic: "test-error-class-corrupt", Partition: 12, Err: sarama.PacketDecodingError{Info: "CRC mismatch"}}`, then assert `error_class="corruption"` is `1` and `error_class="broker_side"` is `0` for that pair.

Both directions are positive assertions, so an implementation that only ever emits `corruption` and `other` fails. As the Desired Behavior 4 evidence, make the same assertion through the classifier instead of a literal: compute `class := ClassifyError(<the same error value>)` and assert `testutil.ToFloat64(consumerErrorCounter.With(prometheus.Labels{"topic": …, "partition": …, "error_class": class.String()}))` is `1`. A handler that hardcodes a class while `ClassifyError` returns a different one fails that comparison.

Do not use `t.Parallel()`: the gauge children are shared across the whole test binary.

b) `TestConsumerErrorCounterLabelSet` — Acceptance Criterion 5: no unbounded label, and the registered label set is exactly `topic`, `partition`, `error_class`. Call `NewMetrics().ErrorCounterInc(Topic("test-label-set"), Partition(1), ErrorClassOther)` so the family has at least one child, then gather from the real registry and read the union of label names:

```go
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics failed: %v", err)
	}
	seen := map[string]bool{}
	for _, family := range families {
		if family.GetName() != "kafka_consumer_error_counter" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				seen[label.GetName()] = true
			}
		}
	}
	expected := []string{"error_class", "partition", "topic"}
	if names := slices.Sorted(maps.Keys(seen)); !slices.Equal(names, expected) {
		t.Errorf("expected labels %v, got %v", expected, names)
	}
```

Import `maps`, `slices`, `github.com/prometheus/client_golang/prometheus` and `github.com/prometheus/client_golang/prometheus/testutil`. The dto types come from the `Gather()` return value, so no `client_model` import is needed.

c) Do not add a test that asserts the class values as strings against a hand-written list — `AvailableErrorClasses` is the enumeration, and the classifier test in the sibling prompt already asserts membership.

### 7. Final self-check

Before finishing: re-run `<verification>` and confirm it passes; then walk Desired Behavior 3 and 4 and Acceptance Criteria 2 and 5 of `specs/in-progress/004-kafka-decode-error-attribution.md` against the change.

</requirements>

<constraints>
- The `error_class` values are exactly `corruption`, `broker_side`, `other`. Do not add a fourth value, a fallback string, or a pass-through of the error text.
- No label may carry an offset, a message key, a timestamp or any other unbounded value. The offset stays out of Prometheus deliberately; it reaches operators as a Sentry tag from the error context data.
- Keep `consumerErrorCounter` a `GaugeVec` with the same metric name. Migrating it to a counter, renaming it, or touching `consumerCorruptBatchSkippedCounter` are explicit spec Non-goals.
- The class the metric emits and the class a caller can test must be the same function (`ClassifyError`). No duplicated classification logic in the handler or in the metrics implementation.
- Regenerate `mocks/kafka-metrics.go` with `make generate`; do not hand-edit generated files.
- Do not change the metric's `Help` string, its namespace, or its subsystem.
- Do not change the error message substrings `parition consumer returns error`, `consume failed` and `run failed`, and do not re-add a log line to the handler.
- Tests in the internal package use plain `testing.T`, not Ginkgo.
- Do not add a new module dependency; `prometheus/testutil` ships with `github.com/prometheus/client_golang`, already a direct dependency. If `go mod tidy` promotes a previously indirect module to direct, keep that change rather than reverting it.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `make precommit` must pass.
</constraints>

<verification>
Run `make precommit` — must pass (this runs ensure, format, generate, test, check and addlicense, and it regenerates `mocks/`).

Then run the targeted checks; each must pass:

- `go test -mod=mod -run 'TestErrorCounterInc_ErrorClassLabel|TestConsumerErrorCounterLabelSet' ./...` — the named evidence tests must exist and pass (without this line a renamed test makes the filter match nothing and exit 0).
- `grep -q 'error_class' kafka_metrics.go` — the label declaration and the label write are both in the metric file.
- `grep -q 'arg3 kafka.ErrorClass' mocks/kafka-metrics.go` — the generated mock carries the third parameter (matched on the parameter, not on the whole signature, because `golines` may wrap it).
- `grep -q 'ClassifyError' kafka_consumer-error-handler.go` — the handler feeds the metric from the classifier.
- `grep -q 'ErrorClassBrokerSide' kafka_consumer-error-class.go` — the `broker_side` value exists and is reachable.
- `! grep -q 'glog\.' kafka_consumer-error-handler.go` — the handler still does not log.

Absence assertions are written as `! grep -q` because `grep -c` exits 1 when the count is zero and would fail the step it was meant to pass. No `git`, `docker`, `kubectl`, `gh` or `dark-factory` command belongs in this block.
</verification>
