// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"maps"
	"slices"
	"testing"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestErrorCounterInc_ErrorClassLabel(t *testing.T) {
	handler := NewConsumerErrorHandler(NewMetrics())

	brokerErr := &sarama.ConsumerError{
		Topic:     "test-error-class-broker",
		Partition: 11,
		Err:       sarama.ErrNotLeaderForPartition,
	}
	if err := handler.HandleError(brokerErr); err != brokerErr {
		t.Fatalf("expected handler to return the original error, got %v", err)
	}
	assertErrorClassCounter(t, "test-error-class-broker", 11, ErrorClassBrokerSide, 1)
	assertErrorClassCounter(t, "test-error-class-broker", 11, ErrorClassCorruption, 0)
	if class := ClassifyError(brokerErr); class != ErrorClassBrokerSide {
		t.Fatalf("expected broker error to classify as %q, got %q", ErrorClassBrokerSide, class)
	} else {
		assertErrorClassCounter(t, "test-error-class-broker", 11, class, 1)
	}

	corruptErr := &sarama.ConsumerError{
		Topic:     "test-error-class-corrupt",
		Partition: 12,
		Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
	}
	if err := handler.HandleError(corruptErr); err != corruptErr {
		t.Fatalf("expected handler to return the original error, got %v", err)
	}
	assertErrorClassCounter(t, "test-error-class-corrupt", 12, ErrorClassCorruption, 1)
	assertErrorClassCounter(t, "test-error-class-corrupt", 12, ErrorClassBrokerSide, 0)
	if class := ClassifyError(corruptErr); class != ErrorClassCorruption {
		t.Fatalf("expected corrupt error to classify as %q, got %q", ErrorClassCorruption, class)
	} else {
		assertErrorClassCounter(t, "test-error-class-corrupt", 12, class, 1)
	}
}

func assertErrorClassCounter(
	t *testing.T,
	topic string,
	partition Partition,
	class ErrorClass,
	expected float64,
) {
	t.Helper()
	value := testutil.ToFloat64(consumerErrorCounter.With(prometheus.Labels{
		"topic":       topic,
		"partition":   partition.String(),
		"error_class": class.String(),
	}))
	if value != expected {
		t.Errorf(
			"expected error_class %q for %s/%s to be %v, got %v",
			class,
			topic,
			partition,
			expected,
			value,
		)
	}
}

func TestConsumerErrorCounterLabelSet(t *testing.T) {
	NewMetrics().ErrorCounterInc(Topic("test-label-set"), Partition(1), ErrorClassOther)

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
}
