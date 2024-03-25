// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

//counterfeiter:generate -o mocks/kafka-metrics.go --fake-name KafkaMetrics . Metrics
type Metrics interface {
	MetricsMessageHandler
	MetricsConsumer
}

type MetricsMessageHandler interface {
	TotalCounterInc(topic Topic, partition Partition)
	FailureCounterInc(topic Topic, partition Partition)
	SuccessCounterInc(topic Topic, partition Partition)
	DurationMeasure(topic Topic, partition Partition, duration time.Duration)
}

type MetricsConsumer interface {
	CurrentOffset(topic Topic, partition Partition, offset Offset)
	HighWaterMarkOffset(topic Topic, partition Partition, offset Offset)
}

const metricsNamespace = "kafka"

var (
	currentOffset = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "consumer",
		Name:      "current_offset",
		Help:      "Offset of last processed message",
	}, []string{"topic", "partition"})
	highWaterMarkOffset = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "consumer",
		Name:      "highwater_mark_offset",
		Help:      "Highest offset in the current topic",
	}, []string{"topic", "partition"})
	totalCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "total_counter",
		Help:      "Counts processed messages",
	}, []string{"topic", "partition"})
	successCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "success_counter",
		Help:      "Counts successful processed messages",
	}, []string{"topic", "partition"})
	failureCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "failure_counter",
		Help:      "Counts failed processed messages",
	}, []string{"topic", "partition"})
	durationMeasure = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "duration",
		Help:      "Duration of message processing",
		Buckets:   prometheus.LinearBuckets(4000, 1, 1),
	}, []string{"topic", "partition"})
)

func init() {
	prometheus.MustRegister(
		currentOffset,
		highWaterMarkOffset,
		totalCounter,
		successCounter,
		failureCounter,
		durationMeasure,
	)
}

func NewMetrics() Metrics {
	return &metrics{}
}

type metrics struct {
}

func (m *metrics) CurrentOffset(topic Topic, partition Partition, offset Offset) {
	currentOffset.With(
		prometheus.Labels{
			"topic":     topic.String(),
			"partition": fmt.Sprint(partition),
		},
	).Set(float64(offset))
}

func (m *metrics) HighWaterMarkOffset(topic Topic, partition Partition, offset Offset) {
	highWaterMarkOffset.With(
		prometheus.Labels{
			"topic":     topic.String(),
			"partition": fmt.Sprint(partition),
		},
	).Set(float64(offset))
}

func (m *metrics) TotalCounterInc(topic Topic, partition Partition) {
	totalCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) FailureCounterInc(topic Topic, partition Partition) {
	failureCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) SuccessCounterInc(topic Topic, partition Partition) {
	successCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) DurationMeasure(topic Topic, partition Partition, duration time.Duration) {
	durationMeasure.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Observe(float64(duration))
}
