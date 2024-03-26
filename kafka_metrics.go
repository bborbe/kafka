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
	MessageHandlerTotalCounterInc(topic Topic, partition Partition)
	MessageHandlerSuccessCounterInc(topic Topic, partition Partition)
	MessageHandlerFailureCounterInc(topic Topic, partition Partition)
	MessageHandlerDurationMeasure(topic Topic, partition Partition, duration time.Duration)
}

type MetricsConsumer interface {
	CurrentOffset(topic Topic, partition Partition, offset Offset)
	HighWaterMarkOffset(topic Topic, partition Partition, offset Offset)
}

type MetricsSyncProducer interface {
	SyncProducerTotalCounterInc(topic Topic)
	SyncProducerFailureCounterInc(topic Topic)
	SyncProducerSuccessCounterInc(topic Topic)
	SyncProducerDurationMeasure(topic Topic, duration time.Duration)
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
)

var (
	messageHandlerTotalCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "total_counter",
		Help:      "Counts processed messages",
	}, []string{"topic", "partition"})
	messageHandlerSuccessCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "success_counter",
		Help:      "Counts successful processed messages",
	}, []string{"topic", "partition"})
	messageHandlerFailureCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "failure_counter",
		Help:      "Counts failed processed messages",
	}, []string{"topic", "partition"})
	messageHandlerDurationMeasure = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "messagehandler",
		Name:      "duration",
		Help:      "Duration of message processing",
		Buckets:   prometheus.LinearBuckets(4000, 1, 1),
	}, []string{"topic", "partition"})
)

var (
	syncProducerTotalCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "syncproducer",
		Name:      "total_counter",
		Help:      "Counts processed messages",
	}, []string{"topic"})
	syncProducerSuccessCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "syncproducer",
		Name:      "success_counter",
		Help:      "Counts successful processed messages",
	}, []string{"topic"})
	syncProducerFailureCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "syncproducer",
		Name:      "failure_counter",
		Help:      "Counts failed processed messages",
	}, []string{"topic"})
	syncProducerDurationMeasure = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "syncproducer",
		Name:      "duration",
		Help:      "Duration of message processing",
		Buckets:   prometheus.LinearBuckets(4000, 1, 1),
	}, []string{"topic"})
)

func init() {
	prometheus.MustRegister(
		currentOffset,
		highWaterMarkOffset,
		messageHandlerTotalCounter,
		messageHandlerSuccessCounter,
		messageHandlerFailureCounter,
		messageHandlerDurationMeasure,
		syncProducerTotalCounter,
		syncProducerSuccessCounter,
		syncProducerFailureCounter,
		syncProducerDurationMeasure,
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

func (m *metrics) MessageHandlerTotalCounterInc(topic Topic, partition Partition) {
	messageHandlerTotalCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) MessageHandlerSuccessCounterInc(topic Topic, partition Partition) {
	messageHandlerSuccessCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) MessageHandlerFailureCounterInc(topic Topic, partition Partition) {
	messageHandlerFailureCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) MessageHandlerDurationMeasure(topic Topic, partition Partition, duration time.Duration) {
	messageHandlerDurationMeasure.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Observe(float64(duration))
}

func (m *metrics) SyncProducerTotalCounterInc(topic Topic, partition Partition) {
	syncProducerTotalCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) SyncProducerSuccessCounterInc(topic Topic, partition Partition) {
	syncProducerSuccessCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) SyncProducerFailureCounterInc(topic Topic, partition Partition) {
	syncProducerFailureCounter.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Inc()
}

func (m *metrics) SyncProducerDurationMeasure(topic Topic, partition Partition, duration time.Duration) {
	syncProducerDurationMeasure.With(prometheus.Labels{
		"topic":     topic.String(),
		"partition": fmt.Sprint(partition),
	}).Observe(float64(duration))
}
