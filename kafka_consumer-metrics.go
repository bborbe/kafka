// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

//counterfeiter:generate -o mocks/kafka-consumer-metrics.go --fake-name KafkaConsumerMetrics . ConsumerMetrics
type ConsumerMetrics interface {
	CurrentOffset(topic Topic, partition Partition, offset Offset)
	HighWaterMarkOffset(topic Topic, partition Partition, offset Offset)
}

var (
	currentOffset = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "current_offset",
		Help:      "Offset of last processed message",
	}, []string{"topic", "partition"})

	highWaterMarkOffset = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "highwater_mark_offset",
		Help:      "Highest offset in the current topic",
	}, []string{"topic", "partition"})
)

func init() {
	prometheus.MustRegister(
		currentOffset,
		highWaterMarkOffset,
	)
}

func NewConsumerMetrics() ConsumerMetrics {
	m := &partitionConsumerMetrics{}
	return m
}

type partitionConsumerMetrics struct {
}

func (m *partitionConsumerMetrics) CurrentOffset(topic Topic, partition Partition, offset Offset) {
	currentOffset.With(
		prometheus.Labels{
			"topic":     topic.String(),
			"partition": fmt.Sprint(partition),
		},
	).Set(float64(offset))
}

func (m *partitionConsumerMetrics) HighWaterMarkOffset(topic Topic, partition Partition, offset Offset) {
	highWaterMarkOffset.With(
		prometheus.Labels{
			"topic":     topic.String(),
			"partition": fmt.Sprint(partition),
		},
	).Set(float64(offset))
}
