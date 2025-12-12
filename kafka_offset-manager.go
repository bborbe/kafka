// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"fmt"
	"io"
)

// TopicPartition represents a specific partition within a Kafka topic.
type TopicPartition struct {
	Topic     Topic
	Partition Partition
}

// Bytes returns a byte representation of the TopicPartition in the format "topic-partition".
func (p TopicPartition) Bytes() []byte {
	return []byte(fmt.Sprintf("%s-%d", p.Topic, p.Partition))
}

//counterfeiter:generate -o mocks/kafka-offset-manager.go --fake-name KafkaOffsetManager . OffsetManager

// OffsetManager manages Kafka consumer offsets for topics and partitions.
// It provides methods to retrieve initial and fallback offsets, track the next offset to consume,
// and mark offsets as processed.
type OffsetManager interface {
	// InitialOffset returns the offset to use when no previous offset exists for a partition.
	InitialOffset() Offset
	// FallbackOffset returns the offset to use when the stored offset is invalid or unavailable.
	FallbackOffset() Offset
	// NextOffset retrieves the next offset to consume for the given topic and partition.
	NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error)
	// MarkOffset marks the provided offset as processed. This only allows forward movement (incrementing).
	// To follow upstream conventions, you should mark the offset of the next message to read, not the last message read.
	MarkOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error
	// ResetOffset resets to the provided offset, allowing backward movement to earlier or smaller values.
	// This acts as a counterpart to MarkOffset and should be called before MarkOffset when setting offsets backwards.
	ResetOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error
	io.Closer
}
