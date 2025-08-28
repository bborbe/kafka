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

// OffsetManager manages Kafka consumer offsets for topics and partitions.
// It provides methods to retrieve initial and fallback offsets, track the next offset to consume,
// and mark offsets as processed.
type OffsetManager interface {
	InitialOffset() Offset
	FallbackOffset() Offset
	NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error)
	MarkOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error
	io.Closer
}
