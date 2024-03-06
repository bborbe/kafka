// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"fmt"
)

type TopicPartition struct {
	Topic     Topic
	Partition Partition
}

func (p TopicPartition) Bytes() []byte {
	return []byte(fmt.Sprintf("%s-%d", p.Topic, p.Partition))
}

type OffsetManager interface {
	InitialOffset() Offset
	NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error)
	MarkOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error
}
