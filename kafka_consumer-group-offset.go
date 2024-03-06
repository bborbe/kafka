// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

func ConsumerGroupOffsets(
	ctx context.Context,
	saramaClient sarama.Client,
	offsetManager OffsetManager,
	topic Topic,
) (map[Partition]Offset, error) {
	partitions, err := saramaClient.Partitions(topic.String())
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get partitions failed")
	}

	result := make(map[Partition]Offset)
	for _, partition := range partitions {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			nextOffset, err := offsetManager.NextOffset(ctx, topic, Partition(partition))
			if err != nil {
				return nil, errors.Wrapf(ctx, err, "get offset failed")
			}
			result[Partition(partition)] = nextOffset
		}
	}
	return result, nil
}
