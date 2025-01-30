// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"strings"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/golang/glog"
)

const OutOfRangeErrorMessage = "The requested offset is outside the range of offsets maintained by the server for the given topic/partition"

// CreatePartitionConsumer create partition consumer and use initial offset if out of range error
func CreatePartitionConsumer(
	ctx context.Context,
	consumerFromClient sarama.Consumer,
	topic Topic,
	partition Partition,
	fallbackOffset Offset, // offset to use if OutOfRangeErrorMessage
	nextOffset Offset,
) (sarama.PartitionConsumer, error) {
	consumePartition, err := consumerFromClient.ConsumePartition(topic.String(), partition.Int32(), nextOffset.Int64())
	if err != nil {
		if strings.Contains(err.Error(), OutOfRangeErrorMessage) {
			glog.Warningf("create partition consumer for topic(%s), partition(%s) anf offset(%s) got out of range error => fallback to initial offset(%s)", topic, partition, nextOffset, fallbackOffset)
			return consumerFromClient.ConsumePartition(topic.String(), partition.Int32(), fallbackOffset.Int64())
		}
		return nil, errors.Wrapf(ctx, err, "create partition consumer failed")
	}
	return consumePartition, nil
}
