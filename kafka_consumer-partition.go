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

// IsOffsetOutOfRange checks if the given error represents an offset out of range condition.
// It checks for sarama.ErrOffsetOutOfRange, sarama.KError, and string matching for backwards compatibility.
func IsOffsetOutOfRange(err error) bool {
	if err == nil {
		return false
	}

	// Check for direct sarama.ErrOffsetOutOfRange
	if errors.Cause(err) == sarama.ErrOffsetOutOfRange {
		return true
	}

	// Check for sarama.KError wrapped offset error
	if kerr, ok := errors.Cause(err).(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
		return true
	}

	// Check for string matching (backwards compatibility)
	if strings.Contains(err.Error(), OutOfRangeErrorMessage) {
		return true
	}

	return false
}

// CreatePartitionConsumer create partition consumer and use initial offset if out of range error
func CreatePartitionConsumer(
	ctx context.Context,
	consumerFromClient sarama.Consumer,
	metricsConsumer MetricsPartitionConsumer,
	topic Topic,
	partition Partition,
	fallbackOffset Offset, // offset to use if OutOfRangeErrorMessage
	nextOffset Offset,
) (sarama.PartitionConsumer, error) {
	metricsConsumer.ConsumePartitionCreateTotalInc(topic, partition)
	metricsConsumer.ConsumePartitionCreateOutOfRangeErrorInitialize(topic, partition)
	consumePartition, err := consumerFromClient.ConsumePartition(topic.String(), partition.Int32(), nextOffset.Int64())
	if err != nil {
		metricsConsumer.ConsumePartitionCreateFailureInc(topic, partition)
		if IsOffsetOutOfRange(err) {
			metricsConsumer.ConsumePartitionCreateOutOfRangeErrorInc(topic, partition)
			glog.Warningf("create partition consumer for topic(%s), partition(%s) anf offset(%s) got out of range error => fallback to initial offset(%s)", topic, partition, nextOffset, fallbackOffset)
			return consumerFromClient.ConsumePartition(topic.String(), partition.Int32(), fallbackOffset.Int64())
		}
		return nil, errors.Wrapf(ctx, err, "create partition consumer failed")
	}
	metricsConsumer.ConsumePartitionCreateSuccessInc(topic, partition)
	return consumePartition, nil
}
