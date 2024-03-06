// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/golang/glog"
)

func HighWaterMarks(
	ctx context.Context,
	saramaClient sarama.Client,
	topic Topic,
) (PartitionOffsets, error) {
	partitions, err := saramaClient.Partitions(topic.String())
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get partitions for topic %s failed", topic)
	}
	result := make(map[Partition]Offset)
	for _, partition := range partitions {
		highwaterMarkOffset, err := HighWaterMark(ctx, saramaClient, topic, Partition(partition))
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "get offset for topic %s failed", topic)
		}
		result[Partition(partition)] = *highwaterMarkOffset
	}
	glog.V(3).Infof("found highwater marks for %v for topic %s", result, topic)
	return result, nil
}

func HighWaterMark(
	ctx context.Context,
	saramaClient sarama.Client,
	topic Topic,
	partition Partition,
) (*Offset, error) {
	highwaterMarkOffset, err := saramaClient.GetOffset(topic.String(), partition.Int32(), sarama.OffsetNewest)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get offset for topic %s failed", topic)
	}
	offset := Offset(highwaterMarkOffset)
	glog.V(3).Infof("found highwater mark %d for topic %s and partition %d", offset, topic, partition)
	return &offset, nil
}

//counterfeiter:generate -o mocks/kafka-highwatermark-provider.go --fake-name KafkaHighwaterMarkProvider . HighwaterMarkProvider
type HighwaterMarkProvider interface {
	HighWaterMark(ctx context.Context, topic Topic, partition Partition) (*Offset, error)
}

func NewHighwaterMarkProvider(
	saramaClient sarama.Client,
) HighwaterMarkProvider {
	return &highwaterMarkProvider{
		saramaClient: saramaClient,
	}
}

type highwaterMarkProvider struct {
	saramaClient sarama.Client
}

func (h *highwaterMarkProvider) HighWaterMark(
	ctx context.Context,
	topic Topic,
	partition Partition,
) (*Offset, error) {
	return HighWaterMark(ctx, h.saramaClient, topic, partition)
}
