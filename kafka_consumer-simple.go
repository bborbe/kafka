// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	"github.com/golang/glog"
)

func NewSimpleConsumer(
	saramaClient sarama.Client,
	topic Topic,
	initalOffset Offset,
	messageHandler MessageHandler,
	logSamplerFactory log.SamplerFactory,
) Consumer {
	return &simpleConsumer{
		saramaClient:   saramaClient,
		topic:          topic,
		initalOffset:   initalOffset,
		messageHandler: messageHandler,
		logSampler:     logSamplerFactory.Sampler(),
		metrics:        NewMetrics(),
	}
}

type simpleConsumer struct {
	saramaClient   sarama.Client
	topic          Topic
	initalOffset   Offset
	messageHandler MessageHandler
	logSampler     log.Sampler
	metrics        MetricsConsumer
}

func (c *simpleConsumer) Consume(ctx context.Context) error {
	consumerFromClient, err := sarama.NewConsumerFromClient(c.saramaClient)
	if err != nil {
		return errors.Wrapf(ctx, err, "create consumer failed")
	}
	defer consumerFromClient.Close()

	partitions, err := c.saramaClient.Partitions(c.topic.String())
	if err != nil {
		return errors.Wrapf(ctx, err, "get partition for topic %s failed", c.topic)
	}

	glog.V(2).Infof("consume topic %s with %d partitions started", c.topic, len(partitions))

	var runs []run.Func
	for _, partition := range partitions {
		runs = append(runs, func(ctx context.Context) error {

			consumePartition, err := consumerFromClient.ConsumePartition(c.topic.String(), partition, c.initalOffset.Int64())
			if err != nil {
				return errors.Wrapf(ctx, err, "create simple partition consumer for topic %s failed", c.topic)
			}
			defer consumePartition.Close()

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case err := <-consumePartition.Errors():
					return errors.Wrapf(ctx, err, "parition consumer returns error")
				case msg := <-consumePartition.Messages():
					glog.V(4).Infof("consume message in topic %s with offset %d partition %d started", msg.Topic, msg.Offset, msg.Partition)
					c.metrics.CurrentOffset(c.topic, Partition(partition), Offset(msg.Offset))
					c.metrics.HighWaterMarkOffset(c.topic, Partition(partition), Offset(consumePartition.HighWaterMarkOffset()))
					if err := c.messageHandler.ConsumeMessage(ctx, msg); err != nil {
						return errors.Wrapf(ctx, err, "consume message failed")
					}
					if c.logSampler.IsSample() {
						glog.V(3).Infof("consume message in topic %s with offset %d partition %d completed (%d highwatermark: %d lag: %d) (sample)", msg.Topic, msg.Offset, msg.Partition, consumePartition.HighWaterMarkOffset(), consumePartition.HighWaterMarkOffset()-msg.Offset)
					}
				}
			}
		})
	}

	if err := run.CancelOnFirstError(ctx, runs...); err != nil {
		return errors.Wrapf(ctx, err, "run failed")
	}
	glog.V(2).Infof("consume topic %s with %d partitions completed", c.topic, len(partitions))
	return nil
}
