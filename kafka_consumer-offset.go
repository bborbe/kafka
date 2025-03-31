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
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

type ConsumerOptions struct {
	TargetLag int64
	Delay     libtime.Duration
}

func NewOffsetConsumer(
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
	messageHandler MessageHandler,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	return NewOffsetConsumerBatch(
		saramaClient,
		topic,
		offsetManager,
		NewMessageHandlerBatch(messageHandler),
		1,
		logSamplerFactory,
		options...,
	)
}

func NewOffsetConsumerBatch(
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
	messageHandlerBatch MessageHandlerBatch,
	batchSize BatchSize,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	consumerOptions := ConsumerOptions{
		TargetLag: 0,
		Delay:     0,
	}
	for _, option := range options {
		option(&consumerOptions)
	}
	return &offsetConsumer{
		batchSize:           batchSize,
		saramaClient:        saramaClient,
		offsetManager:       offsetManager,
		messageHandlerBatch: messageHandlerBatch,
		topic:               topic,
		logSampler:          logSamplerFactory.Sampler(),
		metrics:             NewMetrics(),
		waiter:              libtime.NewWaiterDuration(),
		consumerOptions:     consumerOptions,
	}
}

type offsetConsumer struct {
	saramaClient        sarama.Client
	topic               Topic
	offsetManager       OffsetManager
	messageHandlerBatch MessageHandlerBatch
	batchSize           BatchSize
	logSampler          log.Sampler
	metrics             interface {
		MetricsConsumer
		MetricsPartitionConsumer
	}
	waiter          libtime.WaiterDuration
	consumerOptions ConsumerOptions
}

func (c *offsetConsumer) Consume(ctx context.Context) error {
	consumerFromClient, err := sarama.NewConsumerFromClient(c.saramaClient)
	if err != nil {
		return errors.Wrapf(ctx, err, "create consumer failed")
	}
	defer consumerFromClient.Close()

	partitions, err := c.saramaClient.Partitions(c.topic.String())
	if err != nil {
		return errors.Wrapf(ctx, err, "get partition for topic %s failed", c.topic)
	}

	glog.V(2).Infof("consume topic %s with %d partitions %+v started", c.topic, len(partitions), c.consumerOptions)

	var runs []run.Func
	for _, partition := range partitions {
		runs = append(runs, func(ctx context.Context) error {

			nextOffset, err := c.offsetManager.NextOffset(ctx, c.topic, Partition(partition))
			if err != nil {
				return errors.Wrapf(ctx, err, "get next offset  topic(%s) with partition(%d) failed", c.topic, Partition(partition))
			}

			glog.V(2).Infof("consume topic(%s) with partition(%d) and offset(%s) started", c.topic, partition, nextOffset)
			consumePartition, err := CreatePartitionConsumer(
				ctx,
				consumerFromClient,
				c.metrics,
				c.topic,
				Partition(partition),
				c.offsetManager.FallbackOffset(),
				nextOffset,
			)
			if err != nil {
				return errors.Wrapf(ctx, err, "create partition consumer for topic(%s) with partition(%d) and offset(%s) failed", c.topic, partition, nextOffset)
			}
			defer consumePartition.Close()
			for {
				messages, err := c.consumeMessages(ctx, consumePartition)
				if err != nil {
					return errors.Wrapf(ctx, err, "consume failed")
				}
				msg := messages[len(messages)-1]
				glog.V(4).Infof("consume %d messages in topic %s with offset %d partition %d started", len(messages), msg.Topic, msg.Offset, msg.Partition)

				highWaterMarketOffset := consumePartition.HighWaterMarkOffset()
				lag := highWaterMarketOffset - msg.Offset

				c.metrics.CurrentOffset(c.topic, Partition(partition), Offset(msg.Offset))
				c.metrics.HighWaterMarkOffset(c.topic, Partition(partition), Offset(highWaterMarketOffset))

				if err := c.messageHandlerBatch.ConsumeMessages(ctx, messages); err != nil {
					return errors.Wrapf(ctx, err, "consume message failed")
				}
				nextOffset := msg.Offset + 1
				if err := c.offsetManager.MarkOffset(ctx, c.topic, Partition(partition), Offset(nextOffset)); err != nil {
					return errors.Wrapf(ctx, err, "mark offset failed")
				}

				// wait if lag is low, this allow batch consumer get more messages next time
				if lag < c.consumerOptions.TargetLag && c.consumerOptions.TargetLag > 0 && c.consumerOptions.Delay > 0 {
					glog.V(3).Infof("lag(%d) < targetLag(%d) => wait for %v", lag, c.consumerOptions.TargetLag, c.consumerOptions.Delay)
					if err := c.waiter.Wait(ctx, c.consumerOptions.Delay); err != nil {
						return errors.Wrapf(ctx, err, "wait for %v failed", c.consumerOptions.Delay)
					}
				}

				if c.logSampler.IsSample() {
					glog.V(2).Infof("consume %d messages in topic(%s), partition(%d) and offset(%d) completed (highwatermark: %d lag: %d) (sample)",
						len(messages),
						msg.Topic,
						msg.Partition,
						msg.Offset,
						highWaterMarketOffset,
						lag,
					)
				}
			}
		})
	}

	if err := run.CancelOnFirstError(ctx, runs...); err != nil {
		return errors.Wrapf(ctx, err, "run failed")
	}
	glog.V(2).Infof("consume topic(%s) with %d partitions completed", c.topic, len(partitions))
	return nil
}

// consume
func (c *offsetConsumer) consumeMessages(ctx context.Context, consumePartition sarama.PartitionConsumer) ([]*sarama.ConsumerMessage, error) {
	var result []*sarama.ConsumerMessage

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-consumePartition.Errors():
		return nil, errors.Wrapf(ctx, err, "parition consumer returns error")
	case msg := <-consumePartition.Messages():
		result = append(result, msg)
	}

	for {
		if len(result) == c.batchSize.Int() {
			glog.V(4).Infof("reached batch size => return %d messages", len(result))
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-consumePartition.Errors():
			return nil, errors.Wrapf(ctx, err, "parition consumer returns error")
		case msg := <-consumePartition.Messages():
			result = append(result, msg)
		default:
			glog.V(4).Infof("no more messages => return %d messages", len(result))
			return result, nil
		}
	}
}
