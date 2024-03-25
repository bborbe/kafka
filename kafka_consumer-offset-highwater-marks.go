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

func NewOffsetConsumerHighwaterMarks(
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
	messageHandler MessageHandler,
	trigger run.Fire,
	logSamplerFactory log.SamplerFactory,
) Consumer {
	return NewOffsetConsumerHighwaterMarksBatch(
		saramaClient,
		topic,
		offsetManager,
		NewMessageHandlerBatch(messageHandler),
		1,
		trigger,
		logSamplerFactory,
	)
}

func NewOffsetConsumerHighwaterMarksBatch(
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
	messageHandlerBatch MessageHandlerBatch,
	batchSize BatchSize,
	trigger run.Fire,
	logSamplerFactory log.SamplerFactory,
) Consumer {
	return ConsumerFunc(func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		triggerOffsets, err := createTriggerOffsets(ctx, saramaClient, topic, offsetManager)
		if err != nil {
			return errors.Wrapf(ctx, err, "create trigger offsets failed")
		}
		glog.V(2).Infof("trigger offsets %+v for %s", triggerOffsets, topic)

		return NewOffsetConsumerBatch(
			saramaClient,
			topic,
			offsetManager,
			MessageHandlerBatchList{
				messageHandlerBatch,
				NewMessageHandlerBatch(
					NewOffsetTriggerMessageHandler(triggerOffsets, topic, trigger),
				),
			},
			batchSize,
			logSamplerFactory,
		).Consume(ctx)
	})
}

func createTriggerOffsets(
	ctx context.Context,
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
) (map[Partition]Offset, error) {
	currentHighWaterMarks, err := HighWaterMarks(ctx, saramaClient, topic)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get highwater marks failed")
	}
	glog.V(2).Infof("current highwater mark offsets of topic %s: %+v", topic, currentHighWaterMarks)

	consumerGroupOffsets, err := ConsumerGroupOffsets(ctx, saramaClient, offsetManager, topic)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get consumer group offsets failed")
	}
	glog.V(2).Infof("consumer group offsets of topic %s: %+v", topic, consumerGroupOffsets)

	readConsumerGroupOffsets, err := GetRealOffset(ctx, saramaClient, topic, consumerGroupOffsets)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get consumer group offsets failed")
	}
	glog.V(2).Infof("real consumer group offsets of topic %s: %+v", topic, readConsumerGroupOffsets)

	return combineConsumerGroupAndHighwaterMark(
		readConsumerGroupOffsets,
		currentHighWaterMarks,
	), nil
}

func combineConsumerGroupAndHighwaterMark(
	consumerGroupOffsets map[Partition]Offset,
	currentHighWaterMarks map[Partition]Offset,
) map[Partition]Offset {

	triggerOffsets := map[Partition]Offset{}

	for partition, highwaterMarkOffset := range currentHighWaterMarks {
		if highwaterMarkOffset == 0 {
			glog.V(3).Infof("highwater mark offset = 0 => skip")
			continue
		}
		consumerGroupOffset := consumerGroupOffsets[partition]
		if consumerGroupOffset == Offset(sarama.OffsetNewest) {
			glog.V(3).Infof("consumer group offset = newest => skip")
			continue
		}

		if consumerGroupOffset == highwaterMarkOffset {
			glog.V(3).Infof("currentOffset(%d) == highwaterMarkOffset(%d) => skip", consumerGroupOffset, highwaterMarkOffset)
			continue
		}

		// highwater mark offset is last seen offset + 1, so we have to sub one
		triggerOffsets[partition] = highwaterMarkOffset - 1
	}

	return triggerOffsets
}
