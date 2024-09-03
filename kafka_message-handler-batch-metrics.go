// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

// NewMessageHandlerBatchMetrics is a MessageHandler adapter that create Prometheus metrics for started, completed and failed.
func NewMessageHandlerBatchMetrics(
	messageHandler MessageHandlerBatch,
	metrics MetricsMessageHandler,
) MessageHandlerBatch {
	return MessageHandlerBatchFunc(func(ctx context.Context, msgs []*sarama.ConsumerMessage) error {
		start := time.Now()
		for _, msg := range msgs {
			metrics.MessageHandlerTotalCounterInc(Topic(msg.Topic), Partition(msg.Partition))
		}
		if err := messageHandler.ConsumeMessages(ctx, msgs); err != nil {
			for _, msg := range msgs {
				metrics.MessageHandlerFailureCounterInc(Topic(msg.Topic), Partition(msg.Partition))
			}
			return err
		}
		for _, msg := range msgs {
			metrics.MessageHandlerSuccessCounterInc(Topic(msg.Topic), Partition(msg.Partition))
			metrics.MessageHandlerDurationMeasure(Topic(msg.Topic), Partition(msg.Partition), time.Since(start))
		}
		return nil
	})
}
