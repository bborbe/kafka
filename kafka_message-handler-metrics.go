// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

// NewMessageHandlerMetrics is a MessageHandler adapter that create Prometheus metrics for started, completed and failed.
func NewMessageHandlerMetrics(
	messageHandler MessageHandler,
	metrics MetricsMessageHandler,
) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		start := time.Now()
		metrics.MessageHandlerTotalCounterInc(Topic(msg.Topic), Partition(msg.Partition))
		if err := messageHandler.ConsumeMessage(ctx, msg); err != nil {
			metrics.MessageHandlerFailureCounterInc(Topic(msg.Topic), Partition(msg.Partition))
			return err
		}
		metrics.MessageHandlerSuccessCounterInc(Topic(msg.Topic), Partition(msg.Partition))
		metrics.MessageHandlerDurationMeasure(Topic(msg.Topic), Partition(msg.Partition), time.Since(start))
		return nil
	})
}
