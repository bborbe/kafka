// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
	libtime "github.com/bborbe/time"
)

// NewMessageHandlerBatchTxMetrics is a MessageHandler adapter that create Prometheus metrics for started, completed and failed.
func NewMessageHandlerBatchTxMetrics(
	messageHandler MessageHandlerBatchTx,
	metrics MetricsMessageHandler,
) MessageHandlerBatchTx {
	return MessageHandlerBatchTxFunc(
		func(ctx context.Context, tx libkv.Tx, msgs []*sarama.ConsumerMessage) error {
			start := libtime.Now()
			for _, msg := range msgs {
				metrics.MessageHandlerTotalCounterInc(Topic(msg.Topic), Partition(msg.Partition))
			}
			if err := messageHandler.ConsumeMessages(ctx, tx, msgs); err != nil {
				for _, msg := range msgs {
					metrics.MessageHandlerFailureCounterInc(
						Topic(msg.Topic),
						Partition(msg.Partition),
					)
				}
				return err
			}
			for _, msg := range msgs {
				metrics.MessageHandlerSuccessCounterInc(Topic(msg.Topic), Partition(msg.Partition))
				metrics.MessageHandlerDurationMeasure(
					Topic(msg.Topic),
					Partition(msg.Partition),
					libtime.Now().Sub(start),
				)
			}
			return nil
		},
	)
}
