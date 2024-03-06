// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

//counterfeiter:generate -o mocks/kafka-message-handler-batch.go --fake-name KafkaMessageHandlerBatch . MessageHandlerBatch
type MessageHandlerBatch interface {
	ConsumeMessages(ctx context.Context, messages []*sarama.ConsumerMessage) error
}

func NewMessageHandlerBatch(messageHandler MessageHandler) MessageHandlerBatch {
	return MessageHandlerBatchFunc(func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
		for _, msg := range messages {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := messageHandler.ConsumeMessage(ctx, msg); err != nil {
					return errors.Wrapf(ctx, err, "consume message failed")
				}
			}
		}
		return nil
	})
}
