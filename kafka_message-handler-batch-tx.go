// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

//counterfeiter:generate -o mocks/kafka-message-handler-batch-tx.go --fake-name KafkaMessageHandlerBatchTx . MessageHandlerBatchTx
type MessageHandlerBatchTx interface {
	ConsumeMessages(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error
}

func NewMessageHandlerBatchTx(messageHandler MessageHandlerTx) MessageHandlerBatchTx {
	return MessageHandlerBatchTxFunc(func(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
		for _, msg := range messages {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := messageHandler.ConsumeMessage(ctx, tx, msg); err != nil {
					return errors.Wrapf(ctx, err, "consume message failed")
				}
			}
		}
		return nil
	})
}

func NewMessageHandlerBatchTxView(db libkv.DB, messageHandler MessageHandlerBatchTx) MessageHandlerBatch {
	return MessageHandlerBatchFunc(func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
		return db.View(ctx, func(ctx context.Context, tx libkv.Tx) error {
			return messageHandler.ConsumeMessages(ctx, tx, messages)
		})
	})
}

func NewMessageHandlerBatchTxUpdate(db libkv.DB, messageHandler MessageHandlerBatchTx) MessageHandlerBatch {
	return MessageHandlerBatchFunc(func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
		return db.Update(ctx, func(ctx context.Context, tx libkv.Tx) error {
			return messageHandler.ConsumeMessages(ctx, tx, messages)
		})
	})
}
