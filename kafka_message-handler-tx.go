// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
)

//counterfeiter:generate -o mocks/kafka-message-handler-tx.go --fake-name KafkaMessageHandlerTx . MessageHandlerTx

// MessageHandlerTx defines the interface for handling messages within a transaction context.
type MessageHandlerTx interface {
	ConsumeMessage(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error
}

// NewMessageTxView creates a read-only message handler that executes within a database view transaction.
func NewMessageTxView(db libkv.DB, messageHandlerTx MessageHandlerTx) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, message *sarama.ConsumerMessage) error {
		return db.View(ctx, func(ctx context.Context, tx libkv.Tx) error {
			return messageHandlerTx.ConsumeMessage(ctx, tx, message)
		})
	})
}

// NewMessageTxUpdate creates a message handler that executes within a database update transaction.
func NewMessageTxUpdate(db libkv.DB, messageHandlerTx MessageHandlerTx) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, message *sarama.ConsumerMessage) error {
		return db.Update(ctx, func(ctx context.Context, tx libkv.Tx) error {
			return messageHandlerTx.ConsumeMessage(ctx, tx, message)
		})
	})
}
