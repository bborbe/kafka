// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
)

// MessageHandlerBatchTxFunc is a function type that implements MessageHandlerBatchTx interface.
type MessageHandlerBatchTxFunc func(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error

// ConsumeMessages implements the MessageHandlerBatchTx interface.
func (b MessageHandlerBatchTxFunc) ConsumeMessages(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
	return b(ctx, tx, messages)
}
