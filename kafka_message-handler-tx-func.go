// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
)

// MessageHandlerTxFunc allow use a function as MessageHandler.
type MessageHandlerTxFunc func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error

// ConsumeMessage forward to the function.
func (m MessageHandlerTxFunc) ConsumeMessage(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
	return m(ctx, tx, msg)
}
