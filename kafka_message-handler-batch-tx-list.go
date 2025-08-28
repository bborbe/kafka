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

// MessageHandlerBatchTxList is a list of MessageHandlerBatchTx that executes handlers sequentially within a transaction.
type MessageHandlerBatchTxList []MessageHandlerBatchTx

// ConsumeMessages executes all transaction handlers in the list sequentially.
func (m MessageHandlerBatchTxList) ConsumeMessages(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
	for _, mm := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := mm.ConsumeMessages(ctx, tx, messages); err != nil {
				return errors.Wrapf(ctx, err, "consume message failed")
			}
		}
	}
	return nil
}
