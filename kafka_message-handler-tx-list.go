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

type MessageHanderTxList []MessageHandlerTx

func (m MessageHanderTxList) ConsumeMessage(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
	for _, mm := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := mm.ConsumeMessage(ctx, tx, msg); err != nil {
				return errors.Wrapf(ctx, err, "consume message failed")
			}
		}
	}
	return nil
}
