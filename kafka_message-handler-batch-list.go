// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

// MessageHandlerBatchList is a list of MessageHandlerBatch that executes handlers sequentially.
type MessageHandlerBatchList []MessageHandlerBatch

// ConsumeMessages executes all handlers in the list sequentially.
func (m MessageHandlerBatchList) ConsumeMessages(
	ctx context.Context,
	messages []*sarama.ConsumerMessage,
) error {
	for _, mm := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := mm.ConsumeMessages(ctx, messages); err != nil {
				return errors.Wrapf(ctx, err, "consume message failed")
			}
		}
	}
	return nil
}
