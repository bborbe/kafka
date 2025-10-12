// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
)

// NewMessageHandlerBatchDelay returns a MessageHandlerBatch
// that sleep for the given duration after each consume
// this enabled the consumer to get more messages per consume
func NewMessageHandlerBatchDelay(
	messageHandlerBatch MessageHandlerBatch,
	waiterDuration libtime.WaiterDuration,
	delay libtime.Duration,
) MessageHandlerBatch {
	return MessageHandlerBatchFunc(
		func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
			if err := messageHandlerBatch.ConsumeMessages(ctx, messages); err != nil {
				return errors.Wrapf(ctx, err, "consume messages failed")
			}
			if err := waiterDuration.Wait(ctx, delay); err != nil {
				return errors.Wrapf(ctx, err, "wait failed")
			}
			return nil
		},
	)
}
