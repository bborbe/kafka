// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

// NewMessageHandlerBatchFilter returns a MessageHandlerBatch that removes every message for which skip
// returns true before passing the remaining messages, in their original order, to handler in a single call.
//
// The filter must be composed outside a transaction-opening handler such as
// NewMessageHandlerBatchTxUpdate to have its effect, because only then is the transaction
// never opened for a dropped message. Composed inside, the output is still correct but the
// transaction is opened for every batch.
//
// The skip predicate must be pure and free of side effects and must not carry state across
// calls, because the returned handler is shared by one consumer goroutine per partition and
// the same predicate is invoked concurrently from all of them.
//
// A message that skip drops is never seen by the wrapped handler, so a wrong predicate loses
// the message silently; the filter has no way to detect it.
func NewMessageHandlerBatchFilter(
	skip func(*sarama.ConsumerMessage) bool,
	handler MessageHandlerBatch,
) MessageHandlerBatch {
	return MessageHandlerBatchFunc(
		func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
			kept := make([]*sarama.ConsumerMessage, 0, len(messages))
			for _, msg := range messages {
				if skip(msg) {
					continue
				}
				kept = append(kept, msg)
			}
			if len(kept) == 0 {
				return nil
			}
			return handler.ConsumeMessages(ctx, kept)
		},
	)
}
