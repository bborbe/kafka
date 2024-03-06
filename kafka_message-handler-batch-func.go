// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

type MessageHandlerBatchFunc func(ctx context.Context, messages []*sarama.ConsumerMessage) error

func (b MessageHandlerBatchFunc) ConsumeMessages(ctx context.Context, messages []*sarama.ConsumerMessage) error {
	return b(ctx, messages)
}
