// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
)

// NewMetricsMessageHandlerTx is a MessageHandler adapter that create Prometheus metrics for started, completed and failed.
func NewMetricsMessageHandlerTx(
	messageHandler MessageHandlerTx,
	metrics MetricsMessageHandler,
) MessageHandlerTx {
	return MessageHandlerTxFunc(func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
		start := time.Now()
		metrics.TotalCounterInc(Topic(msg.Topic), Partition(msg.Partition))
		if err := messageHandler.ConsumeMessage(ctx, tx, msg); err != nil {
			metrics.FailureCounterInc(Topic(msg.Topic), Partition(msg.Partition))
			return err
		}
		metrics.SuccessCounterInc(Topic(msg.Topic), Partition(msg.Partition))
		metrics.DurationMeasure(Topic(msg.Topic), Partition(msg.Partition), time.Since(start))
		return nil
	})
}
