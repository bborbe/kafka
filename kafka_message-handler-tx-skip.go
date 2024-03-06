// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	"github.com/bborbe/log"
	"github.com/golang/glog"
)

func NewMessageHandlerTxSkipErrors(
	handler MessageHandlerTx,
	logSamplerFactory log.SamplerFactory,
) MessageHandlerTx {
	logSampler := logSamplerFactory.Sampler()
	return MessageHandlerTxFunc(func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
		if err := handler.ConsumeMessage(ctx, tx, msg); err != nil {
			if logSampler.IsSample() {
				data := errors.DataFromError(
					errors.AddDataToError(
						err,
						map[string]string{
							"topic":     msg.Topic,
							"partition": fmt.Sprintf("%d", msg.Partition),
							"offset":    fmt.Sprintf("%d", msg.Offset),
						},
					),
				)
				glog.Warningf("consume message with offset %d in partition %d in topic %s failed: %v %+v (sample)", msg.Offset, msg.Partition, msg.Topic, err, data)
			}
		}
		return nil
	})
}
