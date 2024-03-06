// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	"github.com/bborbe/log"
	"github.com/golang/glog"
)

func NewMessageHandlerBatchTxSkipErrors(
	handler MessageHandlerBatchTx,
	logSamplerFactory log.SamplerFactory,
) MessageHandlerBatchTx {
	logSampler := logSamplerFactory.Sampler()
	return MessageHandlerBatchTxFunc(func(ctx context.Context, tx libkv.Tx, msgs []*sarama.ConsumerMessage) error {
		if err := handler.ConsumeMessages(ctx, tx, msgs); err != nil {
			if logSampler.IsSample() {
				data := errors.DataFromError(
					err,
				)
				glog.Warningf("consume message with failed: %v %+v (sample)", err, data)
			}
		}
		return nil
	})
}
