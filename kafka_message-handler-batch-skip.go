// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/bborbe/log"
	"github.com/golang/glog"
)

func NewMessageHandlerBatchSkipErrors(
	handler MessageHandlerBatch,
	logSamplerFactory log.SamplerFactory,
) MessageHandlerBatch {
	logSampler := logSamplerFactory.Sampler()
	return MessageHandlerBatchFunc(func(ctx context.Context, msgs []*sarama.ConsumerMessage) error {
		if err := handler.ConsumeMessages(ctx, msgs); err != nil {
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
