// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"errors"

	"github.com/IBM/sarama"
	"github.com/golang/glog"
)

type ConsumerErrorHandler interface {
	HandleError(err *sarama.ConsumerError) error
}

type ConsumerErrorHandlerFunc func(err *sarama.ConsumerError) error

func (c ConsumerErrorHandlerFunc) HandleError(err *sarama.ConsumerError) error {
	return c(err)
}

func NewConsumerErrorHandler(
	metricsConsumer MetricsConsumer,
) ConsumerErrorHandler {
	return ConsumerErrorHandlerFunc(func(err *sarama.ConsumerError) error {
		metricsConsumer.ErrorCounterInc(
			Topic(err.Topic),
			Partition(err.Partition),
		)
		glog.Warningf("got consumer error(%T) => skip: %v", errors.Unwrap(err), err)
		return err
	})
}
