// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"github.com/IBM/sarama"
)

// ConsumerErrorHandler defines an interface for handling Kafka consumer errors.
type ConsumerErrorHandler interface {
	HandleError(err *sarama.ConsumerError) error
}

// ConsumerErrorHandlerFunc is a function type that implements ConsumerErrorHandler.
type ConsumerErrorHandlerFunc func(err *sarama.ConsumerError) error

// HandleError implements the ConsumerErrorHandler interface for ConsumerErrorHandlerFunc.
func (c ConsumerErrorHandlerFunc) HandleError(err *sarama.ConsumerError) error {
	return c(err)
}

// NewConsumerErrorHandler creates a new ConsumerErrorHandler that updates metrics.
func NewConsumerErrorHandler(
	metricsConsumer MetricsConsumer,
) ConsumerErrorHandler {
	return ConsumerErrorHandlerFunc(func(err *sarama.ConsumerError) error {
		metricsConsumer.ErrorCounterInc(
			Topic(err.Topic),
			Partition(err.Partition),
		)
		return err
	})
}
