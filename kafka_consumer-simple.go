// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"github.com/bborbe/log"
)

// NewSimpleConsumer creates a new simple consumer that processes messages individually.
// It wraps the provided MessageHandler in a batch handler with batch size 1.
func NewSimpleConsumer(
	saramaClient SaramaClient,
	topic Topic,
	initalOffset Offset,
	messageHandler MessageHandler,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	return NewSimpleConsumerBatch(
		saramaClient,
		topic,
		initalOffset,
		NewMessageHandlerBatch(messageHandler),
		1,
		logSamplerFactory,
		options...,
	)
}

// NewSimpleConsumerBatch creates a new simple consumer that processes messages in batches.
// It uses a simple offset manager with the provided initial offset for both initial and fallback scenarios.
func NewSimpleConsumerBatch(
	saramaClient SaramaClient,
	topic Topic,
	initalOffset Offset,
	messageHandler MessageHandlerBatch,
	batchSize BatchSize,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	return NewOffsetConsumerBatch(
		saramaClient,
		topic,
		NewSimpleOffsetManager(
			initalOffset,
			initalOffset,
		),
		messageHandler,
		batchSize,
		logSamplerFactory,
		options...,
	)
}
