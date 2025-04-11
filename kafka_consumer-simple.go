// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"github.com/bborbe/log"
)

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
