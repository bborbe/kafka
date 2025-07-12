// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	logmocks "github.com/bborbe/log/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("NewMessageHandlerSkipErrors", func() {
	var (
		ctx               context.Context
		msg               *sarama.ConsumerMessage
		innerHandler      *mocks.KafkaMessageHandler
		logSamplerFactory *logmocks.LogSamplerFactory
		logSampler        *logmocks.LogSampler
		skipHandler       libkafka.MessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()
		msg = &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    123,
		}
		innerHandler = &mocks.KafkaMessageHandler{}
		logSamplerFactory = &logmocks.LogSamplerFactory{}
		logSampler = &logmocks.LogSampler{}
		logSamplerFactory.SamplerReturns(logSampler)

		skipHandler = libkafka.NewMessageHandlerSkipErrors(innerHandler, logSamplerFactory)
	})

	It("should implement MessageHandler interface", func() {
		var _ libkafka.MessageHandler = skipHandler
		Expect(skipHandler).NotTo(BeNil())
	})

	It("should call SamplerFactory.Sampler during construction", func() {
		Expect(logSamplerFactory.SamplerCallCount()).To(Equal(1))
	})

	It("should forward successful calls to inner handler", func() {
		innerHandler.ConsumeMessageReturns(nil)

		err := skipHandler.ConsumeMessage(ctx, msg)
		Expect(err).To(BeNil())

		Expect(innerHandler.ConsumeMessageCallCount()).To(Equal(1))
		receivedCtx, receivedMsg := innerHandler.ConsumeMessageArgsForCall(0)
		Expect(receivedCtx).To(Equal(ctx))
		Expect(receivedMsg).To(Equal(msg))
	})

	It("should skip errors and return nil when inner handler fails", func() {
		innerHandler.ConsumeMessageReturns(errors.New("inner error"))
		logSampler.IsSampleReturns(false)

		err := skipHandler.ConsumeMessage(ctx, msg)
		Expect(err).To(BeNil())

		Expect(innerHandler.ConsumeMessageCallCount()).To(Equal(1))
		Expect(logSampler.IsSampleCallCount()).To(Equal(1))
	})

	It("should log errors when sampling is enabled", func() {
		innerHandler.ConsumeMessageReturns(errors.New("inner error"))
		logSampler.IsSampleReturns(true)

		err := skipHandler.ConsumeMessage(ctx, msg)
		Expect(err).To(BeNil())

		Expect(innerHandler.ConsumeMessageCallCount()).To(Equal(1))
		Expect(logSampler.IsSampleCallCount()).To(Equal(1))
	})

	It("should not log errors when sampling is disabled", func() {
		innerHandler.ConsumeMessageReturns(errors.New("inner error"))
		logSampler.IsSampleReturns(false)

		err := skipHandler.ConsumeMessage(ctx, msg)
		Expect(err).To(BeNil())

		Expect(innerHandler.ConsumeMessageCallCount()).To(Equal(1))
		Expect(logSampler.IsSampleCallCount()).To(Equal(1))
	})

	It("should handle nil inner handler gracefully", func() {
		nilSkipHandler := libkafka.NewMessageHandlerSkipErrors(nil, logSamplerFactory)

		Expect(func() {
			_ = nilSkipHandler.ConsumeMessage(ctx, msg)
		}).To(Panic())
	})
})
