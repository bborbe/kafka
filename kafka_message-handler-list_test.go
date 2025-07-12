// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("MessageHandlerList", func() {
	var (
		ctx      context.Context
		msg      *sarama.ConsumerMessage
		handler1 *mocks.KafkaMessageHandler
		handler2 *mocks.KafkaMessageHandler
		handler3 *mocks.KafkaMessageHandler
		list     libkafka.MessageHanderList
	)

	BeforeEach(func() {
		ctx = context.Background()
		msg = &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    0,
		}
		handler1 = &mocks.KafkaMessageHandler{}
		handler2 = &mocks.KafkaMessageHandler{}
		handler3 = &mocks.KafkaMessageHandler{}
		list = libkafka.MessageHanderList{handler1, handler2, handler3}
	})

	It("should implement MessageHandler interface", func() {
		var handler libkafka.MessageHandler = libkafka.MessageHanderList{}
		Expect(handler).NotTo(BeNil())
	})

	It("should call all handlers in order", func() {
		handler1.ConsumeMessageReturns(nil)
		handler2.ConsumeMessageReturns(nil)
		handler3.ConsumeMessageReturns(nil)

		err := list.ConsumeMessage(ctx, msg)
		Expect(err).To(BeNil())

		Expect(handler1.ConsumeMessageCallCount()).To(Equal(1))
		Expect(handler2.ConsumeMessageCallCount()).To(Equal(1))
		Expect(handler3.ConsumeMessageCallCount()).To(Equal(1))

		ctx1, msg1 := handler1.ConsumeMessageArgsForCall(0)
		Expect(ctx1).To(Equal(ctx))
		Expect(msg1).To(Equal(msg))

		ctx2, msg2 := handler2.ConsumeMessageArgsForCall(0)
		Expect(ctx2).To(Equal(ctx))
		Expect(msg2).To(Equal(msg))

		ctx3, msg3 := handler3.ConsumeMessageArgsForCall(0)
		Expect(ctx3).To(Equal(ctx))
		Expect(msg3).To(Equal(msg))
	})

	It("should stop and return error if handler fails", func() {
		expectedErr := errors.New("handler error")
		handler1.ConsumeMessageReturns(nil)
		handler2.ConsumeMessageReturns(expectedErr)
		handler3.ConsumeMessageReturns(nil)

		err := list.ConsumeMessage(ctx, msg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("consume message failed"))
		Expect(err.Error()).To(ContainSubstring("handler error"))

		Expect(handler1.ConsumeMessageCallCount()).To(Equal(1))
		Expect(handler2.ConsumeMessageCallCount()).To(Equal(1))
		Expect(handler3.ConsumeMessageCallCount()).To(Equal(0))
	})

	It("should work with empty list", func() {
		emptyList := libkafka.MessageHanderList{}
		err := emptyList.ConsumeMessage(ctx, msg)
		Expect(err).To(BeNil())
	})

	It("should respect context cancellation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		handler1.ConsumeMessageReturns(nil)

		err := list.ConsumeMessage(ctx, msg)
		Expect(err).To(Equal(context.Canceled))
		Expect(handler1.ConsumeMessageCallCount()).To(Equal(0))
	})

	It("should stop on context cancellation between handlers", func() {
		handler1.ConsumeMessageStub = func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return nil
		}

		handler2.ConsumeMessageStub = func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			// This should not be called because context will be cancelled
			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())

		handler1.ConsumeMessageStub = func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			cancel()
			return nil
		}

		err := list.ConsumeMessage(ctx, msg)
		Expect(err).To(Equal(context.Canceled))
		Expect(handler1.ConsumeMessageCallCount()).To(Equal(1))
		Expect(handler2.ConsumeMessageCallCount()).To(Equal(0))
	})
})
