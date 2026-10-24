// Copyright (c) 2026 Benjamin Borbe All rights reserved.
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

var _ = Describe("MessageHandlerBatchFilter", func() {
	var ctx context.Context
	var messages []*sarama.ConsumerMessage
	var handler *mocks.KafkaMessageHandlerBatch

	BeforeEach(func() {
		ctx = context.Background()
		messages = []*sarama.ConsumerMessage{
			{Topic: "test-topic", Partition: 0, Offset: 0},
			{Topic: "test-topic", Partition: 0, Offset: 1},
			{Topic: "test-topic", Partition: 0, Offset: 2},
			{Topic: "test-topic", Partition: 0, Offset: 3},
			{Topic: "test-topic", Partition: 0, Offset: 4},
		}
		handler = &mocks.KafkaMessageHandlerBatch{}
	})

	It("returns a MessageHandlerBatch", func() {
		filter := libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool { return false },
			handler,
		)
		var _ libkafka.MessageHandlerBatch = filter
		Expect(filter).NotTo(BeNil())
	})

	It("passes only the surviving messages, in their original order, in one call", func() {
		filter := libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool {
				return msg.Offset == 1 || msg.Offset == 3
			},
			handler,
		)

		err := filter.ConsumeMessages(ctx, messages)
		Expect(err).To(BeNil())

		_, kept := handler.ConsumeMessagesArgsForCall(0)
		Expect(kept).To(Equal([]*sarama.ConsumerMessage{
			messages[0],
			messages[2],
			messages[4],
		}))
		Expect(handler.ConsumeMessagesCallCount()).To(Equal(1))
	})

	It("does not call the wrapped handler for an empty batch", func() {
		filter := libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool { return false },
			handler,
		)

		err := filter.ConsumeMessages(ctx, []*sarama.ConsumerMessage{})
		Expect(err).To(BeNil())
		Expect(handler.ConsumeMessagesCallCount()).To(Equal(0))
	})

	It("does not call the wrapped handler when every message is dropped", func() {
		filter := libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool { return true },
			handler,
		)

		err := filter.ConsumeMessages(ctx, messages)
		Expect(err).To(BeNil())
		Expect(handler.ConsumeMessagesCallCount()).To(Equal(0))
	})

	It("does not mutate the caller's slice", func() {
		original := make([]*sarama.ConsumerMessage, len(messages))
		copy(original, messages)

		filter := libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool { return msg.Offset == 2 },
			handler,
		)

		err := filter.ConsumeMessages(ctx, messages)
		Expect(err).To(BeNil())
		Expect(messages).To(Equal(original))
		for i := range original {
			Expect(messages[i]).To(BeIdenticalTo(original[i]))
		}
	})

	It("returns the wrapped handler's error unchanged", func() {
		sentinelErr := errors.New("sentinel")
		handler.ConsumeMessagesReturns(sentinelErr)
		filter := libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool { return false },
			handler,
		)

		err := filter.ConsumeMessages(ctx, messages)
		Expect(errors.Is(err, sentinelErr)).To(BeTrue())
		Expect(err).To(Equal(sentinelErr))
	})

	It("propagates a panic from the predicate", func() {
		filter := libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool { panic("boom") },
			handler,
		)

		Expect(func() {
			_ = filter.ConsumeMessages(ctx, messages)
		}).To(Panic())
	})
})
