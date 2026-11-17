// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
	kvmocks "github.com/bborbe/kv/mocks"
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

var _ = Describe("MessageHandlerBatchFilter composed outside a transaction", func() {
	var ctx context.Context
	var messages []*sarama.ConsumerMessage
	var skip func(*sarama.ConsumerMessage) bool
	var db *kvmocks.DB
	var batchTx *mocks.KafkaMessageHandlerBatchTx
	var handler libkafka.MessageHandlerBatch

	BeforeEach(func() {
		ctx = context.Background()
		messages = []*sarama.ConsumerMessage{
			{Topic: "test-topic", Partition: 0, Offset: 0},
			{Topic: "test-topic", Partition: 0, Offset: 1},
		}
		db = &kvmocks.DB{}
		batchTx = &mocks.KafkaMessageHandlerBatchTx{}
		handler = libkafka.NewMessageHandlerBatchFilter(
			func(msg *sarama.ConsumerMessage) bool { return skip(msg) },
			libkafka.NewMessageHandlerBatchTxUpdate(db, batchTx),
		)
	})

	It("opens no transaction when the predicate drops every message", func() {
		skip = func(msg *sarama.ConsumerMessage) bool { return true }

		err := handler.ConsumeMessages(ctx, messages)
		Expect(err).To(BeNil())

		// Acceptance Criterion 4: a message dropped by the filter never reaches the
		// transaction-opening handler. Nothing signals this miscomposition at runtime,
		// so this assertion is the only guard against a filter placed inside db.Update.
		Expect(db.UpdateCallCount()).To(Equal(0))
		Expect(batchTx.ConsumeMessagesCallCount()).To(Equal(0))
	})

	It("opens exactly one transaction for a mixed batch and passes only the survivor", func() {
		skip = func(msg *sarama.ConsumerMessage) bool { return msg.Offset == 0 }
		// Without this stub kvmocks.DB.Update returns its zero-value error without
		// running the callback, so batchTx would never be reached.
		db.UpdateCalls(
			func(ctx context.Context, fn func(ctx context.Context, tx libkv.Tx) error) error {
				return fn(ctx, &kvmocks.Tx{})
			},
		)

		err := handler.ConsumeMessages(ctx, messages)
		Expect(err).To(BeNil())

		Expect(db.UpdateCallCount()).To(Equal(1))
		Expect(batchTx.ConsumeMessagesCallCount()).To(Equal(1))

		_, _, argMessages := batchTx.ConsumeMessagesArgsForCall(0)
		Expect(argMessages).To(Equal([]*sarama.ConsumerMessage{messages[1]}))
		Expect(argMessages).To(HaveLen(1))
	})

	It("returns the wrapped handler's error", func() {
		sentinelErr := errors.New("sentinel")
		batchTx.ConsumeMessagesReturns(sentinelErr)
		skip = func(msg *sarama.ConsumerMessage) bool { return msg.Offset == 0 }
		db.UpdateCalls(
			func(ctx context.Context, fn func(ctx context.Context, tx libkv.Tx) error) error {
				return fn(ctx, &kvmocks.Tx{})
			},
		)

		err := handler.ConsumeMessages(ctx, messages)
		Expect(errors.Is(err, sentinelErr)).To(BeTrue())
		Expect(db.UpdateCallCount()).To(Equal(1))
	})
})
