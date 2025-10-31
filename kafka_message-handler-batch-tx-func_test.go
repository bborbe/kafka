// Copyright (c) 2024 Benjamin Borbe All rights reserved.
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
)

var _ = Describe("MessageHandlerBatchTxFunc", func() {
	type contextKey string

	var (
		ctx      context.Context
		tx       *kvmocks.Tx
		messages []*sarama.ConsumerMessage
	)

	BeforeEach(func() {
		ctx = context.Background()
		tx = &kvmocks.Tx{}
		messages = []*sarama.ConsumerMessage{
			{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    0,
			},
		}
	})

	It("should implement MessageHandlerBatchTx interface", func() {
		var handler libkafka.MessageHandlerBatchTx = libkafka.MessageHandlerBatchTxFunc(func(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
			return nil
		})
		Expect(handler).NotTo(BeNil())
	})

	It("should call the function when ConsumeMessages is called", func() {
		called := false
		handler := libkafka.MessageHandlerBatchTxFunc(
			func(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
				called = true
				return nil
			},
		)

		err := handler.ConsumeMessages(ctx, tx, messages)
		Expect(err).To(BeNil())
		Expect(called).To(BeTrue())
	})

	It("should pass context, transaction, and messages to the function", func() {
		var receivedCtx context.Context
		var receivedTx libkv.Tx
		var receivedMessages []*sarama.ConsumerMessage

		handler := libkafka.MessageHandlerBatchTxFunc(
			func(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
				receivedCtx = ctx
				receivedTx = tx
				receivedMessages = messages
				return nil
			},
		)

		ctx = context.WithValue(context.Background(), contextKey("test"), "value")
		messages = []*sarama.ConsumerMessage{
			{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    123,
				Value:     []byte("test-value-1"),
			},
			{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    124,
				Value:     []byte("test-value-2"),
			},
		}

		err := handler.ConsumeMessages(ctx, tx, messages)
		Expect(err).To(BeNil())
		Expect(receivedCtx).To(Equal(ctx))
		Expect(receivedTx).To(Equal(tx))
		Expect(receivedMessages).To(Equal(messages))
	})

	It("should return error from function", func() {
		expectedErr := errors.New("test error")
		handler := libkafka.MessageHandlerBatchTxFunc(
			func(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
				return expectedErr
			},
		)

		err := handler.ConsumeMessages(ctx, tx, messages)
		Expect(err).To(Equal(expectedErr))
	})

	It("should handle empty message slice", func() {
		called := false
		handler := libkafka.MessageHandlerBatchTxFunc(
			func(ctx context.Context, tx libkv.Tx, messages []*sarama.ConsumerMessage) error {
				called = true
				Expect(messages).To(BeEmpty())
				return nil
			},
		)

		err := handler.ConsumeMessages(ctx, tx, []*sarama.ConsumerMessage{})
		Expect(err).To(BeNil())
		Expect(called).To(BeTrue())
	})
})
