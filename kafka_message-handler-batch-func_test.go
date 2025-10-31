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
)

var _ = Describe("MessageHandlerBatchFunc", func() {
	type contextKey string

	It("should implement MessageHandlerBatch interface", func() {
		var handler libkafka.MessageHandlerBatch = libkafka.MessageHandlerBatchFunc(func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
			return nil
		})
		Expect(handler).NotTo(BeNil())
	})

	It("should call the function when ConsumeMessages is called", func() {
		called := false
		handler := libkafka.MessageHandlerBatchFunc(
			func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
				called = true
				return nil
			},
		)

		messages := []*sarama.ConsumerMessage{
			{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    0,
			},
		}

		err := handler.ConsumeMessages(context.Background(), messages)
		Expect(err).To(BeNil())
		Expect(called).To(BeTrue())
	})

	It("should pass context and messages to the function", func() {
		var receivedCtx context.Context
		var receivedMessages []*sarama.ConsumerMessage

		handler := libkafka.MessageHandlerBatchFunc(
			func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
				receivedCtx = ctx
				receivedMessages = messages
				return nil
			},
		)

		ctx := context.WithValue(context.Background(), contextKey("test"), "value")
		messages := []*sarama.ConsumerMessage{
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

		err := handler.ConsumeMessages(ctx, messages)
		Expect(err).To(BeNil())
		Expect(receivedCtx).To(Equal(ctx))
		Expect(receivedMessages).To(Equal(messages))
	})

	It("should return error from function", func() {
		expectedErr := errors.New("test error")
		handler := libkafka.MessageHandlerBatchFunc(
			func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
				return expectedErr
			},
		)

		messages := []*sarama.ConsumerMessage{
			{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    0,
			},
		}

		err := handler.ConsumeMessages(context.Background(), messages)
		Expect(err).To(Equal(expectedErr))
	})

	It("should handle empty message slice", func() {
		called := false
		handler := libkafka.MessageHandlerBatchFunc(
			func(ctx context.Context, messages []*sarama.ConsumerMessage) error {
				called = true
				Expect(messages).To(BeEmpty())
				return nil
			},
		)

		err := handler.ConsumeMessages(context.Background(), []*sarama.ConsumerMessage{})
		Expect(err).To(BeNil())
		Expect(called).To(BeTrue())
	})
})
