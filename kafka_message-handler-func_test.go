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

var _ = Describe("MessageHandlerFunc", func() {
	It("should implement MessageHandler interface", func() {
		var handler libkafka.MessageHandler = libkafka.MessageHandlerFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return nil
		})
		Expect(handler).NotTo(BeNil())
	})

	It("should call the function when ConsumeMessage is called", func() {
		called := false
		handler := libkafka.MessageHandlerFunc(
			func(ctx context.Context, msg *sarama.ConsumerMessage) error {
				called = true
				return nil
			},
		)

		msg := &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    0,
		}

		err := handler.ConsumeMessage(context.Background(), msg)
		Expect(err).To(BeNil())
		Expect(called).To(BeTrue())
	})

	It("should pass context and message to the function", func() {
		var receivedCtx context.Context
		var receivedMsg *sarama.ConsumerMessage

		handler := libkafka.MessageHandlerFunc(
			func(ctx context.Context, msg *sarama.ConsumerMessage) error {
				receivedCtx = ctx
				receivedMsg = msg
				return nil
			},
		)

		ctx := context.WithValue(context.Background(), "test", "value")
		msg := &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    123,
			Value:     []byte("test-value"),
		}

		err := handler.ConsumeMessage(ctx, msg)
		Expect(err).To(BeNil())
		Expect(receivedCtx).To(Equal(ctx))
		Expect(receivedMsg).To(Equal(msg))
	})

	It("should return error from function", func() {
		expectedErr := errors.New("test error")
		handler := libkafka.MessageHandlerFunc(
			func(ctx context.Context, msg *sarama.ConsumerMessage) error {
				return expectedErr
			},
		)

		msg := &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    0,
		}

		err := handler.ConsumeMessage(context.Background(), msg)
		Expect(err).To(Equal(expectedErr))
	})
})
