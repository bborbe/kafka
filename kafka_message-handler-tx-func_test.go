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

var _ = Describe("MessageHandlerTxFunc", func() {
	type contextKey string

	var (
		ctx context.Context
		tx  *kvmocks.Tx
		msg *sarama.ConsumerMessage
	)

	BeforeEach(func() {
		ctx = context.Background()
		tx = &kvmocks.Tx{}
		msg = &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    0,
		}
	})

	It("should implement MessageHandlerTx interface", func() {
		var handler libkafka.MessageHandlerTx = libkafka.MessageHandlerTxFunc(func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
			return nil
		})
		Expect(handler).NotTo(BeNil())
	})

	It("should call the function when ConsumeMessage is called", func() {
		called := false
		handler := libkafka.MessageHandlerTxFunc(
			func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
				called = true
				return nil
			},
		)

		err := handler.ConsumeMessage(ctx, tx, msg)
		Expect(err).To(BeNil())
		Expect(called).To(BeTrue())
	})

	It("should pass context, transaction, and message to the function", func() {
		var receivedCtx context.Context
		var receivedTx libkv.Tx
		var receivedMsg *sarama.ConsumerMessage

		handler := libkafka.MessageHandlerTxFunc(
			func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
				receivedCtx = ctx
				receivedTx = tx
				receivedMsg = msg
				return nil
			},
		)

		ctx = context.WithValue(context.Background(), contextKey("test"), "value")
		msg = &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    123,
			Value:     []byte("test-value"),
		}

		err := handler.ConsumeMessage(ctx, tx, msg)
		Expect(err).To(BeNil())
		Expect(receivedCtx).To(Equal(ctx))
		Expect(receivedTx).To(Equal(tx))
		Expect(receivedMsg).To(Equal(msg))
	})

	It("should return error from function", func() {
		expectedErr := errors.New("test error")
		handler := libkafka.MessageHandlerTxFunc(
			func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
				return expectedErr
			},
		)

		err := handler.ConsumeMessage(ctx, tx, msg)
		Expect(err).To(Equal(expectedErr))
	})
})
