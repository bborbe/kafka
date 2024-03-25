// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	"github.com/IBM/sarama"
	kvmocks "github.com/bborbe/kv/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("MetricsMessageHandlerTx", func() {
	var err error
	var subMessageHandler *mocks.KafkaMessageHandlerTx
	BeforeEach(func() {
		ctx := context.Background()
		subMessageHandler = &mocks.KafkaMessageHandlerTx{}
		messageHandlerTx := kafka.NewMessageHandlerTxMetrics(
			subMessageHandler,
			kafka.NewMetrics(),
		)
		err = messageHandlerTx.ConsumeMessage(ctx, &kvmocks.Tx{}, &sarama.ConsumerMessage{
			Key:   []byte("hello"),
			Value: []byte("world"),
		})
	})
	It("returns no error", func() {
		Expect(err).To(BeNil())
	})
	It("calls sub message handler", func() {
		Expect(subMessageHandler.ConsumeMessageCallCount()).To(Equal(1))
	})
	It("with correct args", func() {
		argCtx, argTx, argMessage := subMessageHandler.ConsumeMessageArgsForCall(0)
		Expect(argCtx).NotTo(BeNil())
		Expect(argTx).NotTo(BeNil())
		Expect(argMessage).NotTo(BeNil())
		Expect(string(argMessage.Key)).To(Equal("hello"))
		Expect(string(argMessage.Value)).To(Equal("world"))
	})
})
