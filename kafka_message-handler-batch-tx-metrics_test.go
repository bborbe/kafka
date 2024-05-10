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
	var subMessageHandler *mocks.KafkaMessageHandlerBatchTx
	BeforeEach(func() {
		ctx := context.Background()
		subMessageHandler = &mocks.KafkaMessageHandlerBatchTx{}
		messageHandlerTx := kafka.NewMessageHandlerBatchTxMetrics(
			subMessageHandler,
			kafka.NewMetrics(),
		)
		err = messageHandlerTx.ConsumeMessages(ctx, &kvmocks.Tx{}, []*sarama.ConsumerMessage{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		})
	})
	It("returns no error", func() {
		Expect(err).To(BeNil())
	})
	It("calls sub message handler", func() {
		Expect(subMessageHandler.ConsumeMessagesCallCount()).To(Equal(1))
	})
	It("with correct args", func() {
		argCtx, argTx, argMessages := subMessageHandler.ConsumeMessagesArgsForCall(0)
		Expect(argCtx).NotTo(BeNil())
		Expect(argTx).NotTo(BeNil())
		Expect(argMessages).To(HaveLen(1))
		Expect(string(argMessages[0].Key)).To(Equal("hello"))
		Expect(string(argMessages[0].Value)).To(Equal("world"))
	})
})
