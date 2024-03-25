// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("MetricsMessageHandler", func() {
	var err error
	var subMessageHandler *mocks.KafkaMessageHandler
	BeforeEach(func() {
		ctx := context.Background()
		subMessageHandler = &mocks.KafkaMessageHandler{}
		messageHandler := kafka.NewMetricsMessageHandler(
			subMessageHandler,
			kafka.NewMetrics(),
		)
		err = messageHandler.ConsumeMessage(ctx, &sarama.ConsumerMessage{
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
		ctx, message := subMessageHandler.ConsumeMessageArgsForCall(0)
		Expect(ctx).NotTo(BeNil())
		Expect(string(message.Key)).To(Equal("hello"))
		Expect(string(message.Value)).To(Equal("world"))
	})
})
