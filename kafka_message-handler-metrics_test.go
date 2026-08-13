// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	stdtime "time"

	"github.com/IBM/sarama"
	libtime "github.com/bborbe/time"
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
		messageHandler := kafka.NewMessageHandlerMetrics(
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

var _ = Describe("MetricsMessageHandler duration clock", func() {
	var originalNow func() stdtime.Time

	BeforeEach(func() { originalNow = libtime.Now })
	AfterEach(func() { libtime.Now = originalNow })

	// Regression: start and elapsed must read the SAME clock. If either call
	// site still used the real clock the measured duration would be ~0 instead
	// of the 7s the fake clock advances by.
	It("measures the duration with the injectable clock", func() {
		current := stdtime.Date(2026, 8, 13, 12, 0, 0, 0, stdtime.UTC)
		libtime.Now = func() stdtime.Time { return current }

		metrics := &mocks.KafkaMetrics{}
		sub := &mocks.KafkaMessageHandler{}
		sub.ConsumeMessageStub = func(context.Context, *sarama.ConsumerMessage) error {
			current = current.Add(7 * stdtime.Second)
			return nil
		}

		handler := kafka.NewMessageHandlerMetrics(sub, metrics)
		Expect(handler.ConsumeMessage(
			context.Background(),
			&sarama.ConsumerMessage{Topic: "t", Partition: 1},
		)).To(Succeed())

		Expect(metrics.MessageHandlerDurationMeasureCallCount()).To(Equal(1))
		_, _, duration := metrics.MessageHandlerDurationMeasureArgsForCall(0)
		Expect(duration).To(Equal(7 * stdtime.Second))
	})
})
