// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	stderrors "errors"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("NewSyncProducerEncryptValue", func() {
	var (
		ctx    context.Context
		inner  *mocks.KafkaSyncProducer
		ep     libkafka.SyncProducer
		called int
	)

	BeforeEach(func() {
		ctx = context.Background()
		inner = &mocks.KafkaSyncProducer{}
		called = 0
		ep = libkafka.NewSyncProducerEncryptValue(
			inner,
			func(_ context.Context, v []byte) ([]byte, error) {
				called++
				out := make([]byte, len(v))
				for i, b := range v {
					out[i] = b ^ 0x42 // trivial reversible transform
				}
				return out, nil
			},
		)
	})

	It("encrypts non-empty values before delegating", func() {
		value := []byte("secret payload")
		msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder(value)}
		_, _, err := ep.SendMessage(ctx, msg)
		Expect(err).To(BeNil())
		Expect(called).To(Equal(1))
		Expect(inner.SendMessageCallCount()).To(Equal(1))

		_, sent := inner.SendMessageArgsForCall(0)
		encoded, eerr := sent.Value.Encode()
		Expect(eerr).To(BeNil())
		Expect(encoded).NotTo(Equal(value))
		Expect(encoded).To(HaveLen(len(value)))
	})

	It("skips encryption when Value is nil", func() {
		msg := &sarama.ProducerMessage{Value: nil}
		_, _, err := ep.SendMessage(ctx, msg)
		Expect(err).To(BeNil())
		Expect(called).To(Equal(0))
	})

	It("skips encryption when Value is empty", func() {
		msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder(nil)}
		_, _, err := ep.SendMessage(ctx, msg)
		Expect(err).To(BeNil())
		Expect(called).To(Equal(0))
	})

	It("propagates encryption errors", func() {
		ep = libkafka.NewSyncProducerEncryptValue(
			inner,
			func(_ context.Context, _ []byte) ([]byte, error) {
				return nil, stderrors.New("kapow")
			},
		)
		msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder([]byte("x"))}
		_, _, err := ep.SendMessage(ctx, msg)
		Expect(err).NotTo(BeNil())
		Expect(inner.SendMessageCallCount()).To(Equal(0))
	})

	It("encrypts each message in SendMessages", func() {
		msgs := []*sarama.ProducerMessage{
			{Value: sarama.ByteEncoder([]byte("a"))},
			{Value: sarama.ByteEncoder([]byte("b"))},
		}
		Expect(ep.SendMessages(ctx, msgs)).To(BeNil())
		Expect(called).To(Equal(2))
		Expect(inner.SendMessagesCallCount()).To(Equal(1))
	})

	It("closes the inner producer", func() {
		Expect(ep.Close()).To(BeNil())
		Expect(inner.CloseCallCount()).To(Equal(1))
	})
})
