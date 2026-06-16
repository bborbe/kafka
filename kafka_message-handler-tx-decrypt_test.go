// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	stderrors "errors"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	kvmocks "github.com/bborbe/kv/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("NewDecryptMessageHandlerTx", func() {
	var (
		ctx     context.Context
		tx      libkv.Tx
		inner   *mocks.KafkaMessageHandlerTx
		handler libkafka.MessageHandlerTx
		called  int
	)

	BeforeEach(func() {
		ctx = context.Background()
		tx = &kvmocks.Tx{}
		inner = &mocks.KafkaMessageHandlerTx{}
		called = 0
		handler = libkafka.NewDecryptMessageHandlerTx(
			inner,
			func(_ context.Context, v []byte) ([]byte, error) {
				called++
				out := make([]byte, len(v))
				for i, b := range v {
					out[i] = b ^ 0x42
				}
				return out, nil
			},
		)
	})

	It("decrypts non-empty values and delegates with the mutated msg", func() {
		value := []byte("secret")
		msg := &sarama.ConsumerMessage{Value: value}
		Expect(handler.ConsumeMessage(ctx, tx, msg)).To(BeNil())
		Expect(called).To(Equal(1))
		Expect(inner.ConsumeMessageCallCount()).To(Equal(1))
		_, passedTx, passedMsg := inner.ConsumeMessageArgsForCall(0)
		Expect(passedTx).To(Equal(tx))
		Expect(passedMsg.Value).NotTo(Equal(value))
		Expect(passedMsg.Value).To(HaveLen(len(value)))
	})

	It("passes empty values through without invoking the modifier", func() {
		msg := &sarama.ConsumerMessage{Value: nil}
		Expect(handler.ConsumeMessage(ctx, tx, msg)).To(BeNil())
		Expect(called).To(Equal(0))
		Expect(inner.ConsumeMessageCallCount()).To(Equal(1))
	})

	It("passes zero-length values through without invoking the modifier", func() {
		msg := &sarama.ConsumerMessage{Value: []byte{}}
		Expect(handler.ConsumeMessage(ctx, tx, msg)).To(BeNil())
		Expect(called).To(Equal(0))
		Expect(inner.ConsumeMessageCallCount()).To(Equal(1))
	})

	It("propagates a decrypt error and does not delegate", func() {
		handler = libkafka.NewDecryptMessageHandlerTx(
			inner,
			func(_ context.Context, _ []byte) ([]byte, error) {
				return nil, stderrors.New("kapow")
			},
		)
		err := handler.ConsumeMessage(ctx, tx, &sarama.ConsumerMessage{Value: []byte("x")})
		Expect(err).NotTo(BeNil())
		Expect(errors.Cause(err).Error()).To(Equal("kapow"))
		Expect(inner.ConsumeMessageCallCount()).To(Equal(0))
	})

	It("propagates the inner handler error", func() {
		inner.ConsumeMessageReturns(stderrors.New("inner-fail"))
		err := handler.ConsumeMessage(ctx, tx, &sarama.ConsumerMessage{Value: []byte("x")})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("inner-fail"))
	})
})
