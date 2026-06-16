// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"bytes"
	"compress/gzip"
	"context"
	stderrors "errors"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
	kvmocks "github.com/bborbe/kv/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

func gzipValue(input []byte) []byte {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	_, _ = w.Write(input)
	_ = w.Close()
	return buf.Bytes()
}

var _ = Describe("NewGzipMessageHandlerTx", func() {
	var (
		ctx     context.Context
		tx      libkv.Tx
		inner   *mocks.KafkaMessageHandlerTx
		handler libkafka.MessageHandlerTx
	)

	BeforeEach(func() {
		ctx = context.Background()
		tx = &kvmocks.Tx{}
		inner = &mocks.KafkaMessageHandlerTx{}
		handler = libkafka.NewGzipMessageHandlerTx(inner)
	})

	It("decompresses values tagged with the gzip header and strips the header", func() {
		plain := []byte("hello world")
		msg := &sarama.ConsumerMessage{
			Value: gzipValue(plain),
			Headers: []*sarama.RecordHeader{
				{Key: []byte("foo"), Value: []byte("bar")},
				{Key: []byte(libkafka.GzipHeaderKey), Value: []byte(libkafka.GzipHeaderValue)},
			},
		}
		Expect(handler.ConsumeMessage(ctx, tx, msg)).To(BeNil())
		Expect(inner.ConsumeMessageCallCount()).To(Equal(1))
		_, _, passed := inner.ConsumeMessageArgsForCall(0)
		Expect(passed.Value).To(Equal(plain))
		Expect(libkafka.GzipActive(passed.Headers)).To(BeFalse())
		Expect(passed.Headers).To(HaveLen(1))
		Expect(string(passed.Headers[0].Key)).To(Equal("foo"))
	})

	It("passes values without the gzip marker through unmodified", func() {
		msg := &sarama.ConsumerMessage{
			Value:   []byte("plain text"),
			Headers: []*sarama.RecordHeader{{Key: []byte("foo"), Value: []byte("bar")}},
		}
		Expect(handler.ConsumeMessage(ctx, tx, msg)).To(BeNil())
		_, _, passed := inner.ConsumeMessageArgsForCall(0)
		Expect(passed.Value).To(Equal([]byte("plain text")))
		Expect(passed.Headers).To(HaveLen(1))
	})

	It("passes empty values through without inspecting headers", func() {
		msg := &sarama.ConsumerMessage{
			Value: nil,
			Headers: []*sarama.RecordHeader{
				{Key: []byte(libkafka.GzipHeaderKey), Value: []byte(libkafka.GzipHeaderValue)},
			},
		}
		Expect(handler.ConsumeMessage(ctx, tx, msg)).To(BeNil())
		_, _, passed := inner.ConsumeMessageArgsForCall(0)
		Expect(passed.Value).To(BeNil())
		// gzip header NOT stripped because decompression did not run.
		Expect(libkafka.GzipActive(passed.Headers)).To(BeTrue())
	})

	It("propagates a gzip decompression error and does not delegate", func() {
		msg := &sarama.ConsumerMessage{
			Value: []byte("not really gzipped"),
			Headers: []*sarama.RecordHeader{
				{Key: []byte(libkafka.GzipHeaderKey), Value: []byte(libkafka.GzipHeaderValue)},
			},
		}
		err := handler.ConsumeMessage(ctx, tx, msg)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("gzip decompress failed"))
		Expect(inner.ConsumeMessageCallCount()).To(Equal(0))
	})

	It("propagates the inner handler error", func() {
		inner.ConsumeMessageReturns(stderrors.New("inner-fail"))
		msg := &sarama.ConsumerMessage{Value: []byte("plain")}
		err := handler.ConsumeMessage(ctx, tx, msg)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("inner-fail"))
	})
})
