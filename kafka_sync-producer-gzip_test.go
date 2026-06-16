// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"bytes"
	"compress/gzip"
	"context"
	stderrors "errors"
	"io"
	"strings"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("NewSyncProducerGzipValue", func() {
	var (
		ctx   context.Context
		inner *mocks.KafkaSyncProducer
		gzp   libkafka.SyncProducer
	)

	BeforeEach(func() {
		ctx = context.Background()
		inner = &mocks.KafkaSyncProducer{}
		gzp = libkafka.NewSyncProducerGzipValue(inner, 100)
	})

	Context("payload below the threshold", func() {
		It("passes the message through unmodified", func() {
			value := []byte("small payload")
			msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder(value)}
			_, _, err := gzp.SendMessage(ctx, msg)
			Expect(err).To(BeNil())
			Expect(inner.SendMessageCallCount()).To(Equal(1))

			_, sent := inner.SendMessageArgsForCall(0)
			encoded, eerr := sent.Value.Encode()
			Expect(eerr).To(BeNil())
			Expect(encoded).To(Equal(value))
			Expect(hasGzipHeader(sent.Headers)).To(BeFalse())
		})
	})

	Context("payload exactly at the threshold", func() {
		It("passes the message through unmodified (boundary: len == maxMsgBytes)", func() {
			value := make([]byte, 100) // exactly maxMsgBytes
			for i := range value {
				value[i] = 'x'
			}
			msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder(value)}
			_, _, err := gzp.SendMessage(ctx, msg)
			Expect(err).To(BeNil())
			Expect(inner.SendMessageCallCount()).To(Equal(1))

			_, sent := inner.SendMessageArgsForCall(0)
			encoded, eerr := sent.Value.Encode()
			Expect(eerr).To(BeNil())
			Expect(encoded).To(Equal(value))
			Expect(hasGzipHeader(sent.Headers)).To(BeFalse())
		})
	})

	Context("payload above the threshold", func() {
		It("compresses the value and adds the gzip header", func() {
			value := []byte(strings.Repeat("compressible payload ", 100))
			msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder(value)}
			_, _, err := gzp.SendMessage(ctx, msg)
			Expect(err).To(BeNil())
			Expect(inner.SendMessageCallCount()).To(Equal(1))

			_, sent := inner.SendMessageArgsForCall(0)
			Expect(hasGzipHeader(sent.Headers)).To(BeTrue())

			compressed, eerr := sent.Value.Encode()
			Expect(eerr).To(BeNil())

			reader, rerr := gzip.NewReader(bytes.NewReader(compressed))
			Expect(rerr).To(BeNil())
			defer reader.Close()
			out, ierr := io.ReadAll(reader)
			Expect(ierr).To(BeNil())
			Expect(out).To(Equal(value))
		})
	})

	Context("nil and empty values", func() {
		It("skips compression when Value is nil", func() {
			msg := &sarama.ProducerMessage{Value: nil}
			_, _, err := gzp.SendMessage(ctx, msg)
			Expect(err).To(BeNil())
			Expect(hasGzipHeader(msg.Headers)).To(BeFalse())
		})

		It("skips compression when Value is empty", func() {
			msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder(nil)}
			_, _, err := gzp.SendMessage(ctx, msg)
			Expect(err).To(BeNil())
			Expect(hasGzipHeader(msg.Headers)).To(BeFalse())
		})
	})

	Context("SendMessages", func() {
		It("compresses each payload above the threshold", func() {
			big := []byte(strings.Repeat("x", 200))
			small := []byte("ok")
			msgs := []*sarama.ProducerMessage{
				{Value: sarama.ByteEncoder(big)},
				{Value: sarama.ByteEncoder(small)},
			}
			Expect(gzp.SendMessages(ctx, msgs)).To(BeNil())
			Expect(hasGzipHeader(msgs[0].Headers)).To(BeTrue())
			Expect(hasGzipHeader(msgs[1].Headers)).To(BeFalse())
			Expect(inner.SendMessagesCallCount()).To(Equal(1))
		})
	})

	Context("inner producer failures propagate", func() {
		It("returns the inner SendMessage error", func() {
			inner.SendMessageReturns(0, 0, stderrors.New("banana"))
			msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder([]byte("ok"))}
			_, _, err := gzp.SendMessage(ctx, msg)
			Expect(err).NotTo(BeNil())
		})
	})

	Context("negative maxMsgBytes falls back to the default", func() {
		It("uses GzipMaxMsgBytes when maxMsgBytes < 0", func() {
			fallback := libkafka.NewSyncProducerGzipValue(inner, -1)
			// A small payload is well below the 1 MiB default, so no compression.
			msg := &sarama.ProducerMessage{Value: sarama.ByteEncoder([]byte("ok"))}
			_, _, err := fallback.SendMessage(ctx, msg)
			Expect(err).To(BeNil())
			_, sent := inner.SendMessageArgsForCall(0)
			Expect(hasGzipHeader(sent.Headers)).To(BeFalse())
		})
	})

	Context("nil-Headers safety", func() {
		It("appends the gzip header even when msg.Headers is nil", func() {
			value := []byte(strings.Repeat("x", 200))
			msg := &sarama.ProducerMessage{
				Value:   sarama.ByteEncoder(value),
				Headers: nil,
			}
			_, _, err := gzp.SendMessage(ctx, msg)
			Expect(err).To(BeNil())
			_, sent := inner.SendMessageArgsForCall(0)
			Expect(hasGzipHeader(sent.Headers)).To(BeTrue())
		})
	})

	Context("cancelled ctx in SendMessages", func() {
		It("returns ctx.Err() without compressing or sending", func() {
			cctx, cancel := context.WithCancel(context.Background())
			cancel()
			msgs := []*sarama.ProducerMessage{
				{Value: sarama.ByteEncoder([]byte(strings.Repeat("x", 200)))},
				{Value: sarama.ByteEncoder([]byte(strings.Repeat("y", 200)))},
			}
			err := gzp.SendMessages(cctx, msgs)
			Expect(err).To(MatchError(context.Canceled))
			Expect(inner.SendMessagesCallCount()).To(Equal(0))
		})
	})

	Context("Close", func() {
		It("closes the inner producer", func() {
			Expect(gzp.Close()).To(BeNil())
			Expect(inner.CloseCallCount()).To(Equal(1))
		})
	})
})

func hasGzipHeader(headers []sarama.RecordHeader) bool {
	for _, h := range headers {
		if string(h.Key) == libkafka.GzipHeaderKey && string(h.Value) == libkafka.GzipHeaderValue {
			return true
		}
	}
	return false
}
