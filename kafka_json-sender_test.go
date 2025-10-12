// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	logmocks "github.com/bborbe/log/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

type testValue struct {
	Name string `json:"name"`
}

func (t testValue) Validate(ctx context.Context) error {
	if t.Name == "" {
		return errors.New("name is required")
	}
	return nil
}

var _ = Describe("JsonSender", func() {
	var (
		mockProducer       *mocks.KafkaSyncProducer
		mockLogSampler     *logmocks.LogSampler
		mockSamplerFactory *logmocks.LogSamplerFactory
		jsonSender         libkafka.JsonSender
		ctx                context.Context
	)

	BeforeEach(func() {
		mockProducer = &mocks.KafkaSyncProducer{}
		mockLogSampler = &logmocks.LogSampler{}
		mockSamplerFactory = &logmocks.LogSamplerFactory{}

		mockSamplerFactory.SamplerReturns(mockLogSampler)
		mockLogSampler.IsSampleReturns(false)

		jsonSender = libkafka.NewJsonSender(mockProducer, mockSamplerFactory)
		ctx = context.Background()
	})

	Describe("SendUpdate", func() {
		It("should send update message successfully", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")
			value := testValue{Name: "test"}

			mockProducer.SendMessageReturns(int32(0), int64(123), nil)

			err := jsonSender.SendUpdate(ctx, topic, key, value)
			Expect(err).To(BeNil())

			Expect(mockProducer.SendMessageCallCount()).To(Equal(1))
			_, msg := mockProducer.SendMessageArgsForCall(0)
			Expect(msg.Topic).To(Equal("test-topic"))
			Expect(msg.Key).To(Equal(sarama.ByteEncoder("test-key")))
			Expect(msg.Value).NotTo(BeNil())
		})

		It("should send update message with headers", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")
			value := testValue{Name: "test"}
			headers := []sarama.RecordHeader{
				{Key: []byte("header1"), Value: []byte("value1")},
			}

			mockProducer.SendMessageReturns(int32(0), int64(123), nil)

			err := jsonSender.SendUpdate(ctx, topic, key, value, headers...)
			Expect(err).To(BeNil())

			Expect(mockProducer.SendMessageCallCount()).To(Equal(1))
			_, msg := mockProducer.SendMessageArgsForCall(0)
			Expect(msg.Headers).To(Equal(headers))
		})

		It("should return error when validation fails", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")
			value := testValue{Name: ""} // Invalid value

			err := jsonSender.SendUpdate(ctx, topic, key, value)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("validate value failed"))

			Expect(mockProducer.SendMessageCallCount()).To(Equal(0))
		})

		It("should return error when producer fails", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")
			value := testValue{Name: "test"}

			expectedErr := errors.New("producer error")
			mockProducer.SendMessageReturns(int32(0), int64(0), expectedErr)

			err := jsonSender.SendUpdate(ctx, topic, key, value)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("send update message failed"))
		})
	})

	Describe("SendDelete", func() {
		It("should send delete message successfully", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")

			mockProducer.SendMessageReturns(int32(0), int64(123), nil)

			err := jsonSender.SendDelete(ctx, topic, key)
			Expect(err).To(BeNil())

			Expect(mockProducer.SendMessageCallCount()).To(Equal(1))
			_, msg := mockProducer.SendMessageArgsForCall(0)
			Expect(msg.Topic).To(Equal("test-topic"))
			Expect(msg.Key).To(Equal(sarama.ByteEncoder("test-key")))
			Expect(msg.Value).To(BeNil()) // Delete messages have nil value
		})

		It("should send delete message with headers", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")
			headers := []sarama.RecordHeader{
				{Key: []byte("header1"), Value: []byte("value1")},
			}

			mockProducer.SendMessageReturns(int32(0), int64(123), nil)

			err := jsonSender.SendDelete(ctx, topic, key, headers...)
			Expect(err).To(BeNil())

			Expect(mockProducer.SendMessageCallCount()).To(Equal(1))
			_, msg := mockProducer.SendMessageArgsForCall(0)
			Expect(msg.Headers).To(Equal(headers))
		})

		It("should return error when producer fails", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")

			expectedErr := errors.New("producer error")
			mockProducer.SendMessageReturns(int32(0), int64(0), expectedErr)

			err := jsonSender.SendDelete(ctx, topic, key)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("send delete message failed"))
		})
	})

	Describe("with validation disabled", func() {
		BeforeEach(func() {
			jsonSender = libkafka.NewJsonSender(
				mockProducer,
				mockSamplerFactory,
				func(options *libkafka.JsonSenderOptions) {
					options.ValidationDisabled = true
				},
			)
		})

		It("should send update even with invalid value", func() {
			topic := libkafka.Topic("test-topic")
			key := libkafka.NewKey("test-key")
			value := testValue{Name: ""} // Invalid value, but validation is disabled

			mockProducer.SendMessageReturns(int32(0), int64(123), nil)

			err := jsonSender.SendUpdate(ctx, topic, key, value)
			Expect(err).To(BeNil())

			Expect(mockProducer.SendMessageCallCount()).To(Equal(1))
		})
	})
})

var _ = Describe("Key", func() {
	Describe("NewKey", func() {
		It("should create key from string", func() {
			key := libkafka.NewKey("test-string")
			Expect(key.String()).To(Equal("test-string"))
			Expect(key.Bytes()).To(Equal([]byte("test-string")))
		})

		It("should create key from bytes", func() {
			input := []byte("test-bytes")
			key := libkafka.NewKey(input)
			Expect(key.String()).To(Equal("test-bytes"))
			Expect(key.Bytes()).To(Equal(input))
		})
	})

	Describe("ParseKey", func() {
		It("should parse string", func() {
			key, err := libkafka.ParseKey(context.Background(), "test")
			Expect(err).To(BeNil())
			Expect(key).NotTo(BeNil())
			Expect((*key).String()).To(Equal("test"))
		})

		It("should parse number as string", func() {
			key, err := libkafka.ParseKey(context.Background(), 123)
			Expect(err).To(BeNil())
			Expect(key).NotTo(BeNil())
			Expect((*key).String()).To(Equal("123"))
		})

		It("should return error for unparsable value", func() {
			key, err := libkafka.ParseKey(context.Background(), func() {})
			Expect(err).NotTo(BeNil())
			Expect(key).To(BeNil())
		})
	})
})
