// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("NewJsonEncoder", func() {
	It("should encode simple string", func() {
		encoder, err := libkafka.NewJsonEncoder(context.Background(), "test")
		Expect(err).To(BeNil())
		Expect(encoder).NotTo(BeNil())

		bytes, err := encoder.Encode()
		Expect(err).To(BeNil())
		Expect(string(bytes)).To(Equal(`"test"`))
	})

	It("should encode number", func() {
		encoder, err := libkafka.NewJsonEncoder(context.Background(), 123)
		Expect(err).To(BeNil())
		Expect(encoder).NotTo(BeNil())

		bytes, err := encoder.Encode()
		Expect(err).To(BeNil())
		Expect(string(bytes)).To(Equal("123"))
	})

	It("should encode struct", func() {
		type TestStruct struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}

		input := TestStruct{Name: "test", Value: 123}
		encoder, err := libkafka.NewJsonEncoder(context.Background(), input)
		Expect(err).To(BeNil())
		Expect(encoder).NotTo(BeNil())

		bytes, err := encoder.Encode()
		Expect(err).To(BeNil())
		Expect(string(bytes)).To(Equal(`{"name":"test","value":123}`))
	})

	It("should encode slice", func() {
		input := []string{"a", "b", "c"}
		encoder, err := libkafka.NewJsonEncoder(context.Background(), input)
		Expect(err).To(BeNil())
		Expect(encoder).NotTo(BeNil())

		bytes, err := encoder.Encode()
		Expect(err).To(BeNil())
		Expect(string(bytes)).To(Equal(`["a","b","c"]`))
	})

	It("should encode map", func() {
		input := map[string]int{"a": 1, "b": 2}
		encoder, err := libkafka.NewJsonEncoder(context.Background(), input)
		Expect(err).To(BeNil())
		Expect(encoder).NotTo(BeNil())

		bytes, err := encoder.Encode()
		Expect(err).To(BeNil())
		// Map encoding order is not guaranteed, so we check both possibilities
		result := string(bytes)
		Expect(result).To(SatisfyAny(
			Equal(`{"a":1,"b":2}`),
			Equal(`{"b":2,"a":1}`),
		))
	})

	It("should encode nil", func() {
		encoder, err := libkafka.NewJsonEncoder(context.Background(), nil)
		Expect(err).To(BeNil())
		Expect(encoder).NotTo(BeNil())

		bytes, err := encoder.Encode()
		Expect(err).To(BeNil())
		Expect(string(bytes)).To(Equal("null"))
	})

	It("should return sarama.ByteEncoder", func() {
		encoder, err := libkafka.NewJsonEncoder(context.Background(), "test")
		Expect(err).To(BeNil())
		Expect(encoder).To(BeAssignableToTypeOf(sarama.ByteEncoder(nil)))
	})

	It("should fail for non-marshalable value", func() {
		// Function types cannot be marshaled to JSON
		input := func() {}
		encoder, err := libkafka.NewJsonEncoder(context.Background(), input)
		Expect(err).NotTo(BeNil())
		Expect(encoder).To(BeNil())
	})
})
