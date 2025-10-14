// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("NewGzipEncoder", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("should compress simple string", func() {
		input := "test data that should be compressed"
		encoder := []byte(input)

		compressed, err := libkafka.NewGzipEncoder(ctx, encoder)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's actually gzipped by decompressing
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(input))
	})

	It("should compress empty data", func() {
		input := ""
		encoder := []byte(input)

		compressed, err := libkafka.NewGzipEncoder(ctx, encoder)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's actually gzipped by decompressing
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(input))
	})

	It("should compress large data", func() {
		// Create a large repeating string that should compress well
		input := bytes.Repeat([]byte("test data "), 1000)

		compressed, err := libkafka.NewGzipEncoder(ctx, input)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify compression actually occurred
		Expect(len(compressed)).To(BeNumerically("<", len(input)))

		// Verify it's actually gzipped by decompressing
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(decompressed).To(Equal(input))
	})

	It("should handle nil data", func() {
		compressed, err := libkafka.NewGzipEncoder(ctx, nil)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's valid gzip (compressed empty data)
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(decompressed).To(HaveLen(0))
	})

	It("should return sarama.ByteEncoder", func() {
		compressed, err := libkafka.NewGzipEncoder(ctx, []byte("test"))
		Expect(err).To(BeNil())
		Expect(compressed).To(BeAssignableToTypeOf(sarama.ByteEncoder(nil)))
	})

	It("should work with JSON-encoded data", func() {
		type TestStruct struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}

		input := TestStruct{Name: "test", Value: 123}
		jsonEncoder, err := libkafka.NewJsonEncoder(ctx, input)
		Expect(err).To(BeNil())

		jsonBytes, err := jsonEncoder.Encode()
		Expect(err).To(BeNil())

		compressed, err := libkafka.NewGzipEncoder(ctx, jsonBytes)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's actually gzipped by decompressing
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(`{"name":"test","value":123}`))
	})
})

var _ = Describe("NewGzipEncoderWithLevel", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("should compress with NoCompression level", func() {
		input := "test data"

		compressed, err := libkafka.NewGzipEncoderWithLevel(ctx, []byte(input), gzip.NoCompression)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's still valid gzip (even with no compression)
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(input))
	})

	It("should compress with BestSpeed level", func() {
		input := bytes.Repeat([]byte("test data "), 100)

		compressed, err := libkafka.NewGzipEncoderWithLevel(ctx, input, gzip.BestSpeed)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's valid gzip
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(decompressed).To(Equal(input))
	})

	It("should compress with BestCompression level", func() {
		input := bytes.Repeat([]byte("test data "), 100)

		compressed, err := libkafka.NewGzipEncoderWithLevel(ctx, input, gzip.BestCompression)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's valid gzip
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(decompressed).To(Equal(input))
	})

	It("should compress with DefaultCompression level", func() {
		input := "test data"

		compressed, err := libkafka.NewGzipEncoderWithLevel(
			ctx,
			[]byte(input),
			gzip.DefaultCompression,
		)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's valid gzip
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(input))
	})

	It("should fail with invalid compression level", func() {
		input := "test data"

		compressed, err := libkafka.NewGzipEncoderWithLevel(ctx, []byte(input), 999)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("create gzip writer failed"))
		Expect(compressed).To(BeNil())
	})

	It("should handle nil data", func() {
		compressed, err := libkafka.NewGzipEncoderWithLevel(ctx, nil, gzip.DefaultCompression)
		Expect(err).To(BeNil())
		Expect(compressed).NotTo(BeNil())

		// Verify it's valid gzip (compressed empty data)
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		Expect(err).To(BeNil())
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		Expect(err).To(BeNil())
		Expect(decompressed).To(HaveLen(0))
	})

	It("should produce different sizes for different compression levels", func() {
		// Use highly compressible data
		input := bytes.Repeat([]byte("test data "), 1000)

		compressedFast, err := libkafka.NewGzipEncoderWithLevel(ctx, input, gzip.BestSpeed)
		Expect(err).To(BeNil())

		compressedBest, err := libkafka.NewGzipEncoderWithLevel(ctx, input, gzip.BestCompression)
		Expect(err).To(BeNil())

		// BestCompression should produce smaller output than BestSpeed
		Expect(len(compressedBest)).To(BeNumerically("<", len(compressedFast)))
	})
})
