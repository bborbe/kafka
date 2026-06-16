// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"bytes"
	"compress/gzip"
	"context"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("GzipDecoder", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("should decompress gzip-compressed data", func() {
		input := "test data that was compressed"

		// Compress the data
		buf := &bytes.Buffer{}
		writer := gzip.NewWriter(buf)
		_, err := writer.Write([]byte(input))
		Expect(err).To(BeNil())
		err = writer.Close()
		Expect(err).To(BeNil())
		compressed := buf.Bytes()

		// Decompress using GzipDecoder
		decompressed, err := libkafka.GzipDecoder(ctx, compressed)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(input))
	})

	It("should decompress empty gzip data", func() {
		input := ""

		// Compress the data
		buf := &bytes.Buffer{}
		writer := gzip.NewWriter(buf)
		_, err := writer.Write([]byte(input))
		Expect(err).To(BeNil())
		err = writer.Close()
		Expect(err).To(BeNil())
		compressed := buf.Bytes()

		// Decompress using GzipDecoder
		decompressed, err := libkafka.GzipDecoder(ctx, compressed)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(input))
	})

	It("should decompress large gzip data", func() {
		input := bytes.Repeat([]byte("test data "), 1000)

		// Compress the data
		buf := &bytes.Buffer{}
		writer := gzip.NewWriter(buf)
		_, err := writer.Write(input)
		Expect(err).To(BeNil())
		err = writer.Close()
		Expect(err).To(BeNil())
		compressed := buf.Bytes()

		// Decompress using GzipDecoder
		decompressed, err := libkafka.GzipDecoder(ctx, compressed)
		Expect(err).To(BeNil())
		Expect(decompressed).To(Equal(input))
	})

	It("should fail with nil compressed data", func() {
		decompressed, err := libkafka.GzipDecoder(ctx, nil)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("compressed data cannot be nil"))
		Expect(decompressed).To(BeNil())
	})

	It("should fail with empty compressed data", func() {
		decompressed, err := libkafka.GzipDecoder(ctx, []byte{})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("compressed data cannot be empty"))
		Expect(decompressed).To(BeNil())
	})

	It("should fail with invalid gzip data", func() {
		invalidData := []byte("this is not gzip compressed data")

		decompressed, err := libkafka.GzipDecoder(ctx, invalidData)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("create gzip reader failed"))
		Expect(decompressed).To(BeNil())
	})

	It("should round-trip with NewGzipEncoder", func() {
		input := "test data for round-trip"

		// Encode using NewGzipEncoder
		encoder, err := libkafka.NewJsonEncoder(ctx, input)
		Expect(err).To(BeNil())
		jsonBytes, err := encoder.Encode()
		Expect(err).To(BeNil())
		compressed, err := libkafka.NewGzipEncoder(ctx, jsonBytes)
		Expect(err).To(BeNil())

		// Decode using GzipDecoder
		decompressed, err := libkafka.GzipDecoder(ctx, compressed)
		Expect(err).To(BeNil())
		Expect(string(decompressed)).To(Equal(`"test data for round-trip"`))
	})

	It("should decompress data compressed with different compression levels", func() {
		input := bytes.Repeat([]byte("test data "), 100)

		for _, level := range []int{gzip.NoCompression, gzip.BestSpeed, gzip.DefaultCompression, gzip.BestCompression} {
			// Compress with specific level
			buf := &bytes.Buffer{}
			writer, err := gzip.NewWriterLevel(buf, level)
			Expect(err).To(BeNil())
			_, err = writer.Write(input)
			Expect(err).To(BeNil())
			err = writer.Close()
			Expect(err).To(BeNil())
			compressed := buf.Bytes()

			// Decompress should work regardless of compression level
			decompressed, err := libkafka.GzipDecoder(ctx, compressed)
			Expect(err).To(BeNil())
			Expect(decompressed).To(Equal(input))
		}
	})
})

var _ = Describe("GzipActive", func() {
	It("returns true when the gzip header is present and active", func() {
		headers := []*sarama.RecordHeader{
			{Key: []byte(libkafka.GzipHeaderKey), Value: []byte(libkafka.GzipHeaderValue)},
		}
		Expect(libkafka.GzipActive(headers)).To(BeTrue())
	})

	It("returns false on nil headers", func() {
		Expect(libkafka.GzipActive(nil)).To(BeFalse())
	})

	It("returns false on empty headers", func() {
		Expect(libkafka.GzipActive([]*sarama.RecordHeader{})).To(BeFalse())
	})

	It("returns false when only other headers are present", func() {
		headers := []*sarama.RecordHeader{
			{Key: []byte("foo"), Value: []byte("bar")},
		}
		Expect(libkafka.GzipActive(headers)).To(BeFalse())
	})

	It("returns false when the gzip key is present but the value is wrong", func() {
		headers := []*sarama.RecordHeader{
			{Key: []byte(libkafka.GzipHeaderKey), Value: []byte("inactive")},
		}
		Expect(libkafka.GzipActive(headers)).To(BeFalse())
	})

	It("ignores nil entries in the slice", func() {
		headers := []*sarama.RecordHeader{
			nil,
			{Key: []byte(libkafka.GzipHeaderKey), Value: []byte(libkafka.GzipHeaderValue)},
		}
		Expect(libkafka.GzipActive(headers)).To(BeTrue())
	})

	It("returns false when a header has a nil Key", func() {
		headers := []*sarama.RecordHeader{
			{Key: nil, Value: []byte(libkafka.GzipHeaderValue)},
		}
		Expect(libkafka.GzipActive(headers)).To(BeFalse())
	})

	It("returns false when a header has a nil Value", func() {
		headers := []*sarama.RecordHeader{
			{Key: []byte(libkafka.GzipHeaderKey), Value: nil},
		}
		Expect(libkafka.GzipActive(headers)).To(BeFalse())
	})
})

var _ = Describe("RemoveGzipHeader", func() {
	It("strips the active gzip marker", func() {
		headers := []*sarama.RecordHeader{
			{Key: []byte("foo"), Value: []byte("bar")},
			{Key: []byte(libkafka.GzipHeaderKey), Value: []byte(libkafka.GzipHeaderValue)},
			{Key: []byte("baz"), Value: []byte("qux")},
		}
		out := libkafka.RemoveGzipHeader(headers)
		Expect(out).To(HaveLen(2))
		Expect(string(out[0].Key)).To(Equal("foo"))
		Expect(string(out[1].Key)).To(Equal("baz"))
	})

	It("returns an empty slice when there are no headers", func() {
		out := libkafka.RemoveGzipHeader(nil)
		Expect(out).To(HaveLen(0))
	})

	It("leaves non-gzip headers untouched", func() {
		headers := []*sarama.RecordHeader{
			{Key: []byte("foo"), Value: []byte("bar")},
		}
		out := libkafka.RemoveGzipHeader(headers)
		Expect(out).To(HaveLen(1))
		Expect(string(out[0].Key)).To(Equal("foo"))
	})

	It("drops nil entries", func() {
		headers := []*sarama.RecordHeader{
			nil,
			{Key: []byte("foo"), Value: []byte("bar")},
		}
		out := libkafka.RemoveGzipHeader(headers)
		Expect(out).To(HaveLen(1))
		Expect(string(out[0].Key)).To(Equal("foo"))
	})

	It("keeps a header with a nil Key (it is not the gzip marker)", func() {
		headers := []*sarama.RecordHeader{
			{Key: nil, Value: []byte(libkafka.GzipHeaderValue)},
		}
		out := libkafka.RemoveGzipHeader(headers)
		Expect(out).To(HaveLen(1))
		Expect(out[0].Key).To(BeNil())
	})

	It("keeps a header with a nil Value (it is not the gzip marker)", func() {
		headers := []*sarama.RecordHeader{
			{Key: []byte(libkafka.GzipHeaderKey), Value: nil},
		}
		out := libkafka.RemoveGzipHeader(headers)
		Expect(out).To(HaveLen(1))
		Expect(out[0].Value).To(BeNil())
	})
})
