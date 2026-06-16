// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("Gzip header helpers", func() {
	Describe("GzipActive", func() {
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
	})

	Describe("RemoveGzipHeader", func() {
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
	})
})
