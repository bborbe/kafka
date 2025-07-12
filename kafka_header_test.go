// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("Header", func() {
	var header libkafka.Header

	BeforeEach(func() {
		header = make(libkafka.Header)
	})

	Describe("Add", func() {
		It("should add single value", func() {
			header.Add("key1", "value1")
			Expect(header["key1"]).To(Equal([]string{"value1"}))
		})

		It("should add multiple values to same key", func() {
			header.Add("key1", "value1")
			header.Add("key1", "value2")
			Expect(header["key1"]).To(Equal([]string{"value1", "value2"}))
		})

		It("should add values to different keys", func() {
			header.Add("key1", "value1")
			header.Add("key2", "value2")
			Expect(header["key1"]).To(Equal([]string{"value1"}))
			Expect(header["key2"]).To(Equal([]string{"value2"}))
		})
	})

	Describe("Set", func() {
		It("should set single value", func() {
			header.Set("key1", []string{"value1"})
			Expect(header["key1"]).To(Equal([]string{"value1"}))
		})

		It("should set multiple values", func() {
			header.Set("key1", []string{"value1", "value2"})
			Expect(header["key1"]).To(Equal([]string{"value1", "value2"}))
		})

		It("should replace existing values", func() {
			header.Add("key1", "oldvalue")
			header.Set("key1", []string{"newvalue"})
			Expect(header["key1"]).To(Equal([]string{"newvalue"}))
		})

		It("should set empty slice", func() {
			header.Add("key1", "value1")
			header.Set("key1", []string{})
			Expect(header["key1"]).To(Equal([]string{}))
		})
	})

	Describe("Get", func() {
		It("should return first value", func() {
			header.Add("key1", "value1")
			header.Add("key1", "value2")
			Expect(header.Get("key1")).To(Equal("value1"))
		})

		It("should return empty string for non-existent key", func() {
			Expect(header.Get("nonexistent")).To(Equal(""))
		})

		It("should return empty string for key with empty values", func() {
			header.Set("key1", []string{})
			Expect(header.Get("key1")).To(Equal(""))
		})
	})

	Describe("Remove", func() {
		It("should remove existing key", func() {
			header.Add("key1", "value1")
			header.Remove("key1")
			Expect(header["key1"]).To(BeNil())
		})

		It("should not fail for non-existent key", func() {
			header.Remove("nonexistent")
		})
	})

	Describe("AsSaramaHeaders", func() {
		It("should convert empty header", func() {
			result := header.AsSaramaHeaders()
			Expect(result).To(BeEmpty())
		})

		It("should convert single key-value", func() {
			header.Add("key1", "value1")
			result := header.AsSaramaHeaders()
			Expect(result).To(HaveLen(1))
			Expect(result[0].Key).To(Equal([]byte("key1")))
			Expect(result[0].Value).To(Equal([]byte("value1")))
		})

		It("should convert multiple values for same key", func() {
			header.Add("key1", "value1")
			header.Add("key1", "value2")
			result := header.AsSaramaHeaders()
			Expect(result).To(HaveLen(2))

			var headers []sarama.RecordHeader
			for _, h := range result {
				if string(h.Key) == "key1" {
					headers = append(headers, h)
				}
			}
			Expect(headers).To(HaveLen(2))
		})

		It("should convert multiple keys", func() {
			header.Add("key1", "value1")
			header.Add("key2", "value2")
			result := header.AsSaramaHeaders()
			Expect(result).To(HaveLen(2))
		})
	})
})

var _ = Describe("ParseHeader", func() {
	It("should parse empty sarama headers", func() {
		result := libkafka.ParseHeader([]*sarama.RecordHeader{})
		Expect(result).To(Equal(libkafka.Header{}))
	})

	It("should parse single sarama header", func() {
		saramaHeaders := []*sarama.RecordHeader{
			{Key: []byte("key1"), Value: []byte("value1")},
		}
		result := libkafka.ParseHeader(saramaHeaders)
		expected := libkafka.Header{"key1": []string{"value1"}}
		Expect(result).To(Equal(expected))
	})

	It("should parse multiple sarama headers with same key", func() {
		saramaHeaders := []*sarama.RecordHeader{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key1"), Value: []byte("value2")},
		}
		result := libkafka.ParseHeader(saramaHeaders)
		expected := libkafka.Header{"key1": []string{"value1", "value2"}}
		Expect(result).To(Equal(expected))
	})

	It("should parse multiple sarama headers with different keys", func() {
		saramaHeaders := []*sarama.RecordHeader{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		}
		result := libkafka.ParseHeader(saramaHeaders)
		expected := libkafka.Header{
			"key1": []string{"value1"},
			"key2": []string{"value2"},
		}
		Expect(result).To(Equal(expected))
	})
})
