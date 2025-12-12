// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"

	libhttp "github.com/bborbe/http"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("OffsetManagerHandler", func() {
	var handler http.Handler
	var offsetManager *mocks.KafkaOffsetManager
	var cancel context.CancelFunc
	var req *http.Request
	var resp *httptest.ResponseRecorder

	BeforeEach(func() {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		_ = ctx
		offsetManager = &mocks.KafkaOffsetManager{}
		resp = httptest.NewRecorder()
	})

	Context("set offset", func() {
		BeforeEach(func() {
			handler = libhttp.NewErrorHandler(kafka.NewOffsetManagerHandler(offsetManager, cancel))
		})

		Context("with valid parameters", func() {
			BeforeEach(func() {
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "0")
				params.Set("offset", "100")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)
			})

			It("calls ResetOffset first", func() {
				handler.ServeHTTP(resp, req)
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(1))
				_, topic, partition, offset := offsetManager.ResetOffsetArgsForCall(0)
				Expect(topic).To(Equal(kafka.Topic("test-topic")))
				Expect(partition).To(Equal(kafka.Partition(0)))
				Expect(offset).To(Equal(kafka.Offset(100)))
			})

			It("calls MarkOffset after ResetOffset", func() {
				handler.ServeHTTP(resp, req)
				Expect(offsetManager.MarkOffsetCallCount()).To(Equal(1))
				_, topic, partition, offset := offsetManager.MarkOffsetArgsForCall(0)
				Expect(topic).To(Equal(kafka.Topic("test-topic")))
				Expect(partition).To(Equal(kafka.Partition(0)))
				Expect(offset).To(Equal(kafka.Offset(100)))
			})

			It("closes offset manager", func() {
				handler.ServeHTTP(resp, req)
				Expect(offsetManager.CloseCallCount()).To(Equal(1))
			})

			It("returns success message", func() {
				handler.ServeHTTP(resp, req)
				Expect(resp.Body.String()).To(ContainSubstring("set offset(100)"))
				Expect(resp.Body.String()).To(ContainSubstring("test-topic"))
				Expect(resp.Body.String()).To(ContainSubstring("completed"))
			})
		})

		Context("backward offset movement", func() {
			BeforeEach(func() {
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "0")
				params.Set("offset", "50")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)
			})

			It("allows setting offset backward via ResetOffset", func() {
				handler.ServeHTTP(resp, req)
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(1))
				_, _, _, offset := offsetManager.ResetOffsetArgsForCall(0)
				Expect(offset).To(Equal(kafka.Offset(50)))
			})
		})

		Context("with negative offset", func() {
			BeforeEach(func() {
				offsetManager.NextOffsetReturns(1000, nil)
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "0")
				params.Set("offset", "-10")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)
			})

			It("calculates offset relative to high water mark", func() {
				handler.ServeHTTP(resp, req)
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(1))
				_, _, _, offset := offsetManager.ResetOffsetArgsForCall(0)
				// -10 + 1000 (highWaterMark) = 990
				Expect(offset).To(Equal(kafka.Offset(990)))
			})
		})

		Context("without offset parameter", func() {
			BeforeEach(func() {
				offsetManager.NextOffsetReturns(500, nil)
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "0")
				req = httptest.NewRequest("POST", "/get?"+params.Encode(), nil)
			})

			It("returns current offset", func() {
				handler.ServeHTTP(resp, req)
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(0))
				Expect(offsetManager.MarkOffsetCallCount()).To(Equal(0))
				Expect(resp.Body.String()).To(ContainSubstring("next offset is 500"))
			})
		})

		Context("missing topic parameter", func() {
			BeforeEach(func() {
				params := url.Values{}
				params.Set("partition", "0")
				params.Set("offset", "100")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)
			})

			It("returns error", func() {
				handler.ServeHTTP(resp, req)
				Expect(resp.Code).To(Equal(http.StatusInternalServerError))
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(0))
			})
		})

		Context("missing partition parameter", func() {
			BeforeEach(func() {
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("offset", "100")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)
			})

			It("returns error", func() {
				handler.ServeHTTP(resp, req)
				Expect(resp.Code).To(Equal(http.StatusInternalServerError))
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(0))
			})
		})

		Context("invalid partition format", func() {
			BeforeEach(func() {
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "invalid")
				params.Set("offset", "100")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)
			})

			It("returns error", func() {
				handler.ServeHTTP(resp, req)
				Expect(resp.Code).To(Equal(http.StatusInternalServerError))
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(0))
			})
		})

		Context("invalid offset format", func() {
			BeforeEach(func() {
				offsetManager.NextOffsetReturns(500, nil)
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "0")
				params.Set("offset", "not-a-number")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)
			})

			It("falls back to get current offset", func() {
				handler.ServeHTTP(resp, req)
				Expect(offsetManager.NextOffsetCallCount()).To(Equal(1))
				Expect(offsetManager.ResetOffsetCallCount()).To(Equal(0))
				Expect(offsetManager.MarkOffsetCallCount()).To(Equal(0))
			})
		})

		Context("with multiple partitions", func() {
			It("handles partition 0", func() {
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "0")
				params.Set("offset", "100")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)

				handler.ServeHTTP(resp, req)
				_, _, partition, _ := offsetManager.ResetOffsetArgsForCall(0)
				Expect(partition).To(Equal(kafka.Partition(0)))
			})

			It("handles partition 5", func() {
				params := url.Values{}
				params.Set("topic", "test-topic")
				params.Set("partition", "5")
				params.Set("offset", "200")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)

				resp = httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				_, _, partition, _ := offsetManager.ResetOffsetArgsForCall(0)
				Expect(partition).To(Equal(kafka.Partition(5)))
			})
		})

		Context("with different topics", func() {
			It("handles topic-a", func() {
				params := url.Values{}
				params.Set("topic", "topic-a")
				params.Set("partition", "0")
				params.Set("offset", "100")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)

				handler.ServeHTTP(resp, req)
				_, topic, _, _ := offsetManager.ResetOffsetArgsForCall(0)
				Expect(topic).To(Equal(kafka.Topic("topic-a")))
			})

			It("handles topic-b", func() {
				params := url.Values{}
				params.Set("topic", "topic-b")
				params.Set("partition", "0")
				params.Set("offset", "200")
				req = httptest.NewRequest("POST", "/set?"+params.Encode(), nil)

				resp = httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				_, topic, _, _ := offsetManager.ResetOffsetArgsForCall(0)
				Expect(topic).To(Equal(kafka.Topic("topic-b")))
			})
		})
	})
})
