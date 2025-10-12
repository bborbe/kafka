// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"errors"

	logmocks "github.com/bborbe/log/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("NewUpdaterHandlerSkipErrors", func() {
	var (
		ctx               context.Context
		innerHandler      *mockUpdateHandler
		logSamplerFactory *logmocks.LogSamplerFactory
		logSampler        *logmocks.LogSampler
		skipHandler       libkafka.UpdaterHandler[string, string]
	)

	BeforeEach(func() {
		ctx = context.Background()
		innerHandler = &mockUpdateHandler{}
		logSamplerFactory = &logmocks.LogSamplerFactory{}
		logSampler = &logmocks.LogSampler{}
		logSamplerFactory.SamplerReturns(logSampler)

		skipHandler = libkafka.NewUpdaterHandlerSkipErrors[string, string](
			innerHandler,
			logSamplerFactory,
		)
	})

	It("should implement UpdaterHandler interface", func() {
		var _ libkafka.UpdaterHandler[string, string] = skipHandler
		Expect(skipHandler).NotTo(BeNil())
	})

	It("should call SamplerFactory.Sampler during construction", func() {
		Expect(logSamplerFactory.SamplerCallCount()).To(Equal(1))
	})

	Describe("Update", func() {
		It("should forward successful calls to inner handler", func() {
			innerHandler.updateErr = nil

			err := skipHandler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())

			Expect(innerHandler.updateCalled).To(BeTrue())
			Expect(innerHandler.lastKey).To(Equal("test-key"))
			Expect(innerHandler.lastObject).To(Equal("test-object"))
		})

		It("should skip errors and return nil when inner handler fails", func() {
			innerHandler.updateErr = errors.New("inner error")
			logSampler.IsSampleReturns(false)

			err := skipHandler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())

			Expect(innerHandler.updateCalled).To(BeTrue())
			Expect(logSampler.IsSampleCallCount()).To(Equal(1))
		})

		It("should log errors when sampling is enabled", func() {
			innerHandler.updateErr = errors.New("inner error")
			logSampler.IsSampleReturns(true)

			err := skipHandler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())

			Expect(innerHandler.updateCalled).To(BeTrue())
			Expect(logSampler.IsSampleCallCount()).To(Equal(1))
		})

		It("should not log errors when sampling is disabled", func() {
			innerHandler.updateErr = errors.New("inner error")
			logSampler.IsSampleReturns(false)

			err := skipHandler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())

			Expect(innerHandler.updateCalled).To(BeTrue())
			Expect(logSampler.IsSampleCallCount()).To(Equal(1))
		})
	})

	Describe("Delete", func() {
		It("should forward successful calls to inner handler", func() {
			innerHandler.deleteErr = nil

			err := skipHandler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())

			Expect(innerHandler.deleteCalled).To(BeTrue())
			Expect(innerHandler.lastKey).To(Equal("test-key"))
		})

		It("should skip errors and return nil when inner handler fails", func() {
			innerHandler.deleteErr = errors.New("inner error")
			logSampler.IsSampleReturns(false)

			err := skipHandler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())

			Expect(innerHandler.deleteCalled).To(BeTrue())
			Expect(logSampler.IsSampleCallCount()).To(Equal(1))
		})

		It("should log errors when sampling is enabled", func() {
			innerHandler.deleteErr = errors.New("inner error")
			logSampler.IsSampleReturns(true)

			err := skipHandler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())

			Expect(innerHandler.deleteCalled).To(BeTrue())
			Expect(logSampler.IsSampleCallCount()).To(Equal(1))
		})

		It("should not log errors when sampling is disabled", func() {
			innerHandler.deleteErr = errors.New("inner error")
			logSampler.IsSampleReturns(false)

			err := skipHandler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())

			Expect(innerHandler.deleteCalled).To(BeTrue())
			Expect(logSampler.IsSampleCallCount()).To(Equal(1))
		})
	})
})
