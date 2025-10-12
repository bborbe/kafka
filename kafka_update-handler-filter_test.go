// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

type mockFilter struct {
	filtered bool
	err      error
}

func (m *mockFilter) Filtered(ctx context.Context, key string, object string) (bool, error) {
	return m.filtered, m.err
}

type mockUpdateHandler struct {
	updateCalled bool
	deleteCalled bool
	updateErr    error
	deleteErr    error
	lastKey      string
	lastObject   string
}

func (m *mockUpdateHandler) Update(ctx context.Context, key string, object string) error {
	m.updateCalled = true
	m.lastKey = key
	m.lastObject = object
	return m.updateErr
}

func (m *mockUpdateHandler) Delete(ctx context.Context, key string) error {
	m.deleteCalled = true
	m.lastKey = key
	return m.deleteErr
}

var _ = Describe("UpdaterHandlerFilter", func() {
	var (
		ctx           context.Context
		filter        *mockFilter
		updateHandler *mockUpdateHandler
		handler       libkafka.UpdaterHandler[string, string]
	)

	BeforeEach(func() {
		ctx = context.Background()
		filter = &mockFilter{}
		updateHandler = &mockUpdateHandler{}
		handler = libkafka.NewUpdaterHandlerFilter[string, string](filter, updateHandler)
	})

	Describe("FilterFunc", func() {
		It("should implement Filter interface", func() {
			filterFunc := libkafka.FilterFunc[string, string](
				func(ctx context.Context, key string, object string) (bool, error) {
					return false, nil
				},
			)
			var f libkafka.Filter[string, string] = filterFunc
			Expect(f).NotTo(BeNil())
		})

		It("should call the function", func() {
			called := false
			filterFunc := libkafka.FilterFunc[string, string](
				func(ctx context.Context, key string, object string) (bool, error) {
					called = true
					return true, nil
				},
			)

			filtered, err := filterFunc.Filtered(ctx, "key", "object")
			Expect(err).To(BeNil())
			Expect(filtered).To(BeTrue())
			Expect(called).To(BeTrue())
		})
	})

	Describe("Update", func() {
		It("should call updateHandler.Update when not filtered", func() {
			filter.filtered = false

			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())
			Expect(updateHandler.updateCalled).To(BeTrue())
			Expect(updateHandler.deleteCalled).To(BeFalse())
			Expect(updateHandler.lastKey).To(Equal("test-key"))
			Expect(updateHandler.lastObject).To(Equal("test-object"))
		})

		It("should call updateHandler.Delete when filtered", func() {
			filter.filtered = true

			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())
			Expect(updateHandler.updateCalled).To(BeFalse())
			Expect(updateHandler.deleteCalled).To(BeTrue())
			Expect(updateHandler.lastKey).To(Equal("test-key"))
		})

		It("should return error when filter fails", func() {
			filter.err = errors.New("filter error")

			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("filtered failed"))
			Expect(err.Error()).To(ContainSubstring("filter error"))
			Expect(updateHandler.updateCalled).To(BeFalse())
			Expect(updateHandler.deleteCalled).To(BeFalse())
		})

		It("should return error when update fails", func() {
			filter.filtered = false
			updateHandler.updateErr = errors.New("update error")

			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("update error"))
		})

		It("should return error when delete fails (filtered case)", func() {
			filter.filtered = true
			updateHandler.deleteErr = errors.New("delete error")

			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("delete error"))
		})
	})

	Describe("Delete", func() {
		It("should call updateHandler.Delete", func() {
			err := handler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())
			Expect(updateHandler.deleteCalled).To(BeTrue())
			Expect(updateHandler.updateCalled).To(BeFalse())
			Expect(updateHandler.lastKey).To(Equal("test-key"))
		})

		It("should return error when delete fails", func() {
			updateHandler.deleteErr = errors.New("delete error")

			err := handler.Delete(ctx, "test-key")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("delete error"))
		})
	})
})
