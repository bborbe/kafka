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

var _ = Describe("UpdaterHandlerFunc", func() {
	type contextKey string

	var (
		ctx          context.Context
		updateCalled bool
		deleteCalled bool
		updateFunc   func(ctx context.Context, key string, object string) error
		deleteFunc   func(ctx context.Context, key string) error
		handler      libkafka.UpdaterHandler[string, string]
	)

	BeforeEach(func() {
		ctx = context.Background()
		updateCalled = false
		deleteCalled = false
		updateFunc = func(ctx context.Context, key string, object string) error {
			updateCalled = true
			return nil
		}
		deleteFunc = func(ctx context.Context, key string) error {
			deleteCalled = true
			return nil
		}
		handler = libkafka.UpdaterHandlerFunc(updateFunc, deleteFunc)
	})

	It("should implement UpdaterHandler interface", func() {
		Expect(handler).NotTo(BeNil())
	})

	Describe("Update", func() {
		It("should call the update function", func() {
			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())
			Expect(updateCalled).To(BeTrue())
			Expect(deleteCalled).To(BeFalse())
		})

		It("should return error from update function", func() {
			expectedErr := errors.New("update error")
			updateFunc = func(ctx context.Context, key string, object string) error {
				return expectedErr
			}
			handler = libkafka.UpdaterHandlerFunc(updateFunc, deleteFunc)

			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("update failed"))
			Expect(err.Error()).To(ContainSubstring("update error"))
		})

		It("should handle nil update function", func() {
			handler = libkafka.UpdaterHandlerFunc[string, string](nil, deleteFunc)

			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())
		})

		It("should pass correct parameters to update function", func() {
			var receivedCtx context.Context
			var receivedKey string
			var receivedObject string

			updateFunc = func(ctx context.Context, key string, object string) error {
				receivedCtx = ctx
				receivedKey = key
				receivedObject = object
				return nil
			}
			handler = libkafka.UpdaterHandlerFunc(updateFunc, deleteFunc)

			ctx = context.WithValue(context.Background(), contextKey("test"), "value")
			err := handler.Update(ctx, "test-key", "test-object")
			Expect(err).To(BeNil())
			Expect(receivedCtx).To(Equal(ctx))
			Expect(receivedKey).To(Equal("test-key"))
			Expect(receivedObject).To(Equal("test-object"))
		})
	})

	Describe("Delete", func() {
		It("should call the delete function", func() {
			err := handler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())
			Expect(deleteCalled).To(BeTrue())
			Expect(updateCalled).To(BeFalse())
		})

		It("should return error from delete function", func() {
			expectedErr := errors.New("delete error")
			deleteFunc = func(ctx context.Context, key string) error {
				return expectedErr
			}
			handler = libkafka.UpdaterHandlerFunc(updateFunc, deleteFunc)

			err := handler.Delete(ctx, "test-key")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("delete failed"))
			Expect(err.Error()).To(ContainSubstring("delete error"))
		})

		It("should handle nil delete function", func() {
			handler = libkafka.UpdaterHandlerFunc(updateFunc, nil)

			err := handler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())
		})

		It("should pass correct parameters to delete function", func() {
			var receivedCtx context.Context
			var receivedKey string

			deleteFunc = func(ctx context.Context, key string) error {
				receivedCtx = ctx
				receivedKey = key
				return nil
			}
			handler = libkafka.UpdaterHandlerFunc(updateFunc, deleteFunc)

			ctx = context.WithValue(context.Background(), contextKey("test"), "value")
			err := handler.Delete(ctx, "test-key")
			Expect(err).To(BeNil())
			Expect(receivedCtx).To(Equal(ctx))
			Expect(receivedKey).To(Equal("test-key"))
		})
	})

	Describe("With byte slice keys", func() {
		It("should work with []byte keys", func() {
			updateByteFunc := func(ctx context.Context, key []byte, object string) error {
				return nil
			}
			deleteByteFunc := func(ctx context.Context, key []byte) error {
				return nil
			}

			byteHandler := libkafka.UpdaterHandlerFunc(updateByteFunc, deleteByteFunc)

			err := byteHandler.Update(ctx, []byte("test-key"), "test-object")
			Expect(err).To(BeNil())

			err = byteHandler.Delete(ctx, []byte("test-key"))
			Expect(err).To(BeNil())
		})
	})
})
