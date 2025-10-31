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

var _ = Describe("ConsumerFunc", func() {
	type contextKey string

	It("should implement Consumer interface", func() {
		var consumer libkafka.Consumer = libkafka.ConsumerFunc(func(ctx context.Context) error {
			return nil
		})
		Expect(consumer).NotTo(BeNil())
	})

	It("should call the function when Consume is called", func() {
		called := false
		consumer := libkafka.ConsumerFunc(func(ctx context.Context) error {
			called = true
			return nil
		})

		err := consumer.Consume(context.Background())
		Expect(err).To(BeNil())
		Expect(called).To(BeTrue())
	})

	It("should pass context to the function", func() {
		var receivedCtx context.Context
		consumer := libkafka.ConsumerFunc(func(ctx context.Context) error {
			receivedCtx = ctx
			return nil
		})

		ctx := context.WithValue(context.Background(), contextKey("test"), "value")
		err := consumer.Consume(ctx)
		Expect(err).To(BeNil())
		Expect(receivedCtx).To(Equal(ctx))
	})

	It("should return error from function", func() {
		expectedErr := errors.New("test error")
		consumer := libkafka.ConsumerFunc(func(ctx context.Context) error {
			return expectedErr
		})

		err := consumer.Consume(context.Background())
		Expect(err).To(Equal(expectedErr))
	})
})
