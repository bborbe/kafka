// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
)

var _ = Describe("BatchSize", func() {
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
	})
	Context("Validate", func() {
		It("valid", func() {
			Expect(kafka.BatchSize(100).Validate(ctx)).To(BeNil())
		})
		It("invalid", func() {
			Expect(kafka.BatchSize(0).Validate(ctx)).NotTo(BeNil())
		})
		It("invalid", func() {
			Expect(kafka.BatchSize(-1).Validate(ctx)).NotTo(BeNil())
		})
	})
	It("String", func() {
		Expect(kafka.BatchSize(100).String()).To(Equal("100"))
	})
	It("Int", func() {
		Expect(kafka.BatchSize(100).Int()).To(Equal(100))
	})
})
