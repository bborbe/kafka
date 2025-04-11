// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("Topics", func() {
	var topics libkafka.Topics
	BeforeEach(func() {
		topics = libkafka.Topics{"a", "c", "b"}
	})
	Context("Sort", func() {
		BeforeEach(func() {
			sort.Sort(topics)
		})
		It("sorts correct", func() {
			Expect(topics).To(Equal(libkafka.Topics{"a", "b", "c"}))
		})
	})
})
