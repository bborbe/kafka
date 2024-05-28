// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"errors"

	libbadgerkv "github.com/bborbe/badgerkv"
	libkv "github.com/bborbe/kv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
)

var _ = Describe("OffsetStore", func() {
	var ctx context.Context
	var err error
	var offsetStore kafka.OffsetStore
	var partition kafka.Partition
	var topic kafka.Topic
	var db libkv.DB
	var offset kafka.Offset
	BeforeEach(func() {
		ctx = context.Background()

		partition = 0
		topic = "my-topic"

		db, err = libbadgerkv.OpenMemory(ctx)
		Expect(err).To(BeNil())

		offsetStore = kafka.NewOffsetStore(db)
	})
	JustBeforeEach(func() {
		offset, err = offsetStore.Get(ctx, topic, partition)
	})
	Context("success not found", func() {
		BeforeEach(func() {
			Expect(offsetStore.Set(ctx, topic, partition, 1337)).To(BeNil())
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns offset", func() {
			Expect(offset).To(Equal(kafka.Offset(1337)))
		})
	})
	Context("key not found", func() {
		BeforeEach(func() {
			Expect(offsetStore.Set(ctx, "other-topic", partition, 1337)).To(BeNil())
		})
		It("returns error", func() {
			Expect(err).NotTo(BeNil())
			Expect(errors.Is(err, libkv.KeyNotFoundError)).To(BeTrue())
		})
	})
	Context("bucket not found", func() {
		It("returns error", func() {
			Expect(err).NotTo(BeNil())
			Expect(errors.Is(err, libkv.BucketNotFoundError)).To(BeTrue())
		})
	})
})
