// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	libboltkv "github.com/bborbe/boltkv"
	libkafka "github.com/bborbe/kafka"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StoreOffsetManager", func() {
	var ctx context.Context
	var err error
	var storeOffsetManager libkafka.OffsetManager
	var topic libkafka.Topic
	var partition libkafka.Partition
	var db libboltkv.DB
	BeforeEach(func() {
		ctx = context.Background()

		db, err = libboltkv.OpenTemp(ctx)
		Expect(err).To(BeNil())

		topic = "my-topic"
		partition = 0
	})
	AfterEach(func() {
		_ = db.Close()
		_ = db.Remove()
	})
	Context("NextOffset", func() {
		var offset libkafka.Offset
		BeforeEach(func() {

		})
		JustBeforeEach(func() {
			offset, err = storeOffsetManager.NextOffset(ctx, topic, partition)
		})
		Context("initial OffsetOldest", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.OffsetOldest,
					libkafka.NewOffsetStore(db),
				)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns correct offset", func() {
				Expect(offset).To(Equal(libkafka.OffsetOldest))
			})
		})
		Context("initial OffsetOldest with mark other topic", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.OffsetOldest,
					libkafka.NewOffsetStore(db),
				)
				Expect(storeOffsetManager.MarkOffset(ctx, "test", 0, 42)).To(BeNil())
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns correct offset", func() {
				Expect(offset).To(Equal(libkafka.OffsetOldest))
			})
		})
		Context("initial OffsetNewest", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.OffsetNewest,
					libkafka.NewOffsetStore(db),
				)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns correct offset", func() {
				Expect(offset).To(Equal(libkafka.OffsetNewest))
			})
		})
		Context("initial number", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					1337,
					libkafka.NewOffsetStore(db),
				)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns correct offset", func() {
				Expect(offset).To(Equal(libkafka.Offset(1337)))
			})
		})
	})
})
