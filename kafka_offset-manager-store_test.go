// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	stderrors "errors"

	libboltkv "github.com/bborbe/boltkv"
	libkv "github.com/bborbe/kv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
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
					libkafka.NewOffsetStore(db),
					libkafka.OffsetOldest,
					libkafka.OffsetOldest,
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
					libkafka.NewOffsetStore(db),
					libkafka.OffsetOldest,
					libkafka.OffsetOldest,
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
					libkafka.NewOffsetStore(db),
					libkafka.OffsetNewest,
					libkafka.OffsetNewest,
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
					libkafka.NewOffsetStore(db),
					1337,
					1337,
				)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns correct offset", func() {
				Expect(offset).To(Equal(libkafka.Offset(1337)))
			})
		})
		Context("BucketNotFoundError => initial number", func() {
			BeforeEach(func() {
				offsetStore := &mocks.KafkaOffsetStore{}
				offsetStore.GetReturns(0, libkv.BucketNotFoundError)
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					offsetStore,
					1337,
					1337,
				)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns correct offset", func() {
				Expect(offset).To(Equal(libkafka.Offset(1337)))
			})
		})
		Context("KeyNotFoundError => initial number", func() {
			BeforeEach(func() {
				offsetStore := &mocks.KafkaOffsetStore{}
				offsetStore.GetReturns(0, libkv.KeyNotFoundError)
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					offsetStore,
					1337,
					1337,
				)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns correct offset", func() {
				Expect(offset).To(Equal(libkafka.Offset(1337)))
			})
		})
		Context("any error => initial number", func() {
			BeforeEach(func() {
				offsetStore := &mocks.KafkaOffsetStore{}
				offsetStore.GetReturns(0, stderrors.New("banana"))
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					offsetStore,
					1337,
					1337,
				)
			})
			It("returns error", func() {
				Expect(err).NotTo(BeNil())
			})
			It("returns no offset", func() {
				Expect(offset).To(Equal(libkafka.Offset(0)))
			})
		})
	})
	Context("MarkOffset", func() {
		BeforeEach(func() {
			storeOffsetManager = libkafka.NewStoreOffsetManager(
				libkafka.NewOffsetStore(db),
				libkafka.OffsetOldest,
				libkafka.OffsetOldest,
			)
		})
		JustBeforeEach(func() {
			err = storeOffsetManager.MarkOffset(ctx, topic, partition, 42)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("stores offset in database", func() {
			offset, err := storeOffsetManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(42)))
		})
	})
	Context("ResetOffset", func() {
		BeforeEach(func() {
			storeOffsetManager = libkafka.NewStoreOffsetManager(
				libkafka.NewOffsetStore(db),
				libkafka.OffsetOldest,
				libkafka.OffsetOldest,
			)
		})
		Context("forward movement", func() {
			JustBeforeEach(func() {
				err = storeOffsetManager.MarkOffset(ctx, topic, partition, 100)
				Expect(err).To(BeNil())
				err = storeOffsetManager.ResetOffset(ctx, topic, partition, 200)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("updates offset forward", func() {
				offset, err := storeOffsetManager.NextOffset(ctx, topic, partition)
				Expect(err).To(BeNil())
				Expect(offset).To(Equal(libkafka.Offset(200)))
			})
		})
		Context("backward movement", func() {
			JustBeforeEach(func() {
				err = storeOffsetManager.MarkOffset(ctx, topic, partition, 100)
				Expect(err).To(BeNil())
				err = storeOffsetManager.ResetOffset(ctx, topic, partition, 50)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("updates offset backward", func() {
				offset, err := storeOffsetManager.NextOffset(ctx, topic, partition)
				Expect(err).To(BeNil())
				Expect(offset).To(Equal(libkafka.Offset(50)))
			})
		})
	})
	Context("InitialOffset", func() {
		Context("with OffsetOldest", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.NewOffsetStore(db),
					libkafka.OffsetOldest,
					libkafka.OffsetNewest,
				)
			})
			It("returns OffsetOldest", func() {
				Expect(storeOffsetManager.InitialOffset()).To(Equal(libkafka.OffsetOldest))
			})
		})
		Context("with OffsetNewest", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.NewOffsetStore(db),
					libkafka.OffsetNewest,
					libkafka.OffsetOldest,
				)
			})
			It("returns OffsetNewest", func() {
				Expect(storeOffsetManager.InitialOffset()).To(Equal(libkafka.OffsetNewest))
			})
		})
		Context("with custom offset", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.NewOffsetStore(db),
					1337,
					9999,
				)
			})
			It("returns custom offset", func() {
				Expect(storeOffsetManager.InitialOffset()).To(Equal(libkafka.Offset(1337)))
			})
		})
	})
	Context("FallbackOffset", func() {
		Context("with OffsetOldest", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.NewOffsetStore(db),
					libkafka.OffsetNewest,
					libkafka.OffsetOldest,
				)
			})
			It("returns OffsetOldest", func() {
				Expect(storeOffsetManager.FallbackOffset()).To(Equal(libkafka.OffsetOldest))
			})
		})
		Context("with OffsetNewest", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.NewOffsetStore(db),
					libkafka.OffsetOldest,
					libkafka.OffsetNewest,
				)
			})
			It("returns OffsetNewest", func() {
				Expect(storeOffsetManager.FallbackOffset()).To(Equal(libkafka.OffsetNewest))
			})
		})
		Context("with custom offset", func() {
			BeforeEach(func() {
				storeOffsetManager = libkafka.NewStoreOffsetManager(
					libkafka.NewOffsetStore(db),
					1337,
					9999,
				)
			})
			It("returns custom offset", func() {
				Expect(storeOffsetManager.FallbackOffset()).To(Equal(libkafka.Offset(9999)))
			})
		})
	})
})
