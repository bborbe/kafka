// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("SimpleOffsetManager", func() {
	var ctx context.Context
	var err error
	var simpleOffsetManager libkafka.OffsetManager
	var topic libkafka.Topic
	var partition libkafka.Partition
	BeforeEach(func() {
		ctx = context.Background()
		topic = "my-topic"
		partition = 0
	})
	Context("NextOffset", func() {
		var offset libkafka.Offset
		JustBeforeEach(func() {
			offset, err = simpleOffsetManager.NextOffset(ctx, topic, partition)
		})
		Context("initial OffsetOldest", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
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
		Context("initial OffsetNewest", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
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
		Context("initial custom offset", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
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
		Context("after MarkOffset", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetOldest,
					libkafka.OffsetOldest,
				)
				err = simpleOffsetManager.MarkOffset(ctx, topic, partition, 42)
				Expect(err).To(BeNil())
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns marked offset", func() {
				Expect(offset).To(Equal(libkafka.Offset(42)))
			})
		})
		Context("different partition", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetOldest,
					libkafka.OffsetOldest,
				)
				err = simpleOffsetManager.MarkOffset(ctx, "other-topic", 1, 99)
				Expect(err).To(BeNil())
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("returns initial offset", func() {
				Expect(offset).To(Equal(libkafka.OffsetOldest))
			})
		})
	})
	Context("MarkOffset", func() {
		BeforeEach(func() {
			simpleOffsetManager = libkafka.NewSimpleOffsetManager(
				libkafka.OffsetOldest,
				libkafka.OffsetOldest,
			)
		})
		JustBeforeEach(func() {
			err = simpleOffsetManager.MarkOffset(ctx, topic, partition, 42)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("stores offset in memory", func() {
			offset, err := simpleOffsetManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(42)))
		})
		Context("when closed", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetOldest,
					libkafka.OffsetOldest,
				)
				err = simpleOffsetManager.Close()
				Expect(err).To(BeNil())
			})
			It("returns error", func() {
				Expect(err).NotTo(BeNil())
			})
		})
	})
	Context("ResetOffset", func() {
		BeforeEach(func() {
			simpleOffsetManager = libkafka.NewSimpleOffsetManager(
				libkafka.OffsetOldest,
				libkafka.OffsetOldest,
			)
		})
		Context("forward movement", func() {
			JustBeforeEach(func() {
				err = simpleOffsetManager.MarkOffset(ctx, topic, partition, 100)
				Expect(err).To(BeNil())
				err = simpleOffsetManager.ResetOffset(ctx, topic, partition, 200)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("updates offset forward", func() {
				offset, err := simpleOffsetManager.NextOffset(ctx, topic, partition)
				Expect(err).To(BeNil())
				Expect(offset).To(Equal(libkafka.Offset(200)))
			})
		})
		Context("backward movement", func() {
			JustBeforeEach(func() {
				err = simpleOffsetManager.MarkOffset(ctx, topic, partition, 100)
				Expect(err).To(BeNil())
				err = simpleOffsetManager.ResetOffset(ctx, topic, partition, 50)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("updates offset backward", func() {
				offset, err := simpleOffsetManager.NextOffset(ctx, topic, partition)
				Expect(err).To(BeNil())
				Expect(offset).To(Equal(libkafka.Offset(50)))
			})
		})
		Context("when closed", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetOldest,
					libkafka.OffsetOldest,
				)
				err = simpleOffsetManager.Close()
				Expect(err).To(BeNil())
			})
			JustBeforeEach(func() {
				err = simpleOffsetManager.ResetOffset(ctx, topic, partition, 42)
			})
			It("returns error", func() {
				Expect(err).NotTo(BeNil())
			})
		})
	})
	Context("InitialOffset", func() {
		Context("with OffsetOldest", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetOldest,
					libkafka.OffsetNewest,
				)
			})
			It("returns OffsetOldest", func() {
				Expect(simpleOffsetManager.InitialOffset()).To(Equal(libkafka.OffsetOldest))
			})
		})
		Context("with OffsetNewest", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetNewest,
					libkafka.OffsetOldest,
				)
			})
			It("returns OffsetNewest", func() {
				Expect(simpleOffsetManager.InitialOffset()).To(Equal(libkafka.OffsetNewest))
			})
		})
		Context("with custom offset", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					1337,
					9999,
				)
			})
			It("returns custom offset", func() {
				Expect(simpleOffsetManager.InitialOffset()).To(Equal(libkafka.Offset(1337)))
			})
		})
	})
	Context("FallbackOffset", func() {
		Context("with OffsetOldest", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetNewest,
					libkafka.OffsetOldest,
				)
			})
			It("returns OffsetOldest", func() {
				Expect(simpleOffsetManager.FallbackOffset()).To(Equal(libkafka.OffsetOldest))
			})
		})
		Context("with OffsetNewest", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					libkafka.OffsetOldest,
					libkafka.OffsetNewest,
				)
			})
			It("returns OffsetNewest", func() {
				Expect(simpleOffsetManager.FallbackOffset()).To(Equal(libkafka.OffsetNewest))
			})
		})
		Context("with custom offset", func() {
			BeforeEach(func() {
				simpleOffsetManager = libkafka.NewSimpleOffsetManager(
					1337,
					9999,
				)
			})
			It("returns custom offset", func() {
				Expect(simpleOffsetManager.FallbackOffset()).To(Equal(libkafka.Offset(9999)))
			})
		})
	})
	Context("Close", func() {
		BeforeEach(func() {
			simpleOffsetManager = libkafka.NewSimpleOffsetManager(
				libkafka.OffsetOldest,
				libkafka.OffsetOldest,
			)
		})
		JustBeforeEach(func() {
			err = simpleOffsetManager.Close()
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("prevents further MarkOffset calls", func() {
			err = simpleOffsetManager.MarkOffset(ctx, topic, partition, 42)
			Expect(err).NotTo(BeNil())
		})
		It("prevents further ResetOffset calls", func() {
			err = simpleOffsetManager.ResetOffset(ctx, topic, partition, 42)
			Expect(err).NotTo(BeNil())
		})
	})
})
