// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("SaramaOffsetManager", func() {
	var saramaOffsetManager kafka.OffsetManager
	var saramaClient *mocks.KafkaSaramaClient
	var group kafka.Group

	BeforeEach(func() {
		saramaClient = &mocks.KafkaSaramaClient{}
		group = "test-group"
	})

	Context("InitialOffset", func() {
		Context("with OffsetOldest", func() {
			BeforeEach(func() {
				saramaOffsetManager = kafka.NewSaramaOffsetManager(
					saramaClient,
					group,
					kafka.OffsetOldest,
					kafka.OffsetNewest,
				)
			})
			It("returns OffsetOldest", func() {
				Expect(saramaOffsetManager.InitialOffset()).To(Equal(kafka.OffsetOldest))
			})
		})
		Context("with OffsetNewest", func() {
			BeforeEach(func() {
				saramaOffsetManager = kafka.NewSaramaOffsetManager(
					saramaClient,
					group,
					kafka.OffsetNewest,
					kafka.OffsetOldest,
				)
			})
			It("returns OffsetNewest", func() {
				Expect(saramaOffsetManager.InitialOffset()).To(Equal(kafka.OffsetNewest))
			})
		})
		Context("with custom offset", func() {
			BeforeEach(func() {
				saramaOffsetManager = kafka.NewSaramaOffsetManager(
					saramaClient,
					group,
					1337,
					9999,
				)
			})
			It("returns custom offset", func() {
				Expect(saramaOffsetManager.InitialOffset()).To(Equal(kafka.Offset(1337)))
			})
		})
	})

	Context("FallbackOffset", func() {
		Context("with OffsetOldest", func() {
			BeforeEach(func() {
				saramaOffsetManager = kafka.NewSaramaOffsetManager(
					saramaClient,
					group,
					kafka.OffsetNewest,
					kafka.OffsetOldest,
				)
			})
			It("returns OffsetOldest", func() {
				Expect(saramaOffsetManager.FallbackOffset()).To(Equal(kafka.OffsetOldest))
			})
		})
		Context("with OffsetNewest", func() {
			BeforeEach(func() {
				saramaOffsetManager = kafka.NewSaramaOffsetManager(
					saramaClient,
					group,
					kafka.OffsetOldest,
					kafka.OffsetNewest,
				)
			})
			It("returns OffsetNewest", func() {
				Expect(saramaOffsetManager.FallbackOffset()).To(Equal(kafka.OffsetNewest))
			})
		})
		Context("with custom offset", func() {
			BeforeEach(func() {
				saramaOffsetManager = kafka.NewSaramaOffsetManager(
					saramaClient,
					group,
					1337,
					9999,
				)
			})
			It("returns custom offset", func() {
				Expect(saramaOffsetManager.FallbackOffset()).To(Equal(kafka.Offset(9999)))
			})
		})
	})

	// Note: NextOffset, MarkOffset, and ResetOffset require a running Kafka broker
	// and Sarama OffsetManager/PartitionOffsetManager mocks for full testing.
	// These methods are tested through integration tests in sm-octopus.
})
