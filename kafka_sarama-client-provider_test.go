// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
)

var _ = Describe("SaramaClientProvider", func() {
	var ctx context.Context
	var brokers kafka.Brokers

	BeforeEach(func() {
		ctx = context.Background()
		brokers = kafka.ParseBrokers([]string{"localhost:9092"})
	})

	Describe("NewSaramaClientProviderReused", func() {
		var provider kafka.SaramaClientProvider

		BeforeEach(func() {
			provider = kafka.NewSaramaClientProviderReused(brokers)
		})

		It("returns the same client on multiple calls", func() {
			Skip("requires running Kafka broker")

			client1, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client1).NotTo(BeNil())

			client2, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client2).NotTo(BeNil())

			Expect(client1).To(BeIdenticalTo(client2))
		})

		It("can be closed multiple times", func() {
			Skip("requires running Kafka broker")

			_, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = provider.Close()
			Expect(err).NotTo(HaveOccurred())

			err = provider.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error after close", func() {
			Skip("requires running Kafka broker")

			_, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = provider.Close()
			Expect(err).NotTo(HaveOccurred())

			_, err = provider.Client(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("can be closed without calling Client", func() {
			err := provider.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("NewSaramaClientProviderNew", func() {
		var provider kafka.SaramaClientProvider

		BeforeEach(func() {
			provider = kafka.NewSaramaClientProviderNew(brokers)
		})

		It("returns different clients on multiple calls", func() {
			Skip("requires running Kafka broker")

			client1, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client1).NotTo(BeNil())

			client2, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client2).NotTo(BeNil())

			Expect(client1).NotTo(BeIdenticalTo(client2))
		})

		It("can be closed multiple times", func() {
			Skip("requires running Kafka broker")

			_, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())

			_, err = provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = provider.Close()
			Expect(err).NotTo(HaveOccurred())

			err = provider.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error after close", func() {
			Skip("requires running Kafka broker")

			_, err := provider.Client(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = provider.Close()
			Expect(err).NotTo(HaveOccurred())

			_, err = provider.Client(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("can be closed without calling Client", func() {
			err := provider.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
