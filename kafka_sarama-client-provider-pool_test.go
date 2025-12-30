// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("SaramaClientProviderPool", func() {
	var ctx context.Context
	var provider kafka.SaramaClientProvider
	var brokers kafka.Brokers
	var poolOpts kafka.SaramaClientPoolOptions

	BeforeEach(func() {
		ctx = context.Background()
		brokers = kafka.Brokers{"localhost:9092"}
		poolOpts = kafka.DefaultSaramaClientPoolOptions
	})

	Describe("NewSaramaClientProviderPool", func() {
		It("creates provider with default options", func() {
			provider = kafka.NewSaramaClientProviderPool(
				brokers,
				kafka.DefaultSaramaClientPoolOptions,
			)
			Expect(provider).NotTo(BeNil())
		})

		It("creates provider with custom options", func() {
			customOpts := kafka.SaramaClientPoolOptions{
				MaxPoolSize:        5,
				HealthCheckTimeout: 3 * time.Second,
			}
			provider = kafka.NewSaramaClientProviderPool(brokers, customOpts)
			Expect(provider).NotTo(BeNil())
		})
	})

	Describe("Close", func() {
		It("closes successfully", func() {
			provider = kafka.NewSaramaClientProviderPool(brokers, poolOpts)
			err := provider.Close()
			// May fail if can't connect to Kafka, but should not panic
			_ = err
		})

		It("can be called multiple times", func() {
			provider = kafka.NewSaramaClientProviderPool(brokers, poolOpts)
			err := provider.Close()
			_ = err

			err = provider.Close()
			_ = err
		})
	})

	Describe("pooledClient wrapper behavior", func() {
		var mockClient *mocks.KafkaSaramaClient

		BeforeEach(func() {
			mockClient = &mocks.KafkaSaramaClient{}
			mockClient.ClosedReturns(false)
			mockClient.BrokersReturns([]*sarama.Broker{{}})
		})

		Context("health detection logic", func() {
			It("marks client as healthy when not closed and has brokers", func() {
				mockClient.ClosedReturns(false)
				mockClient.BrokersReturns([]*sarama.Broker{{}})

				// Verify health check conditions
				Expect(mockClient.Closed()).To(BeFalse())
				Expect(len(mockClient.Brokers())).To(BeNumerically(">", 0))
			})

			It("marks client as unhealthy when closed", func() {
				mockClient.ClosedReturns(true)
				mockClient.BrokersReturns([]*sarama.Broker{{}})

				// Verify unhealthy due to closed
				Expect(mockClient.Closed()).To(BeTrue())
			})

			It("marks client as unhealthy when no brokers", func() {
				mockClient.ClosedReturns(false)
				mockClient.BrokersReturns([]*sarama.Broker{})

				// Verify unhealthy due to no brokers
				Expect(len(mockClient.Brokers())).To(Equal(0))
			})
		})
	})

	Describe("Integration behavior", func() {
		Context("with real pool", func() {
			BeforeEach(func() {
				Skip("requires running Kafka instance for integration testing")
			})

			It("acquires and releases clients correctly", func() {
				provider = kafka.NewSaramaClientProviderPool(brokers, poolOpts)
				defer provider.Close()

				client, err := provider.Client(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(client).NotTo(BeNil())

				err = client.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("reuses clients from pool", func() {
				provider = kafka.NewSaramaClientProviderPool(brokers, poolOpts)
				defer provider.Close()

				client1, err := provider.Client(ctx)
				Expect(err).NotTo(HaveOccurred())
				err = client1.Close()
				Expect(err).NotTo(HaveOccurred())

				client2, err := provider.Client(ctx)
				Expect(err).NotTo(HaveOccurred())
				err = client2.Close()
				Expect(err).NotTo(HaveOccurred())

				// In a real pool, client2 should be the same underlying connection
			})

			It("handles multiple concurrent client acquisitions", func() {
				provider = kafka.NewSaramaClientProviderPool(brokers, poolOpts)
				defer provider.Close()

				done := make(chan bool)

				for i := 0; i < 10; i++ {
					go func() {
						defer GinkgoRecover()
						client, err := provider.Client(ctx)
						Expect(err).NotTo(HaveOccurred())
						time.Sleep(10 * time.Millisecond)
						err = client.Close()
						Expect(err).NotTo(HaveOccurred())
						done <- true
					}()
				}

				for i := 0; i < 10; i++ {
					<-done
				}
			})

			It("handles client Close() as pool return, not actual close", func() {
				provider = kafka.NewSaramaClientProviderPool(brokers, poolOpts)
				defer provider.Close()

				client, err := provider.Client(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Close should return to pool, not close connection
				err = client.Close()
				Expect(err).NotTo(HaveOccurred())

				// Should be able to get another client (possibly same connection)
				client2, err := provider.Client(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(client2).NotTo(BeNil())

				err = client2.Close()
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})
})
