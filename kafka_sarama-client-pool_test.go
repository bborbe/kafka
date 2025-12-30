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

var _ = Describe("SaramaClientPool", func() {
	var ctx context.Context
	var pool kafka.SaramaClientPool
	var factory func(context.Context) (kafka.SaramaClient, error)
	var createdClients []*mocks.KafkaSaramaClient

	BeforeEach(func() {
		ctx = context.Background()
		createdClients = make([]*mocks.KafkaSaramaClient, 0)

		factory = func(ctx context.Context) (kafka.SaramaClient, error) {
			client := &mocks.KafkaSaramaClient{}
			// Mock healthy client by default
			client.ClosedReturns(false)
			client.BrokersReturns([]*sarama.Broker{{}}) // Non-empty brokers = healthy
			client.CloseReturns(nil)
			createdClients = append(createdClients, client)
			return client, nil
		}
	})

	Describe("NewSaramaClientPool", func() {
		It("creates pool with default options", func() {
			pool = kafka.NewSaramaClientPool(factory, kafka.SaramaClientPoolOptions{})
			Expect(pool).NotTo(BeNil())
		})

		It("creates pool with custom options", func() {
			opts := kafka.SaramaClientPoolOptions{
				MaxPoolSize:        5,
				HealthCheckTimeout: 3 * time.Second,
			}
			pool = kafka.NewSaramaClientPool(factory, opts)
			Expect(pool).NotTo(BeNil())
		})
	})

	Describe("Acquire", func() {
		BeforeEach(func() {
			pool = kafka.NewSaramaClientPool(factory, kafka.SaramaClientPoolOptions{
				MaxPoolSize:        3,
				HealthCheckTimeout: 1 * time.Second,
			})
		})

		AfterEach(func() {
			if pool != nil {
				_ = pool.Close()
			}
		})

		It("creates new client when pool is empty", func() {
			client, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client).NotTo(BeNil())
			Expect(len(createdClients)).To(Equal(1))
		})

		It("reuses healthy client from pool", func() {
			// Acquire and release a client
			client1, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			pool.Release(client1, true)

			// Acquire again - should reuse
			client2, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client2).To(BeIdenticalTo(client1))
			Expect(len(createdClients)).To(Equal(1)) // No new client created
		})

		It("creates new client when pool client is unhealthy", func() {
			// Acquire and release an unhealthy client
			client1, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Mark client as closed (unhealthy)
			mockClient, ok := client1.(*mocks.KafkaSaramaClient)
			Expect(ok).To(BeTrue())
			mockClient.ClosedReturns(true)
			pool.Release(client1, true)

			// Acquire again - should detect unhealthy and create new
			client2, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client2).NotTo(BeIdenticalTo(client1))
			Expect(len(createdClients)).To(Equal(2))
		})

		It("creates new client when pool client has no brokers", func() {
			// Acquire a client
			client1, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Mark client as having no brokers (unhealthy)
			mockClient, ok := client1.(*mocks.KafkaSaramaClient)
			Expect(ok).To(BeTrue())
			mockClient.BrokersReturns([]*sarama.Broker{}) // Empty brokers
			pool.Release(client1, true)

			// Acquire again - should detect unhealthy and create new
			client2, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client2).NotTo(BeIdenticalTo(client1))
			Expect(len(createdClients)).To(Equal(2))
		})

		It("returns error after pool is closed", func() {
			err := pool.Close()
			Expect(err).NotTo(HaveOccurred())

			_, err = pool.Acquire(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("pool is closed"))
		})
	})

	Describe("Release", func() {
		BeforeEach(func() {
			pool = kafka.NewSaramaClientPool(factory, kafka.SaramaClientPoolOptions{
				MaxPoolSize:        2,
				HealthCheckTimeout: 1 * time.Second,
			})
		})

		AfterEach(func() {
			if pool != nil {
				_ = pool.Close()
			}
		})

		It("returns healthy client to pool", func() {
			client, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			pool.Release(client, true)

			// Verify client is reused
			client2, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client2).To(BeIdenticalTo(client))
		})

		It("closes unhealthy client instead of returning to pool", func() {
			client, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			mockClient, ok := client.(*mocks.KafkaSaramaClient)
			Expect(ok).To(BeTrue())
			pool.Release(client, false) // Mark as unhealthy

			// Verify client was closed
			Expect(mockClient.CloseCallCount()).To(Equal(1))

			// Verify new client is created on next acquire
			client2, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(client2).NotTo(BeIdenticalTo(client))
		})

		It("closes client when pool is full", func() {
			// Acquire multiple clients without releasing (to create more than pool size)
			client1, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			client2, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			client3, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Release first two - they should go back to pool
			pool.Release(client1, true)
			pool.Release(client2, true)

			// Release third - pool is full (max size = 2), so it should be closed
			mockClient3, ok := client3.(*mocks.KafkaSaramaClient)
			Expect(ok).To(BeTrue())
			pool.Release(client3, true)

			// Third client should be closed (pool full)
			Expect(mockClient3.CloseCallCount()).To(Equal(1))
		})

		It("handles nil client gracefully", func() {
			pool.Release(nil, true)
			// Should not panic
		})

		It("closes client if pool is closed", func() {
			client, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			mockClient, ok := client.(*mocks.KafkaSaramaClient)
			Expect(ok).To(BeTrue())
			err = pool.Close()
			Expect(err).NotTo(HaveOccurred())

			pool.Release(client, true)

			// Client should be closed when released after pool close
			Expect(mockClient.CloseCallCount()).To(BeNumerically(">", 0))
		})
	})

	Describe("Close", func() {
		BeforeEach(func() {
			pool = kafka.NewSaramaClientPool(factory, kafka.SaramaClientPoolOptions{
				MaxPoolSize:        3,
				HealthCheckTimeout: 1 * time.Second,
			})
		})

		It("closes all clients in pool", func() {
			// Acquire and release multiple clients
			client1, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			pool.Release(client1, true)

			client2, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())
			pool.Release(client2, true)

			// Close pool
			err = pool.Close()
			Expect(err).NotTo(HaveOccurred())

			// Verify all clients were closed
			for _, client := range createdClients {
				Expect(client.CloseCallCount()).To(BeNumerically(">=", 1))
			}
		})

		It("can be called multiple times", func() {
			err := pool.Close()
			Expect(err).NotTo(HaveOccurred())

			err = pool.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("can be called without acquiring clients", func() {
			err := pool.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("closes clients not in pool", func() {
			// Acquire but don't release (simulate in-use client)
			client, err := pool.Acquire(ctx)
			Expect(err).NotTo(HaveOccurred())

			mockClient, ok := client.(*mocks.KafkaSaramaClient)
			Expect(ok).To(BeTrue())

			// Close pool
			err = pool.Close()
			Expect(err).NotTo(HaveOccurred())

			// Verify client was closed
			Expect(mockClient.CloseCallCount()).To(Equal(1))
		})
	})

	Describe("Pool behavior", func() {
		BeforeEach(func() {
			pool = kafka.NewSaramaClientPool(factory, kafka.SaramaClientPoolOptions{
				MaxPoolSize:        3,
				HealthCheckTimeout: 1 * time.Second,
			})
		})

		AfterEach(func() {
			if pool != nil {
				_ = pool.Close()
			}
		})

		It("maintains pool size within max limit", func() {
			// Acquire multiple clients
			clients := make([]kafka.SaramaClient, 5)
			for i := 0; i < 5; i++ {
				var err error
				clients[i], err = pool.Acquire(ctx)
				Expect(err).NotTo(HaveOccurred())
			}

			// Release all clients
			for _, client := range clients {
				pool.Release(client, true)
			}

			// Verify only MaxPoolSize clients remain
			// (others should have been closed when pool was full)
			closedCount := 0
			for _, client := range createdClients {
				if client.CloseCallCount() > 0 {
					closedCount++
				}
			}
			Expect(closedCount).To(BeNumerically(">=", 2)) // At least 2 closed (5 - 3 max)
		})

		It("handles concurrent acquire and release", func() {
			done := make(chan bool)

			// Concurrent acquires and releases
			for i := 0; i < 10; i++ {
				go func() {
					defer GinkgoRecover()
					client, err := pool.Acquire(ctx)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(10 * time.Millisecond)
					pool.Release(client, true)
					done <- true
				}()
			}

			// Wait for all goroutines
			for i := 0; i < 10; i++ {
				<-done
			}

			// Should not panic or deadlock
		})
	})
})
