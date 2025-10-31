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

var _ = Describe("SaramaClientProviderType", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("ParseSaramaClientProviderType", func() {
		It("parses 'reused' correctly", func() {
			result, err := kafka.ParseSaramaClientProviderType(ctx, "reused")
			Expect(err).NotTo(HaveOccurred())
			Expect(*result).To(Equal(kafka.SaramaClientProviderTypeReused))
		})

		It("parses 'new' correctly", func() {
			result, err := kafka.ParseSaramaClientProviderType(ctx, "new")
			Expect(err).NotTo(HaveOccurred())
			Expect(*result).To(Equal(kafka.SaramaClientProviderTypeNew))
		})

		It("parses SaramaClientProviderType value", func() {
			result, err := kafka.ParseSaramaClientProviderType(
				ctx,
				kafka.SaramaClientProviderTypeReused,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result).To(Equal(kafka.SaramaClientProviderTypeReused))
		})

		It("parses *SaramaClientProviderType value", func() {
			input := kafka.SaramaClientProviderTypeNew
			result, err := kafka.ParseSaramaClientProviderType(ctx, &input)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result).To(Equal(kafka.SaramaClientProviderTypeNew))
		})

		It("returns unknown value for invalid input", func() {
			result, err := kafka.ParseSaramaClientProviderType(ctx, "invalid")
			Expect(err).NotTo(HaveOccurred())
			Expect(*result).To(Equal(kafka.SaramaClientProviderType("invalid")))
		})
	})

	Describe("Validate", func() {
		It("validates 'reused' successfully", func() {
			err := kafka.SaramaClientProviderTypeReused.Validate(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("validates 'new' successfully", func() {
			err := kafka.SaramaClientProviderTypeNew.Validate(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error for invalid type", func() {
			invalidType := kafka.SaramaClientProviderType("invalid")
			err := invalidType.Validate(ctx)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("String", func() {
		It("returns correct string for 'reused'", func() {
			Expect(kafka.SaramaClientProviderTypeReused.String()).To(Equal("reused"))
		})

		It("returns correct string for 'new'", func() {
			Expect(kafka.SaramaClientProviderTypeNew.String()).To(Equal("new"))
		})
	})

	Describe("Ptr", func() {
		It("returns pointer to SaramaClientProviderType", func() {
			providerType := kafka.SaramaClientProviderTypeReused
			ptr := providerType.Ptr()
			Expect(ptr).NotTo(BeNil())
			Expect(*ptr).To(Equal(kafka.SaramaClientProviderTypeReused))
		})
	})

	Describe("SaramaClientProviderTypes.Contains", func() {
		It("returns true for valid type", func() {
			Expect(
				kafka.AvailableSaramaClientProviderTypes.Contains(
					kafka.SaramaClientProviderTypeReused,
				),
			).To(BeTrue())
			Expect(
				kafka.AvailableSaramaClientProviderTypes.Contains(
					kafka.SaramaClientProviderTypeNew,
				),
			).To(BeTrue())
		})

		It("returns false for invalid type", func() {
			Expect(
				kafka.AvailableSaramaClientProviderTypes.Contains(
					kafka.SaramaClientProviderType("invalid"),
				),
			).To(BeFalse())
		})
	})

	Describe("NewSaramaClientProviderByType", func() {
		var brokers kafka.Brokers

		BeforeEach(func() {
			brokers = kafka.ParseBrokers([]string{"localhost:9092"})
		})

		It("creates reused provider", func() {
			provider, err := kafka.NewSaramaClientProviderByType(
				ctx,
				kafka.SaramaClientProviderTypeReused,
				brokers,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(provider).NotTo(BeNil())
			defer provider.Close()
		})

		It("creates new provider", func() {
			provider, err := kafka.NewSaramaClientProviderByType(
				ctx,
				kafka.SaramaClientProviderTypeNew,
				brokers,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(provider).NotTo(BeNil())
			defer provider.Close()
		})

		It("returns error for invalid type", func() {
			provider, err := kafka.NewSaramaClientProviderByType(
				ctx,
				kafka.SaramaClientProviderType("invalid"),
				brokers,
			)
			Expect(err).To(HaveOccurred())
			Expect(provider).To(BeNil())
		})
	})
})
