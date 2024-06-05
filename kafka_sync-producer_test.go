// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	stderrors "errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("SyncProducer", func() {
	var err error
	var saramaSyncProducer *mocks.KafkaSaramaSyncProducer
	var syncProducer kafka.SyncProducer
	BeforeEach(func() {
		saramaSyncProducer = &mocks.KafkaSaramaSyncProducer{}
		syncProducer = kafka.NewSyncProducerFromSaramaSyncProducer(saramaSyncProducer)
	})
	Context("Close", func() {
		JustBeforeEach(func() {
			err = syncProducer.Close()
		})
		Context("success", func() {
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("calls close", func() {
				Expect(saramaSyncProducer.CloseCallCount()).To(Equal(1))
			})
		})
		Context("failed", func() {
			BeforeEach(func() {
				saramaSyncProducer.CloseReturns(stderrors.New("banana"))
			})
			It("returns error", func() {
				Expect(err).NotTo(BeNil())
			})
			It("calls close", func() {
				Expect(saramaSyncProducer.CloseCallCount()).To(Equal(1))
			})
		})
	})
})
