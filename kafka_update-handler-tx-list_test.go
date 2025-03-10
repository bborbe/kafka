// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	"github.com/bborbe/kv/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
)

var _ = Describe("UpdaterHandlerTxList", func() {
	var ctx context.Context
	var err error
	var updateHandlerList kafka.UpdaterHandlerTx[string, struct{}]
	BeforeEach(func() {
		ctx = context.Background()
		updateHandlerList = kafka.UpdaterHandlerTxList[string, struct{}]{}
	})
	Context("Update", func() {
		JustBeforeEach(func() {
			err = updateHandlerList.Update(ctx, &mocks.Tx{}, "foo", struct{}{})
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
	})
	Context("Delete", func() {
		JustBeforeEach(func() {
			err = updateHandlerList.Delete(ctx, &mocks.Tx{}, "foo")
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
	})
})
