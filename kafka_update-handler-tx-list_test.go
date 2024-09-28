package kafka_test

import (
	"context"
	"github.com/bborbe/kafka"
	"github.com/bborbe/kv/mocks"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
