package kafka_test

import (
	"context"
	"github.com/bborbe/kafka"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("UpdaterHandlerList", func() {
	var ctx context.Context
	var err error
	var updateHandlerList kafka.UpdaterHandler[string, struct{}]
	BeforeEach(func() {
		ctx = context.Background()
		updateHandlerList = kafka.UpdaterHandlerList[string, struct{}]{}
	})
	Context("Update", func() {
		JustBeforeEach(func() {
			err = updateHandlerList.Update(ctx, "foo", struct{}{})
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
	})
	Context("Delete", func() {
		JustBeforeEach(func() {
			err = updateHandlerList.Delete(ctx, "foo")
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
	})
})
