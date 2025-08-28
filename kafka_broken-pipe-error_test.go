package kafka_test

import (
	"context"
	stderrors "errors"

	"github.com/bborbe/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("IsBrokenPipeError", func() {
	var err error
	var isBrokenPipeError bool
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
	})
	JustBeforeEach(func() {
		isBrokenPipeError = libkafka.IsBrokenPipeError(err)
	})
	Context("no broken pipe error", func() {
		BeforeEach(func() {
			err = stderrors.New("banana")
		})
		It("return false", func() {
			Expect(isBrokenPipeError).To(BeFalse())
		})
	})
	Context("broken pipe error", func() {
		BeforeEach(func() {
			err = stderrors.New("broken pipe")
		})
		It("return true", func() {
			Expect(isBrokenPipeError).To(BeTrue())
		})
	})
	Context("broken pipe error with prefix", func() {
		BeforeEach(func() {
			err = stderrors.New("bla bla bla: broken pipe")
		})
		It("return true", func() {
			Expect(isBrokenPipeError).To(BeTrue())
		})
	})
	Context("broken pipe error with prefix", func() {
		BeforeEach(func() {
			err = errors.Wrap(ctx, stderrors.New("broken pipe"), "bla bla bla")
		})
		It("return true", func() {
			Expect(isBrokenPipeError).To(BeTrue())
		})
	})
})
