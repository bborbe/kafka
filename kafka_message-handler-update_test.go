// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	"github.com/IBM/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
)

var _ = Describe("essageHandlerUpdate", func() {
	type MyKey string
	type MyStruct struct {
		Name string `json:"name"`
	}
	var ctx context.Context
	var err error
	var updateCounter int
	var deleteCounter int
	var updaterHandler kafka.UpdaterHandler[MyKey, MyStruct]
	var messageHandlerUpdate kafka.MessageHandler
	var msg *sarama.ConsumerMessage
	BeforeEach(func() {
		ctx = context.Background()
		updateCounter = 0
		deleteCounter = 0
		updaterHandler = kafka.UpdaterHandlerFunc[MyKey, MyStruct](
			func(ctx context.Context, key MyKey, object MyStruct) error {
				updateCounter++
				return nil
			},
			func(ctx context.Context, key MyKey) error {
				deleteCounter++
				return nil
			},
		)
		messageHandlerUpdate = kafka.NewMessageHandlerUpdate[MyKey, MyStruct](updaterHandler)
		msg = &sarama.ConsumerMessage{}
	})
	Context("ConsumeMessage", func() {
		JustBeforeEach(func() {
			err = messageHandlerUpdate.ConsumeMessage(ctx, msg)
		})
		Context("Update", func() {
			BeforeEach(func() {
				msg.Key = []byte("my-key")
				msg.Value = []byte(`{"name":"banana"}`)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("calls update", func() {
				Expect(updateCounter).To(Equal(1))
			})
			It("calls not delete", func() {
				Expect(deleteCounter).To(Equal(0))
			})
		})
		Context("Delete", func() {
			BeforeEach(func() {
				msg.Key = []byte("my-key")
				msg.Value = []byte(``)
			})
			It("returns no error", func() {
				Expect(err).To(BeNil())
			})
			It("calls update", func() {
				Expect(updateCounter).To(Equal(0))
			})
			It("calls not delete", func() {
				Expect(deleteCounter).To(Equal(1))
			})
		})
	})
})
