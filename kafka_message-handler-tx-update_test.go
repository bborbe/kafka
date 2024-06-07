// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	"github.com/IBM/sarama"
	libkv "github.com/bborbe/kv"
	kvmocks "github.com/bborbe/kv/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka"
)

var _ = Describe("MessageHandlerTxUpdate", func() {
	type MyKey string
	type MyStruct struct {
		Name string `json:"name"`
	}
	var ctx context.Context
	var err error
	var updateCounter int
	var deleteCounter int
	var updaterHandlerTx kafka.UpdaterHandlerTx[MyStruct, MyKey]
	var messageHandlerTxUpdate kafka.MessageHandlerTx
	var msg *sarama.ConsumerMessage
	BeforeEach(func() {
		ctx = context.Background()
		updateCounter = 0
		deleteCounter = 0
		updaterHandlerTx = kafka.UpdaterHandlerTxFunc[MyStruct, MyKey](
			func(ctx context.Context, tx libkv.Tx, key MyKey, object MyStruct) error {
				updateCounter++
				return nil
			},
			func(ctx context.Context, tx libkv.Tx, key MyKey) error {
				deleteCounter++
				return nil
			},
		)
		messageHandlerTxUpdate = kafka.NewMessageHandlerTxUpdate[MyStruct, MyKey](updaterHandlerTx)
		msg = &sarama.ConsumerMessage{}
	})
	Context("ConsumeMessage", func() {
		JustBeforeEach(func() {
			err = messageHandlerTxUpdate.ConsumeMessage(ctx, &kvmocks.Tx{}, msg)
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
