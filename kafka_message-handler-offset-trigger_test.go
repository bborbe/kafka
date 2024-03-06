// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/bborbe/run"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = Describe("OffsetTriggerMessageHandler", func() {
	var ctx context.Context
	var messageHandler libkafka.MessageHandler
	var trigger run.Trigger
	var triggerOffsets map[libkafka.Partition]libkafka.Offset
	var messages []sarama.ConsumerMessage
	var topic libkafka.Topic
	BeforeEach(func() {
		ctx = context.Background()
		trigger = run.NewTrigger()
		topic = "my-topic"
		triggerOffsets = map[libkafka.Partition]libkafka.Offset{
			0: 5,
		}
	})
	JustBeforeEach(func() {
		messageHandler = libkafka.NewOffsetTriggerMessageHandler(
			triggerOffsets,
			topic,
			trigger,
		)
		for _, message := range messages {
			err := messageHandler.ConsumeMessage(ctx, &message)
			Expect(err).To(BeNil())
		}
		time.Sleep(50 * time.Millisecond)
	})
	Context("before highwater", func() {
		BeforeEach(func() {
			messages = []sarama.ConsumerMessage{
				{
					Partition: 0,
					Offset:    1,
				},
			}
		})
		It("trigger", func() {
			select {
			case <-trigger.Done():
				Fail("should not be done")
			default:
				Succeed()
			}
		})
	})
	Context("highwater reached", func() {
		BeforeEach(func() {
			messages = []sarama.ConsumerMessage{
				{
					Partition: 0,
					Offset:    5,
				},
			}
		})
		It("trigger", func() {
			select {
			case <-trigger.Done():
				Succeed()
			default:
				Fail("should be done")
			}
		})
	})
	Context("after highwater", func() {
		BeforeEach(func() {
			messages = []sarama.ConsumerMessage{
				{
					Partition: 0,
					Offset:    9,
				},
			}
		})
		It("trigger", func() {
			select {
			case <-trigger.Done():
				Succeed()
			default:
				Fail("should not be done")
			}
		})
	})
	Context("multiple offset", func() {
		BeforeEach(func() {

			triggerOffsets = map[libkafka.Partition]libkafka.Offset{
				0: 5,
				1: 5,
			}
		})
		Context("no reached", func() {
			BeforeEach(func() {
				messages = []sarama.ConsumerMessage{
					{
						Partition: 0,
						Offset:    1,
					},
					{
						Partition: 1,
						Offset:    1,
					},
				}
			})
			It("trigger", func() {
				select {
				case <-trigger.Done():
					Fail("should not be done")
				default:
					Succeed()
				}
			})
		})
		Context("some reached", func() {
			BeforeEach(func() {
				messages = []sarama.ConsumerMessage{
					{
						Partition: 0,
						Offset:    5,
					},
					{
						Partition: 1,
						Offset:    1,
					},
				}
			})
			It("trigger", func() {
				select {
				case <-trigger.Done():
					Fail("should not be done")
				default:
					Succeed()
				}
			})
		})
		Context("all reached", func() {
			BeforeEach(func() {
				messages = []sarama.ConsumerMessage{
					{
						Partition: 0,
						Offset:    5,
					},
					{
						Partition: 1,
						Offset:    9,
					},
				}
			})
			It("trigger", func() {
				select {
				case <-trigger.Done():
					Succeed()
				default:
					Fail("should be done")
				}
			})
		})
	})
})
