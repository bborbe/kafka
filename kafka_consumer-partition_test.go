// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	stderrors "errors"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

var _ = Describe("CreatePartitionConsumer", func() {
	var ctx context.Context
	var err error
	var partitionConsumer sarama.PartitionConsumer
	var topic libkafka.Topic
	var partition libkafka.Partition
	var fallbackOffset libkafka.Offset
	var nextOffext libkafka.Offset
	var consumerFromClient *mocks.KafkaSaramaConsumer
	var metricsConsumer *mocks.KafkaMetrics
	BeforeEach(func() {
		ctx = context.Background()
		topic = "my-topic"
		partition = 42
		fallbackOffset = libkafka.OffsetNewest
		nextOffext = 1337
		consumerFromClient = &mocks.KafkaSaramaConsumer{}
		metricsConsumer = &mocks.KafkaMetrics{}
	})
	JustBeforeEach(func() {
		partitionConsumer, err = libkafka.CreatePartitionConsumer(
			ctx,
			consumerFromClient,
			metricsConsumer,
			topic,
			partition,
			fallbackOffset,
			nextOffext,
		)
	})
	Context("without error", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturns(&mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
			{
				argTopic, argPartition, argOffset := consumerFromClient.ConsumePartitionArgsForCall(0)
				Expect(argTopic).To(Equal(topic.String()))
				Expect(argPartition).To(Equal(partition.Int32()))
				Expect(argOffset).To(Equal(nextOffext.Int64()))
			}
		})
		It("calls metrics methods correctly for success", func() {
			Expect(metricsConsumer.ConsumePartitionCreateTotalIncCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateOutOfRangeErrorInitializeCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateSuccessIncCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateFailureIncCallCount()).To(Equal(0))
			Expect(metricsConsumer.ConsumePartitionCreateOutOfRangeErrorIncCallCount()).To(Equal(0))
		})
	})
	Context("with outOfRange error via string matching", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, stderrors.New(libkafka.OutOfRangeErrorMessage))
			consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
			{
				argTopic, argPartition, argOffset := consumerFromClient.ConsumePartitionArgsForCall(0)
				Expect(argTopic).To(Equal(topic.String()))
				Expect(argPartition).To(Equal(partition.Int32()))
				Expect(argOffset).To(Equal(nextOffext.Int64()))
			}
			{
				argTopic, argPartition, argOffset := consumerFromClient.ConsumePartitionArgsForCall(1)
				Expect(argTopic).To(Equal(topic.String()))
				Expect(argPartition).To(Equal(partition.Int32()))
				Expect(argOffset).To(Equal(fallbackOffset.Int64()))
			}
		})
		It("calls metrics methods correctly for outOfRange recovery", func() {
			Expect(metricsConsumer.ConsumePartitionCreateTotalIncCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateOutOfRangeErrorInitializeCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateFailureIncCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateOutOfRangeErrorIncCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateSuccessIncCallCount()).To(Equal(0))
		})
	})
	Context("with outOfRange error via sarama.ErrOffsetOutOfRange", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, sarama.ErrOffsetOutOfRange)
			consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
		})
	})
	Context("with outOfRange error via sarama.KError", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, sarama.KError(sarama.ErrOffsetOutOfRange))
			consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
		})
	})
	Context("with wrapped outOfRange error via sarama.ErrOffsetOutOfRange", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, errors.Wrapf(ctx, sarama.ErrOffsetOutOfRange, "wrapped error"))
			consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
		})
	})
	Context("with wrapped outOfRange error via sarama.KError", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, errors.Wrapf(ctx, sarama.KError(sarama.ErrOffsetOutOfRange), "wrapped kerror"))
			consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
		})
	})
	Context("with fallback offset failure after outOfRange error", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, sarama.ErrOffsetOutOfRange)
			consumerFromClient.ConsumePartitionReturnsOnCall(1, nil, stderrors.New("fallback failed"))
		})
		It("returns error from fallback", func() {
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("fallback failed"))
		})
		It("returns no partitionConsumer", func() {
			Expect(partitionConsumer).To(BeNil())
		})
		It("calls ConsumePartition twice", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
		})
	})
	Context("with multiple wrapped offset errors", func() {
		BeforeEach(func() {
			wrappedErr := errors.Wrapf(ctx, errors.Wrapf(ctx, sarama.ErrOffsetOutOfRange, "inner wrap"), "outer wrap")
			consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, wrappedErr)
			consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition twice", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
		})
	})
	Context("with partial string match that should not trigger fallback", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturns(nil, stderrors.New("offset is outside but different error"))
		})
		It("returns error without fallback", func() {
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("offset is outside but different error"))
		})
		It("returns no partitionConsumer", func() {
			Expect(partitionConsumer).To(BeNil())
		})
		It("calls ConsumePartition only once", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
		})
	})
	Context("with different sarama.KError that should not trigger fallback", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturns(nil, sarama.KError(sarama.ErrNotLeaderForPartition))
		})
		It("returns error without fallback", func() {
			Expect(err).NotTo(BeNil())
		})
		It("returns no partitionConsumer", func() {
			Expect(partitionConsumer).To(BeNil())
		})
		It("calls ConsumePartition only once", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
		})
	})
	Context("with nil error from ConsumePartition", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturns(&mocks.KafkaSaramaPartitionConsumer{}, nil)
		})
		It("returns no error", func() {
			Expect(err).To(BeNil())
		})
		It("returns partitionConsumer", func() {
			Expect(partitionConsumer).NotTo(BeNil())
		})
		It("calls ConsumePartition only once", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
		})
	})
	Context("any error", func() {
		BeforeEach(func() {
			consumerFromClient.ConsumePartitionReturns(nil, stderrors.New("banana"))
		})
		It("returns error", func() {
			Expect(err).NotTo(BeNil())
		})
		It("returns no partitionConsumer", func() {
			Expect(partitionConsumer).To(BeNil())
		})
		It("calls ConsumePartition", func() {
			Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
			{
				argTopic, argPartition, argOffset := consumerFromClient.ConsumePartitionArgsForCall(0)
				Expect(argTopic).To(Equal(topic.String()))
				Expect(argPartition).To(Equal(partition.Int32()))
				Expect(argOffset).To(Equal(nextOffext.Int64()))
			}
		})
		It("calls metrics methods correctly for general error", func() {
			Expect(metricsConsumer.ConsumePartitionCreateTotalIncCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateOutOfRangeErrorInitializeCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateFailureIncCallCount()).To(Equal(1))
			Expect(metricsConsumer.ConsumePartitionCreateOutOfRangeErrorIncCallCount()).To(Equal(0))
			Expect(metricsConsumer.ConsumePartitionCreateSuccessIncCallCount()).To(Equal(0))
		})
	})

	Context("parameter validation", func() {
		Context("with different topic values", func() {
			BeforeEach(func() {
				topic = "test-topic-123"
				consumerFromClient.ConsumePartitionReturns(&mocks.KafkaSaramaPartitionConsumer{}, nil)
			})
			It("uses correct topic string", func() {
				Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
				argTopic, _, _ := consumerFromClient.ConsumePartitionArgsForCall(0)
				Expect(argTopic).To(Equal("test-topic-123"))
			})
		})
		Context("with different partition values", func() {
			BeforeEach(func() {
				partition = 999
				consumerFromClient.ConsumePartitionReturns(&mocks.KafkaSaramaPartitionConsumer{}, nil)
			})
			It("uses correct partition int32", func() {
				Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
				_, argPartition, _ := consumerFromClient.ConsumePartitionArgsForCall(0)
				Expect(argPartition).To(Equal(int32(999)))
			})
		})
		Context("with different offset values", func() {
			BeforeEach(func() {
				nextOffext = 12345
				fallbackOffset = -1
				consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, sarama.ErrOffsetOutOfRange)
				consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
			})
			It("uses correct offset values", func() {
				Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
				_, _, argOffset1 := consumerFromClient.ConsumePartitionArgsForCall(0)
				_, _, argOffset2 := consumerFromClient.ConsumePartitionArgsForCall(1)
				Expect(argOffset1).To(Equal(int64(12345)))
				Expect(argOffset2).To(Equal(int64(-1)))
			})
		})
	})

	Context("comprehensive error chain testing", func() {
		Context("with nested wrapped errors containing offset error", func() {
			BeforeEach(func() {
				deepErr := errors.Wrapf(ctx,
					errors.Wrapf(ctx,
						errors.Wrapf(ctx, sarama.ErrOffsetOutOfRange, "level 1"),
						"level 2"),
					"level 3")
				consumerFromClient.ConsumePartitionReturnsOnCall(0, nil, deepErr)
				consumerFromClient.ConsumePartitionReturnsOnCall(1, &mocks.KafkaSaramaPartitionConsumer{}, nil)
			})
			It("successfully unwraps and detects offset error", func() {
				Expect(err).To(BeNil())
				Expect(partitionConsumer).NotTo(BeNil())
				Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(2))
			})
		})
		Context("with error chain containing non-offset error", func() {
			BeforeEach(func() {
				deepErr := errors.Wrapf(ctx,
					errors.Wrapf(ctx,
						errors.Wrapf(ctx, sarama.ErrNotLeaderForPartition, "level 1"),
						"level 2"),
					"level 3")
				consumerFromClient.ConsumePartitionReturns(nil, deepErr)
			})
			It("does not trigger fallback for non-offset error", func() {
				Expect(err).NotTo(BeNil())
				Expect(partitionConsumer).To(BeNil())
				Expect(consumerFromClient.ConsumePartitionCallCount()).To(Equal(1))
			})
		})
	})
})

var _ = Describe("IsOffsetOutOfRange", func() {
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
	})

	Context("with nil error", func() {
		It("returns false", func() {
			Expect(libkafka.IsOffsetOutOfRange(nil)).To(BeFalse())
		})
	})

	Context("with sarama.ErrOffsetOutOfRange", func() {
		It("returns true", func() {
			Expect(libkafka.IsOffsetOutOfRange(sarama.ErrOffsetOutOfRange)).To(BeTrue())
		})
	})

	Context("with sarama.KError offset out of range", func() {
		It("returns true", func() {
			err := sarama.KError(sarama.ErrOffsetOutOfRange)
			Expect(libkafka.IsOffsetOutOfRange(err)).To(BeTrue())
		})
	})

	Context("with wrapped sarama.ErrOffsetOutOfRange", func() {
		It("returns true", func() {
			wrappedErr := errors.Wrapf(ctx, sarama.ErrOffsetOutOfRange, "wrapped error")
			Expect(libkafka.IsOffsetOutOfRange(wrappedErr)).To(BeTrue())
		})
	})

	Context("with wrapped sarama.KError", func() {
		It("returns true", func() {
			wrappedErr := errors.Wrapf(ctx, sarama.KError(sarama.ErrOffsetOutOfRange), "wrapped kerror")
			Expect(libkafka.IsOffsetOutOfRange(wrappedErr)).To(BeTrue())
		})
	})

	Context("with multiple wrapped errors", func() {
		It("returns true", func() {
			deepErr := errors.Wrapf(ctx,
				errors.Wrapf(ctx,
					errors.Wrapf(ctx, sarama.ErrOffsetOutOfRange, "level 1"),
					"level 2"),
				"level 3")
			Expect(libkafka.IsOffsetOutOfRange(deepErr)).To(BeTrue())
		})
	})

	Context("with string matching", func() {
		It("returns true for exact message", func() {
			err := stderrors.New(libkafka.OutOfRangeErrorMessage)
			Expect(libkafka.IsOffsetOutOfRange(err)).To(BeTrue())
		})

		It("returns true for message containing substring", func() {
			err := stderrors.New("some prefix " + libkafka.OutOfRangeErrorMessage + " some suffix")
			Expect(libkafka.IsOffsetOutOfRange(err)).To(BeTrue())
		})

		It("returns false for partial match", func() {
			err := stderrors.New("offset is outside but different error")
			Expect(libkafka.IsOffsetOutOfRange(err)).To(BeFalse())
		})
	})

	Context("with different sarama errors", func() {
		It("returns false for non-offset errors", func() {
			Expect(libkafka.IsOffsetOutOfRange(sarama.ErrNotLeaderForPartition)).To(BeFalse())
			Expect(libkafka.IsOffsetOutOfRange(sarama.ErrInvalidMessage)).To(BeFalse())
			Expect(libkafka.IsOffsetOutOfRange(sarama.KError(sarama.ErrNotLeaderForPartition))).To(BeFalse())
		})
	})

	Context("with wrapped non-offset errors", func() {
		It("returns false", func() {
			wrappedErr := errors.Wrapf(ctx, sarama.ErrNotLeaderForPartition, "wrapped non-offset error")
			Expect(libkafka.IsOffsetOutOfRange(wrappedErr)).To(BeFalse())
		})
	})

	Context("with generic errors", func() {
		It("returns false", func() {
			err := stderrors.New("some random error")
			Expect(libkafka.IsOffsetOutOfRange(err)).To(BeFalse())
		})
	})
})
