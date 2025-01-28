package kafka_test

import (
	"context"
	stderrors "errors"
	"github.com/IBM/sarama"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
	BeforeEach(func() {
		ctx = context.Background()
		topic = "my-topic"
		partition = 42
		fallbackOffset = libkafka.OffsetNewest
		nextOffext = 1337
		consumerFromClient = &mocks.KafkaSaramaConsumer{}
	})
	JustBeforeEach(func() {
		partitionConsumer, err = libkafka.CreatePartitionConsumer(ctx, consumerFromClient, topic, partition, fallbackOffset, nextOffext)
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
	})
	Context("with outOfRange error", func() {
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
	})
})
