// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"
	stderrors "errors"
	stdtime "time"

	libkv "github.com/bborbe/kv"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/kafka/mocks"
)

func newMapBackedOffsetStore() (*mocks.KafkaOffsetStore, map[libkafka.TopicPartition]libkafka.Offset) {
	offsets := map[libkafka.TopicPartition]libkafka.Offset{}
	offsetStore := &mocks.KafkaOffsetStore{}
	offsetStore.GetCalls(func(
		ctx context.Context,
		topic libkafka.Topic,
		partition libkafka.Partition,
	) (libkafka.Offset, error) {
		offset, ok := offsets[libkafka.TopicPartition{Topic: topic, Partition: partition}]
		if !ok {
			return 0, libkv.KeyNotFoundError
		}
		return offset, nil
	})
	offsetStore.SetCalls(func(
		ctx context.Context,
		topic libkafka.Topic,
		partition libkafka.Partition,
		offset libkafka.Offset,
	) error {
		offsets[libkafka.TopicPartition{Topic: topic, Partition: partition}] = offset
		return nil
	})
	return offsetStore, offsets
}

var _ = Describe("TTLSeekOffsetManager", func() {
	var ctx context.Context
	var topic libkafka.Topic
	var partition libkafka.Partition
	var fixedNow stdtime.Time
	var ttl libtime.Duration
	var currentDateTime libtime.CurrentDateTimeGetter
	var saramaClient *mocks.KafkaSaramaClient
	var offsetStore *mocks.KafkaOffsetStore

	BeforeEach(func() {
		ctx = context.Background()
		topic = libkafka.Topic("my-topic")
		partition = libkafka.Partition(0)
		fixedNow = stdtime.Date(2026, 10, 24, 12, 0, 0, 0, stdtime.UTC)
		currentDateTime = libtime.CurrentDateTimeGetterFunc(func() libtime.DateTime {
			return libtime.DateTime(fixedNow)
		})
		ttl = libtime.Duration(libtime.Hour)
		saramaClient = &mocks.KafkaSaramaClient{}
		offsetStore = &mocks.KafkaOffsetStore{}
	})

	Context("store hit", func() {
		BeforeEach(func() {
			offsetStore.GetReturns(libkafka.Offset(1234), nil)
		})
		It("is served without contacting the broker", func() {
			offsetManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := offsetManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(1234)))
			Expect(saramaClient.GetOffsetCallCount()).To(Equal(0))
		})
	})

	Context("store miss", func() {
		BeforeEach(func() {
			offsetStore.GetReturns(0, libkv.KeyNotFoundError)
			saramaClient.GetOffsetReturns(5000, nil)
		})
		It("seeks at now minus ttl in unix milliseconds", func() {
			offsetManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := offsetManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(5000)))
			Expect(saramaClient.GetOffsetCallCount()).To(Equal(1))

			seekTopic, seekPartition, seekTimestamp := saramaClient.GetOffsetArgsForCall(0)
			Expect(seekTopic).To(Equal(topic.String()))
			Expect(seekPartition).To(Equal(partition.Int32()))
			Expect(seekTimestamp).To(Equal(fixedNow.Add(-stdtime.Hour).UnixMilli()))
		})
	})

	Context("bucket miss", func() {
		BeforeEach(func() {
			offsetStore.GetReturns(0, libkv.BucketNotFoundError)
			saramaClient.GetOffsetReturns(5000, nil)
		})
		It("also seeks", func() {
			offsetManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			_, err := offsetManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(saramaClient.GetOffsetCallCount()).To(Equal(1))
		})
	})

	Context("persistence", func() {
		BeforeEach(func() {
			offsetStore, _ = newMapBackedOffsetStore()
			saramaClient.GetOffsetReturns(5000, nil)
		})
		It("persists the resolved offset before returning it", func() {
			firstManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := firstManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(5000)))

			_, _, _, storedOffset := offsetStore.SetArgsForCall(0)
			Expect(storedOffset).To(Equal(libkafka.Offset(5000)))

			secondManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err = secondManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(5000)))
			Expect(saramaClient.GetOffsetCallCount()).To(Equal(1))
		})
	})

	Context("persist failure", func() {
		BeforeEach(func() {
			offsetStore, _ = newMapBackedOffsetStore()
			saramaClient.GetOffsetReturns(5000, nil)
		})
		It("returns an error and does not return the offset", func() {
			offsetStore.SetReturns(stderrors.New("disk full"))

			firstManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := firstManager.NextOffset(ctx, topic, partition)
			Expect(err).NotTo(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(0)))

			secondManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			_, err = secondManager.NextOffset(ctx, topic, partition)
			Expect(err).NotTo(BeNil())
			Expect(saramaClient.GetOffsetCallCount()).To(Equal(2))
		})
	})

	Context("broker seek failure", func() {
		BeforeEach(func() {
			offsetStore.GetReturns(0, libkv.KeyNotFoundError)
			saramaClient.GetOffsetReturns(0, stderrors.New("broker unreachable"))
		})
		It("returns an error instead of a sentinel", func() {
			offsetManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := offsetManager.NextOffset(ctx, topic, partition)
			Expect(err).NotTo(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(0)))
			Expect(offset).NotTo(Equal(libkafka.OffsetNewest))
			Expect(offset).NotTo(Equal(libkafka.OffsetOldest))
		})
	})

	Context("non-miss store error", func() {
		BeforeEach(func() {
			offsetStore.GetReturns(0, stderrors.New("disk full"))
		})
		It("returns an error and never reaches the broker", func() {
			offsetManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := offsetManager.NextOffset(ctx, topic, partition)
			Expect(err).NotTo(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(0)))
			Expect(saramaClient.GetOffsetCallCount()).To(Equal(0))
		})
	})

	Context("negative broker result", func() {
		BeforeEach(func() {
			offsetStore.GetReturns(0, libkv.KeyNotFoundError)
			saramaClient.GetOffsetReturns(-2, nil)
		})
		It("resolves to the newest sentinel and persists nothing", func() {
			offsetManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := offsetManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.OffsetNewest))
			Expect(offsetStore.SetCallCount()).To(Equal(0))
		})
	})

	Context("sentinels", func() {
		It("reports the newest offset for both", func() {
			offsetManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			Expect(offsetManager.InitialOffset()).To(Equal(libkafka.OffsetNewest))
			Expect(offsetManager.FallbackOffset()).To(Equal(libkafka.OffsetNewest))
		})
	})

	Context("delegation and close", func() {
		BeforeEach(func() {
			offsetStore, _ = newMapBackedOffsetStore()
		})
		It("delegates mark, reset and close", func() {
			firstManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			Expect(firstManager.MarkOffset(ctx, topic, partition, libkafka.Offset(700))).
				To(BeNil())

			secondManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err := secondManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(700)))

			Expect(firstManager.ResetOffset(ctx, topic, partition, libkafka.Offset(500))).
				To(BeNil())

			thirdManager := libkafka.NewTTLSeekOffsetManager(
				saramaClient, offsetStore, ttl, currentDateTime,
			)
			offset, err = thirdManager.NextOffset(ctx, topic, partition)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(libkafka.Offset(500)))

			Expect(firstManager.Close()).To(BeNil())
			offset, err = firstManager.NextOffset(ctx, topic, partition)
			Expect(err).NotTo(BeNil())
			Expect(stderrors.Is(err, libkafka.ErrClosed)).To(BeTrue())
			Expect(offset).To(Equal(libkafka.Offset(0)))
		})
	})
})
