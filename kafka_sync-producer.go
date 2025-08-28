// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

//counterfeiter:generate -o mocks/kafka-sync-producer.go --fake-name KafkaSyncProducer . SyncProducer

// SyncProducer defines the interface for synchronously sending messages to Kafka.
type SyncProducer interface {
	// SendMessage sends a single message to Kafka and returns the partition and offset where it was stored.
	SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	// SendMessages sends multiple messages to Kafka in a single batch operation.
	SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error
	// Close closes the producer and releases its resources.
	Close() error
}

// NewSyncProducer creates a new synchronous Kafka producer with the given brokers and configuration options.
func NewSyncProducer(
	ctx context.Context,
	brokers Brokers,
	opts ...SaramaConfigOptions,
) (SyncProducer, error) {
	saramaConfig, err := CreateSaramaConfig(ctx, brokers, opts...)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create sarama config failed")
	}
	saramaSyncProducer, err := sarama.NewSyncProducer(brokers.Hosts(), saramaConfig)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create sync producer failed")
	}
	return NewSyncProducerFromSaramaSyncProducer(saramaSyncProducer), nil
}

// NewSyncProducerFromSaramaSyncProducer creates a new SyncProducer wrapper around an existing Sarama SyncProducer.
func NewSyncProducerFromSaramaSyncProducer(saramaSyncProducer sarama.SyncProducer) SyncProducer {
	return &syncProducer{
		saramaSyncProducer: saramaSyncProducer,
	}
}

type syncProducer struct {
	saramaSyncProducer sarama.SyncProducer
}

func (s *syncProducer) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	partition, offset, err := s.saramaSyncProducer.SendMessage(msg)
	if err != nil {
		return -1, -1, errors.Wrapf(ctx, err, "send message failed")
	}
	return partition, offset, nil
}

func (s *syncProducer) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error {
	if err := s.saramaSyncProducer.SendMessages(msgs); err != nil {
		return errors.Wrapf(ctx, err, "send %d messages failed", len(msgs))
	}
	return nil
}

func (s *syncProducer) Close() error {
	return s.saramaSyncProducer.Close()
}
