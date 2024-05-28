// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

func NewSyncProducerMetrics(
	syncProducer SyncProducer,
	metricsSyncProducer MetricsSyncProducer,
) SyncProducer {
	return &syncProducerMetrics{
		syncProducer:        syncProducer,
		metricsSyncProducer: metricsSyncProducer,
	}
}

type syncProducerMetrics struct {
	syncProducer        SyncProducer
	metricsSyncProducer MetricsSyncProducer
}

func (s *syncProducerMetrics) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	start := time.Now()
	s.metricsSyncProducer.SyncProducerTotalCounterInc(Topic(msg.Topic))
	partition, offset, err := s.syncProducer.SendMessage(ctx, msg)
	if err != nil {
		s.metricsSyncProducer.SyncProducerFailureCounterInc(Topic(msg.Topic))
		return 0, 0, err
	}
	s.metricsSyncProducer.SyncProducerSuccessCounterInc(Topic(msg.Topic))
	s.metricsSyncProducer.SyncProducerDurationMeasure(Topic(msg.Topic), time.Since(start))
	return partition, offset, nil
}

func (s *syncProducerMetrics) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error {
	start := time.Now()
	for _, msg := range msgs {
		s.metricsSyncProducer.SyncProducerTotalCounterInc(Topic(msg.Topic))
	}
	if err := s.syncProducer.SendMessages(ctx, msgs); err != nil {
		for _, msg := range msgs {
			s.metricsSyncProducer.SyncProducerFailureCounterInc(Topic(msg.Topic))
		}
		return err
	}
	for _, msg := range msgs {
		s.metricsSyncProducer.SyncProducerSuccessCounterInc(Topic(msg.Topic))
		s.metricsSyncProducer.SyncProducerDurationMeasure(Topic(msg.Topic), time.Since(start)/time.Duration(len(msgs)))
	}
	return nil
}

func (s *syncProducerMetrics) Close() error {
	return s.syncProducer.Close()
}
