// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

func NewSyncProducerModify(
	syncProducer SyncProducer,
	fn func(ctx context.Context, message *sarama.ProducerMessage) error,
) SyncProducer {
	return &syncProducerModify{
		syncProducer: syncProducer,
		fn:           fn,
	}
}

type syncProducerModify struct {
	syncProducer SyncProducer
	fn           func(ctx context.Context, message *sarama.ProducerMessage) error
}

func (s *syncProducerModify) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if err := s.fn(ctx, msg); err != nil {
		return 0, 0, err
	}
	return s.syncProducer.SendMessage(ctx, msg)
}

func (s *syncProducerModify) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := s.fn(ctx, msg); err != nil {
				return errors.Wrapf(ctx, err, "modify message failed")
			}
		}
	}
	return s.syncProducer.SendMessages(ctx, msgs)
}

func (s *syncProducerModify) Close() error {
	return s.syncProducer.Close()
}
