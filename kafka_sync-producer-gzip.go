// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

// GzipMaxMsgBytes is the default message-value threshold (1 MiB) above which
// NewSyncProducerGzipValue will gzip-compress the payload. Below the
// threshold, messages pass through uncompressed. Wire-compatible with the
// legacy seibert-data/lib-kafka constant of the same name and value.
const GzipMaxMsgBytes int = 1048576

// NewSyncProducerGzipValue wraps a SyncProducer and transparently gzip-compresses
// any message whose value exceeds maxMsgBytes. Compressed messages get the
// GzipHeaderKey / GzipHeaderValue header so consumers can detect and decompress.
// Smaller messages and nil/empty values pass through unmodified.
//
// Use this as a drop-in replacement for the legacy
// seibert-data/lib-kafka/producer.NewSyncProducerGzipValue. The on-the-wire
// format (header + compressed bytes) is identical.
func NewSyncProducerGzipValue(
	syncProducer SyncProducer,
	maxMsgBytes int,
) SyncProducer {
	return &syncProducerGzipValue{
		syncProducer: syncProducer,
		maxMsgBytes:  maxMsgBytes,
	}
}

type syncProducerGzipValue struct {
	syncProducer SyncProducer
	maxMsgBytes  int
}

// SendMessage compresses the message value when above the threshold and
// delegates to the underlying SyncProducer.
func (s *syncProducerGzipValue) SendMessage(
	ctx context.Context,
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	if err := s.compressIfNeeded(ctx, msg); err != nil {
		return -1, -1, err
	}
	return s.syncProducer.SendMessage(ctx, msg)
}

// SendMessages compresses each message's value when above the threshold and
// delegates to the underlying SyncProducer.
func (s *syncProducerGzipValue) SendMessages(
	ctx context.Context,
	msgs []*sarama.ProducerMessage,
) error {
	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := s.compressIfNeeded(ctx, msg); err != nil {
				return err
			}
		}
	}
	return s.syncProducer.SendMessages(ctx, msgs)
}

// Close closes the underlying SyncProducer.
func (s *syncProducerGzipValue) Close() error {
	return s.syncProducer.Close()
}

func (s *syncProducerGzipValue) compressIfNeeded(
	ctx context.Context,
	msg *sarama.ProducerMessage,
) error {
	if msg.Value == nil || msg.Value.Length() == 0 {
		return nil
	}
	if msg.Value.Length() <= s.maxMsgBytes {
		return nil
	}
	value, err := msg.Value.Encode()
	if err != nil {
		return errors.Wrapf(ctx, err, "encode value failed")
	}
	encoded, err := NewGzipEncoder(ctx, value)
	if err != nil {
		return errors.Wrapf(ctx, err, "gzip value failed")
	}
	msg.Value = encoded
	msg.Headers = append(msg.Headers, sarama.RecordHeader{
		Key:   []byte(GzipHeaderKey),
		Value: []byte(GzipHeaderValue),
	})
	return nil
}
