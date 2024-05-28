// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

func NewSyncProducerWithHeader(
	ctx context.Context,
	brokers Brokers,
	headers Header,
	opts ...SaramaConfigOptions,
) (SyncProducer, error) {
	syncProducer, err := NewSyncProducer(ctx, brokers, opts...)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create sync producer failed")
	}
	return NewSyncProducerModify(syncProducer, func(ctx context.Context, msg *sarama.ProducerMessage) error {
		for key, values := range headers {
			for _, value := range values {
				msg.Headers = append(msg.Headers, sarama.RecordHeader{
					Key:   []byte(key),
					Value: []byte(value),
				})
			}
		}
		return nil
	}), nil
}

func NewSyncProducerWithName(
	ctx context.Context,
	brokers Brokers,
	name string,
	opts ...SaramaConfigOptions,
) (SyncProducer, error) {
	return NewSyncProducerWithHeader(
		ctx,
		brokers,
		Header{"name": []string{name}},
		opts...,
	)
}
