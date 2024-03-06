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
type SyncProducer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	SendMessages(msgs []*sarama.ProducerMessage) error
	Close() error
}

func NewSyncProducer(
	ctx context.Context,
	brokers Brokers,
	opts ...SaramaConfigOptions,
) (SyncProducer, error) {
	saramaConfig := CreateSaramaConfig(opts...)
	syncProducer, err := sarama.NewSyncProducer(brokers.Strings(), saramaConfig)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create sync producer failed")
	}
	return syncProducer, nil
}
