// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

//counterfeiter:generate -o mocks/kafka-sarama-client.go --fake-name KafkaSaramaClient . SaramaClient
type SaramaClient interface {
	sarama.Client
}

func CreateSaramaClient(
	ctx context.Context,
	brokers Brokers,
	opts ...SaramaConfigOptions,
) (SaramaClient, error) {
	saramaConfig, err := CreateSaramaConfig(ctx, brokers, opts...)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create sarama config failed")
	}
	saramaClient, err := sarama.NewClient(brokers.Hosts(), saramaConfig)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create client failed")
	}
	return saramaClient, nil
}
