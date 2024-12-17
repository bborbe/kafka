// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

type SaramaConfigOptions func(config *sarama.Config)

func CreateSaramaConfig(
	ctx context.Context,
	brokers Brokers,
	opts ...SaramaConfigOptions,
) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.Retention = 14 * 24 * time.Hour // 14 days
	config.Consumer.Return.Errors = true
	config.Metadata.Retry.Max = 10
	config.Admin.Retry.Max = 10
	config.Admin.Retry.Backoff = time.Second

	if brokers.Schemas().Contains(TLSSchema) {
		tlsConfig, err := NewTLSConfig(
			ctx,
			"/client-cert/file",
			"/client-key/file",
			"/server-cert/file",
		)
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "read tls files failed")
		}
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
		config.ClientID = "sarama-client-ssl"
	}

	for _, opt := range opts {
		opt(config)
	}

	return config, nil
}
