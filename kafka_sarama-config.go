// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

type SaramaConfigOptions func(config *sarama.Config)

func CreateSaramaConfig(
	opts ...SaramaConfigOptions,
) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.Retention = 14 * 24 * time.Hour // 14 days
	config.Consumer.Return.Errors = true
	config.Metadata.Retry.Max = 10
	config.Admin.Retry.Max = 10
	config.Admin.Retry.Backoff = time.Second

	for _, opt := range opts {
		opt(config)
	}

	return config
}
