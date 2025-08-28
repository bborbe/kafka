// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "github.com/IBM/sarama"

//counterfeiter:generate -o mocks/kafka-sarama-partition-consumer.go --fake-name KafkaSaramaPartitionConsumer . SaramaPartitionConsumer

// SaramaPartitionConsumer represents a wrapper interface for sarama.PartitionConsumer.
type SaramaPartitionConsumer interface {
	sarama.PartitionConsumer
}
