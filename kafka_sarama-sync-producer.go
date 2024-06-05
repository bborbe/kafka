// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "github.com/IBM/sarama"

//counterfeiter:generate -o mocks/kafka-sarama-sync-producer.go --fake-name KafkaSaramaSyncProducer . SaramaSyncProducer
type SaramaSyncProducer interface {
	sarama.SyncProducer
}
