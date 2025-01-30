// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "github.com/IBM/sarama"

//counterfeiter:generate -o mocks/kafka-sarama-consumer.go --fake-name KafkaSaramaConsumer . SaramaConsumer
type SaramaConsumer interface {
	sarama.Consumer
}
