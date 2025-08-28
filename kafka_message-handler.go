// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

//counterfeiter:generate -o mocks/kafka-message-handler.go --fake-name KafkaMessageHandler . MessageHandler

// MessageHandler defines the interface for processing individual Kafka messages.
type MessageHandler interface {
	// ConsumeMessage processes a single Kafka message and returns an error if processing fails.
	ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
}
