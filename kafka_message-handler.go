// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

//counterfeiter:generate -o mocks/kafka-message-handler.go --fake-name KafkaMessageHandler . MessageHandler
type MessageHandler interface {
	ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
}
