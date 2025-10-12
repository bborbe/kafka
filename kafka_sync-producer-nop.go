// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/golang/glog"
)

// NewSyncProducerNop creates a no-operation sync producer that logs but doesn't actually send messages.
func NewSyncProducerNop() SyncProducer {
	return &syncProducerNo{}
}

// syncProducerNo is a no-operation implementation of SyncProducer for testing and dry-run scenarios.
type syncProducerNo struct {
}

// SendMessage logs that it would send a message but doesn't actually send it.
func (s *syncProducerNo) SendMessage(
	ctx context.Context,
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	glog.V(3).Infof("would send message")
	return -1, -1, nil
}

// SendMessages logs that it would send messages but doesn't actually send them.
func (s *syncProducerNo) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error {
	glog.V(3).Infof("would send %d messages", len(msgs))
	return nil
}

// Close is a no-operation close method.
func (s *syncProducerNo) Close() error {
	return nil
}
