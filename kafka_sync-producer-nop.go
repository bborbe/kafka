// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/golang/glog"
)

func NewSyncProducerNop() SyncProducer {
	return &syncProducerNo{}
}

type syncProducerNo struct {
}

func (s *syncProducerNo) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	glog.V(3).Infof("would send message")
	return -1, -1, nil
}

func (s *syncProducerNo) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error {
	glog.V(3).Infof("would send %d messages", len(msgs))
	return nil
}

func (s *syncProducerNo) Close() error {
	return nil
}
