// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/bborbe/run"
	"github.com/golang/glog"
)

// NewOffsetTriggerMessageHandler returns message handler that call the given trigger if all offset are reached
func NewOffsetTriggerMessageHandler(
	triggerOffsets PartitionOffsets,
	topic Topic,
	trigger run.Fire,
) MessageHandler {
	var mux sync.Mutex
	clonedTriggerOffset := triggerOffsets.Clone()

	var wg sync.WaitGroup
	offsetCount := len(clonedTriggerOffset)
	wg.Add(offsetCount)
	go func() {
		wg.Wait()
		trigger.Fire()
		glog.V(2).Infof("reached %d highwaterMarks for %s => trigger fired", offsetCount, topic)
	}()

	return MessageHandlerFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		mux.Lock()
		triggerOffset, ok := clonedTriggerOffset[Partition(msg.Partition)]
		mux.Unlock()
		if ok {
			if triggerOffset <= Offset(msg.Offset) {
				glog.V(2).Infof("partiton %d reached highwater mark offset of %d for %s", msg.Partition, msg.Offset, msg.Topic)
				mux.Lock()
				delete(clonedTriggerOffset, Partition(msg.Partition))
				mux.Unlock()
				wg.Done()
			}
		}
		return nil
	})
}
