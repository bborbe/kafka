// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

type MessageHanderList []MessageHandler

func (m MessageHanderList) ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	for _, mm := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := mm.ConsumeMessage(ctx, msg); err != nil {
				return errors.Wrapf(ctx, err, "consume message failed")
			}
		}
	}
	return nil
}
