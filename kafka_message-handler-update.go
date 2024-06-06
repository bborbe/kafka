// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

func NewMessageHandlerUpdate[OBJECT any](updateHandler UpdaterHandler[OBJECT, Key]) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		key := NewKey(msg.Key)
		if len(msg.Value) == 0 {
			if err := updateHandler.Delete(ctx, key); err != nil {
				return errors.Wrapf(ctx, err, "delete %s failed", key)
			}
			return nil
		}
		var object OBJECT
		if err := json.Unmarshal(msg.Value, &object); err != nil {
			return errors.Wrapf(ctx, err, "unmarshal value of %s failed", key)
		}
		if err := updateHandler.Update(ctx, key, object); err != nil {
			return errors.Wrapf(ctx, err, "update %s failed", key)
		}
		return nil
	})
}
