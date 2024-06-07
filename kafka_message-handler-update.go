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

func NewMessageHandlerUpdate[KEY ~[]byte | ~string, OBJECT any](updateHandler UpdaterHandler[KEY, OBJECT]) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var objectID = KEY(msg.Key)
		if len(msg.Value) == 0 {
			if err := updateHandler.Delete(ctx, objectID); err != nil {
				return errors.Wrapf(ctx, err, "delete %s failed", objectID)
			}
			return nil
		}
		var object OBJECT
		if err := json.Unmarshal(msg.Value, &object); err != nil {
			return errors.Wrapf(ctx, err, "unmarshal value of %s failed", objectID)
		}
		if err := updateHandler.Update(ctx, objectID, object); err != nil {
			return errors.Wrapf(ctx, err, "update %s failed", objectID)
		}
		return nil
	})
}
