// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

func NewMessageHandlerTxUpdate[KEY ~[]byte | ~string, OBJECT any](updateHandlerTx UpdaterHandlerTx[KEY, OBJECT]) MessageHandlerTx {
	return MessageHandlerTxFunc(func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
		var objectID = KEY(msg.Key)
		if len(msg.Value) == 0 {
			if err := updateHandlerTx.Delete(ctx, tx, objectID); err != nil {
				return errors.Wrapf(ctx, err, "delete %s failed", objectID)
			}
			return nil
		}
		var object OBJECT
		if err := json.Unmarshal(msg.Value, &object); err != nil {
			return errors.Wrapf(ctx, err, "unmarshal value of %s failed", objectID)
		}
		if err := updateHandlerTx.Update(ctx, tx, objectID, object); err != nil {
			return errors.Wrapf(ctx, err, "update %s failed", objectID)
		}
		return nil
	})
}
