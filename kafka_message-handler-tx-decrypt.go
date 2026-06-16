// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

// NewDecryptMessageHandlerTx returns a MessageHandlerTx that decrypts
// msg.Value with the given ValueModifierFunc before delegating to the inner
// handler. Empty values pass through unmodified.
//
// bborbe/kafka does not depend on a specific crypto package — callers pass a
// function. For the canonical AES-GCM implementation, use
// github.com/bborbe/crypto and pass crypter.Decrypt as the modifier:
//
//	h := libkafka.NewDecryptMessageHandlerTx(inner, crypter.Decrypt)
func NewDecryptMessageHandlerTx(
	inner MessageHandlerTx,
	decrypt ValueModifierFunc,
) MessageHandlerTx {
	return MessageHandlerTxFunc(
		func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
			if len(msg.Value) > 0 {
				decrypted, err := decrypt(ctx, msg.Value)
				if err != nil {
					return errors.Wrapf(ctx, err, "decrypt msg failed")
				}
				msg.Value = decrypted
			}
			return inner.ConsumeMessage(ctx, tx, msg)
		},
	)
}
