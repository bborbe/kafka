// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

// ValueModifierFunc transforms a raw message-value byte slice into a new byte
// slice. Used by NewSyncProducerEncryptValue (and decrypt-message-handler
// wrappers on the consumer side) to plug a caller-supplied encrypt/decrypt
// function into the producer/consumer pipeline without forcing a particular
// crypto package dependency on bborbe/kafka.
//
// Pass `crypter.Encrypt` directly when using github.com/bborbe/crypto:
//
//	sp = libkafka.NewSyncProducerEncryptValue(sp, crypter.Encrypt)
type ValueModifierFunc func(ctx context.Context, value []byte) ([]byte, error)

// NewSyncProducerEncryptValue wraps a SyncProducer and applies the given
// ValueModifierFunc (typically a crypter's Encrypt method) to every non-empty
// message value before delegating to the underlying producer. Nil / empty
// values pass through unmodified.
//
// bborbe/kafka intentionally does not depend on a specific crypto package —
// callers pass a function. For the canonical AES-GCM implementation, use
// github.com/bborbe/crypto and pass crypter.Encrypt as the modifier.
func NewSyncProducerEncryptValue(
	syncProducer SyncProducer,
	encrypt ValueModifierFunc,
) SyncProducer {
	return &syncProducerEncryptValue{
		syncProducer: syncProducer,
		encrypt:      encrypt,
	}
}

type syncProducerEncryptValue struct {
	syncProducer SyncProducer
	encrypt      ValueModifierFunc
}

// SendMessage encrypts the message value (when non-empty) and delegates to the
// underlying SyncProducer.
func (s *syncProducerEncryptValue) SendMessage(
	ctx context.Context,
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	if err := s.encryptValue(ctx, msg); err != nil {
		return -1, -1, errors.Wrapf(ctx, err, "encrypt value failed")
	}
	return s.syncProducer.SendMessage(ctx, msg)
}

// SendMessages encrypts each message value (when non-empty) and delegates to
// the underlying SyncProducer.
func (s *syncProducerEncryptValue) SendMessages(
	ctx context.Context,
	msgs []*sarama.ProducerMessage,
) error {
	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := s.encryptValue(ctx, msg); err != nil {
				return errors.Wrapf(ctx, err, "encrypt value failed")
			}
		}
	}
	return s.syncProducer.SendMessages(ctx, msgs)
}

// Close closes the underlying SyncProducer.
func (s *syncProducerEncryptValue) Close() error {
	return s.syncProducer.Close()
}

func (s *syncProducerEncryptValue) encryptValue(
	ctx context.Context,
	msg *sarama.ProducerMessage,
) error {
	if msg.Value == nil || msg.Value.Length() == 0 {
		return nil
	}
	value, err := msg.Value.Encode()
	if err != nil {
		return errors.Wrapf(ctx, err, "encode value failed")
	}
	encrypted, err := s.encrypt(ctx, value)
	if err != nil {
		return errors.Wrapf(ctx, err, "encrypt value failed")
	}
	msg.Value = sarama.ByteEncoder(encrypted)
	return nil
}
