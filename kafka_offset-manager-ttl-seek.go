// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	libtime "github.com/bborbe/time"
)

// NewTTLSeekOffsetManager creates an offset manager that, when a partition has no committed
// offset, starts at the oldest message younger than ttl instead of at the oldest retained message.
//
// The rationale is replay cost: a topic holding tens of thousands of messages across several
// days costs roughly an hour of replay at the measured rate before the first live message is
// reached, and every message older than the TTL is discarded on arrival anyway.
//
// Only libkv.KeyNotFoundError and libkv.BucketNotFoundError count as a store miss. Treating a
// store outage as a miss would silently turn a broken database into a full replay, so every
// other store error is returned.
//
// InitialOffset and FallbackOffset both report the newest offset. FallbackOffset is read by the
// consumer's four recovery paths — the per-partition start in Consume, skipAndAdvance,
// recoverFromConsumeError, and resetOnOffsetOutOfRange — and resolving any of them to the oldest
// sentinel would restart the replay this manager exists to remove.
func NewTTLSeekOffsetManager(
	saramaClient SaramaClient,
	offsetStore OffsetStore,
	ttl libtime.Duration,
	currentDateTime libtime.CurrentDateTimeGetter,
) OffsetManager {
	return &ttlSeekOffsetManager{
		saramaClient:    saramaClient,
		offsetStore:     offsetStore,
		ttl:             ttl,
		currentDateTime: currentDateTime,
		offsetManager:   NewStoreOffsetManager(offsetStore, OffsetNewest, OffsetNewest),
	}
}

// ttlSeekOffsetManager resolves a missing offset by asking the broker for the first offset at or
// after now minus a caller-supplied TTL, persisting that resolution, and returning it.
type ttlSeekOffsetManager struct {
	saramaClient    SaramaClient
	offsetStore     OffsetStore
	ttl             libtime.Duration
	currentDateTime libtime.CurrentDateTimeGetter
	offsetManager   OffsetManager

	mux    sync.Mutex
	closed bool
}

// InitialOffset returns the newest offset. The true initial offset is time-dependent and cannot
// be expressed as a constant; no consumer reads this method.
func (s *ttlSeekOffsetManager) InitialOffset() Offset {
	return OffsetNewest
}

// FallbackOffset returns the newest offset. The consumer reads this in its recovery paths, so
// resolving them to the oldest sentinel would reinstate the full replay this manager removes.
func (s *ttlSeekOffsetManager) FallbackOffset() Offset {
	return OffsetNewest
}

// NextOffset retrieves the next offset to consume for the given topic and partition, seeking to
// the first offset at or after now minus the TTL when the store holds no offset for it.
func (s *ttlSeekOffsetManager) NextOffset(
	ctx context.Context,
	topic Topic,
	partition Partition,
) (Offset, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.closed {
		return 0, errors.Wrapf(ctx, ErrClosed, "offsetManager is closed")
	}

	offset, err := s.offsetStore.Get(ctx, topic, partition)
	if err == nil {
		return offset, nil
	}
	if !errors.Is(err, libkv.KeyNotFoundError) && !errors.Is(err, libkv.BucketNotFoundError) {
		return 0, errors.Wrapf(ctx, err, "get offset failed")
	}

	seekTimestamp := s.currentDateTime.Now().Add(-s.ttl).Time().UnixMilli()
	brokerOffset, err := s.saramaClient.GetOffset(
		topic.String(),
		partition.Int32(),
		seekTimestamp,
	)
	if err != nil {
		return 0, errors.Wrapf(
			ctx,
			err,
			"get offset for topic(%s) partition(%d) at timestamp(%d) failed",
			topic,
			partition,
			seekTimestamp,
		)
	}
	if brokerOffset < 0 {
		return OffsetNewest, nil
	}
	if err := s.offsetStore.Set(ctx, topic, partition, Offset(brokerOffset)); err != nil {
		return 0, errors.Wrapf(ctx, err, "set offset failed")
	}
	return Offset(brokerOffset), nil
}

// MarkOffset persists the given offset as consumed for the specified topic and partition.
func (s *ttlSeekOffsetManager) MarkOffset(
	ctx context.Context,
	topic Topic,
	partition Partition,
	nextOffset Offset,
) error {
	return s.offsetManager.MarkOffset(ctx, topic, partition, nextOffset)
}

// ResetOffset resets to the provided offset, allowing backward movement.
func (s *ttlSeekOffsetManager) ResetOffset(
	ctx context.Context,
	topic Topic,
	partition Partition,
	nextOffset Offset,
) error {
	return s.offsetManager.ResetOffset(ctx, topic, partition, nextOffset)
}

// Close marks the offset manager as closed.
func (s *ttlSeekOffsetManager) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	return s.offsetManager.Close()
}
