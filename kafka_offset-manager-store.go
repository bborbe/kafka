// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	stderrors "errors"
	"sync"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

// ErrClosed is returned when operations are attempted on a closed offset manager.
var ErrClosed = stderrors.New("closed")

// ClosedError is deprecated: Use ErrClosed instead.
//
//nolint:errname // Deprecated alias for backward compatibility
var ClosedError = ErrClosed

// NewStoreOffsetManager creates a new offset manager that persists offsets using a store.
func NewStoreOffsetManager(
	offsetStore OffsetStore,
	initalOffset Offset,
	fallbackOffset Offset,
) OffsetManager {
	return &storeOffsetManager{
		initalOffset:   initalOffset,
		fallbackOffset: fallbackOffset,
		offsetStore:    offsetStore,
	}
}

// storeOffsetManager implements OffsetManager using a persistent store.
type storeOffsetManager struct {
	initalOffset   Offset
	fallbackOffset Offset
	offsetStore    OffsetStore

	mux    sync.Mutex
	closed bool
}

// Close marks the offset manager as closed.
func (s *storeOffsetManager) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	return nil
}

// InitialOffset returns the initial offset to use for new partitions.
func (s *storeOffsetManager) InitialOffset() Offset {
	return s.initalOffset
}

// FallbackOffset returns the fallback offset to use when initial offset fails.
func (s *storeOffsetManager) FallbackOffset() Offset {
	return s.fallbackOffset
}

// NextOffset retrieves the next offset to consume for the given topic and partition from the store.
func (s *storeOffsetManager) NextOffset(
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
	if err != nil {
		if errors.Is(err, libkv.KeyNotFoundError) || errors.Is(err, libkv.BucketNotFoundError) {
			return s.initalOffset, nil
		}
		return 0, errors.Wrapf(ctx, err, "get offest failed")
	}
	return offset, nil
}

// MarkOffset persists the given offset as consumed for the specified topic and partition.
func (s *storeOffsetManager) MarkOffset(
	ctx context.Context,
	topic Topic,
	partition Partition,
	nextOffset Offset,
) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.closed {
		return errors.Wrapf(ctx, ErrClosed, "offsetManager is closed")
	}
	if err := s.offsetStore.Set(ctx, topic, partition, nextOffset); err != nil {
		return errors.Wrapf(ctx, err, "set offset failed")
	}
	return nil
}
