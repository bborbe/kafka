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

var ClosedError = stderrors.New("closed")

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

type storeOffsetManager struct {
	initalOffset   Offset
	fallbackOffset Offset
	offsetStore    OffsetStore

	mux    sync.Mutex
	closed bool
}

func (s *storeOffsetManager) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	return nil
}

func (s *storeOffsetManager) InitialOffset() Offset {
	return s.initalOffset
}

func (s *storeOffsetManager) FallbackOffset() Offset {
	return s.fallbackOffset
}

func (s *storeOffsetManager) NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.closed {
		return 0, errors.Wrapf(ctx, ClosedError, "offsetManager is closed")
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

func (s *storeOffsetManager) MarkOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.closed {
		return errors.Wrapf(ctx, ClosedError, "offsetManager is closed")
	}
	if err := s.offsetStore.Set(ctx, topic, partition, nextOffset); err != nil {
		return errors.Wrapf(ctx, err, "set offset failed")
	}
	return nil
}
