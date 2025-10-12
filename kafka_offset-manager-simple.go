// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"

	"github.com/bborbe/errors"
)

// NewSimpleOffsetManager creates a new simple in-memory offset manager.
func NewSimpleOffsetManager(
	initalOffset Offset,
	fallbackOffset Offset,
) OffsetManager {
	return &simpleOffsetManager{
		initalOffset:   initalOffset,
		fallbackOffset: fallbackOffset,
		offsets:        make(map[TopicPartition]Offset),
	}
}

// simpleOffsetManager implements OffsetManager using in-memory storage.
type simpleOffsetManager struct {
	initalOffset   Offset
	fallbackOffset Offset

	mux     sync.Mutex
	closed  bool
	offsets map[TopicPartition]Offset
}

// Close marks the offset manager as closed.
func (s *simpleOffsetManager) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	return nil
}

// InitialOffset returns the initial offset to use for new partitions.
func (s *simpleOffsetManager) InitialOffset() Offset {
	return s.initalOffset
}

// FallbackOffset returns the fallback offset to use when initial offset fails.
func (s *simpleOffsetManager) FallbackOffset() Offset {
	return s.fallbackOffset
}

// NextOffset retrieves the next offset to consume for the given topic and partition.
func (s *simpleOffsetManager) NextOffset(
	ctx context.Context,
	topic Topic,
	partition Partition,
) (Offset, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	result, ok := s.offsets[TopicPartition{
		Topic:     topic,
		Partition: partition,
	}]
	if !ok {
		return s.initalOffset, nil
	}
	return result, nil
}

// MarkOffset marks the given offset as consumed for the specified topic and partition.
func (s *simpleOffsetManager) MarkOffset(
	ctx context.Context,
	topic Topic,
	partition Partition,
	nextOffset Offset,
) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.closed {
		return errors.Wrapf(ctx, ClosedError, "offsetManager is closed")
	}
	s.offsets[TopicPartition{
		Topic:     topic,
		Partition: partition,
	}] = nextOffset
	return nil
}
