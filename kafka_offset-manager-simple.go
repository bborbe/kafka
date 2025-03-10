// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"

	"github.com/bborbe/errors"
)

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

type simpleOffsetManager struct {
	initalOffset   Offset
	fallbackOffset Offset

	mux     sync.Mutex
	closed  bool
	offsets map[TopicPartition]Offset
}

func (s *simpleOffsetManager) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	return nil
}

func (s *simpleOffsetManager) InitialOffset() Offset {
	return s.initalOffset
}

func (s *simpleOffsetManager) FallbackOffset() Offset {
	return s.fallbackOffset
}

func (s *simpleOffsetManager) NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error) {
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

func (s *simpleOffsetManager) MarkOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error {
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
