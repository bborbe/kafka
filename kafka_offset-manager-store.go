// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

func NewStoreOffsetManager(
	initalOffset Offset,
	offsetStore OffsetStore,
) OffsetManager {
	return &storeOffsetManager{
		initalOffset: initalOffset,
		offsetStore:  offsetStore,
	}
}

type storeOffsetManager struct {
	initalOffset Offset
	offsetStore  OffsetStore
}

func (s *storeOffsetManager) InitialOffset() Offset {
	return s.initalOffset
}

func (s *storeOffsetManager) NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error) {
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
	if err := s.offsetStore.Set(ctx, topic, partition, nextOffset); err != nil {
		return errors.Wrapf(ctx, err, "set offset failed")
	}
	return nil
}
