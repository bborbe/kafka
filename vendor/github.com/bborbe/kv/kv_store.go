// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kv

import (
	"context"

	"github.com/bborbe/errors"
)

type StoreMapper[KEY ~[]byte | ~string, OBJECT any] interface {
	Map(ctx context.Context, fn func(ctx context.Context, key KEY, object OBJECT) error) error
}

type StoreAdder[KEY ~[]byte | ~string, OBJECT any] interface {
	Add(ctx context.Context, key KEY, object OBJECT) error
}

type StoreRemover[KEY ~[]byte | ~string] interface {
	Remove(ctx context.Context, key KEY) error
}

type StoreGetter[KEY ~[]byte | ~string, OBJECT any] interface {
	Get(ctx context.Context, key KEY) (*OBJECT, error)
}

type Store[KEY ~[]byte | ~string, OBJECT any] interface {
	StoreAdder[KEY, OBJECT]
	StoreRemover[KEY]
	StoreGetter[KEY, OBJECT]
	StoreMapper[KEY, OBJECT]
}

func NewStore[KEY ~[]byte | ~string, OBJECT any](db DB, bucketName BucketName) Store[KEY, OBJECT] {
	return &store[KEY, OBJECT]{
		db:    db,
		store: NewStoreTx[KEY, OBJECT](bucketName),
	}
}

type store[KEY ~[]byte | ~string, OBJECT any] struct {
	db    DB
	store StoreTx[KEY, OBJECT]
}

func (s store[KEY, OBJECT]) Add(ctx context.Context, key KEY, object OBJECT) error {
	return s.db.Update(ctx, func(ctx context.Context, tx Tx) error {
		return s.store.Add(ctx, tx, key, object)
	})
}

func (s store[KEY, OBJECT]) Remove(ctx context.Context, key KEY) error {
	return s.db.Update(ctx, func(ctx context.Context, tx Tx) error {
		return s.store.Remove(ctx, tx, key)
	})
}

func (s store[KEY, OBJECT]) Get(ctx context.Context, key KEY) (*OBJECT, error) {
	var object *OBJECT
	err := s.db.View(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		object, err = s.store.Get(ctx, tx, key)
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "view failed")
	}
	return object, nil
}

func (s store[KEY, OBJECT]) Map(ctx context.Context, fn func(ctx context.Context, key KEY, object OBJECT) error) error {
	return s.db.View(ctx, func(ctx context.Context, tx Tx) error {
		return s.store.Map(ctx, tx, fn)
	})
}
