// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package badgerkv

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	"github.com/dgraph-io/badger/v4"
)

type Bucket interface {
	libkv.Bucket
	Tx() *badger.Txn
	BucketName() libkv.BucketName
}

func NewBucket(
	badgerTx *badger.Txn,
	bucketName libkv.BucketName,
) Bucket {
	return &bucket{
		bucketName: bucketName,
		badgerTx:   badgerTx,
	}
}

type bucket struct {
	badgerTx   *badger.Txn
	bucketName libkv.BucketName
}

func (b *bucket) Tx() *badger.Txn {
	return b.badgerTx
}

func (b *bucket) BucketName() libkv.BucketName {
	return b.bucketName
}

func (b *bucket) Iterator() libkv.Iterator {
	return NewIterator(b.badgerTx, b.bucketName)
}

func (b *bucket) IteratorReverse() libkv.Iterator {
	return NewIteratorReverse(b.badgerTx, b.bucketName)
}

func (b *bucket) Get(ctx context.Context, key []byte) (libkv.Item, error) {
	item, err := b.badgerTx.Get(BucketAddKey(b.bucketName, key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return libkv.NewByteItem(key, nil), nil
		}
		return nil, errors.Wrapf(ctx, err, "get failed")
	}
	return NewItem(b.bucketName, item), nil
}

func (b *bucket) Put(ctx context.Context, key []byte, value []byte) error {
	return b.badgerTx.Set(BucketAddKey(b.bucketName, key), value)
}

func (b *bucket) Delete(ctx context.Context, key []byte) error {
	return b.badgerTx.Delete(BucketAddKey(b.bucketName, key))
}
