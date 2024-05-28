// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package badgerkv

import (
	libkv "github.com/bborbe/kv"
	"github.com/dgraph-io/badger/v4"
)

type Iterator interface {
	libkv.Iterator
	BucketName() libkv.BucketName
	Iterator() *badger.Iterator
}

func NewIterator(
	badgerTx *badger.Txn,
	bucketName libkv.BucketName,
) Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	opts.Reverse = false
	return &iterator{
		badgerIterator: badgerTx.NewIterator(opts),
		bucketName:     bucketName,
	}
}

type iterator struct {
	badgerIterator *badger.Iterator
	bucketName     libkv.BucketName
}

func (i iterator) BucketName() libkv.BucketName {
	return i.bucketName
}

func (i iterator) Iterator() *badger.Iterator {
	return i.badgerIterator
}

func (i iterator) Close() {
	i.badgerIterator.Close()
}

func (i iterator) Item() libkv.Item {
	return NewItem(
		i.bucketName,
		i.badgerIterator.Item(),
	)
}

func (i iterator) Next() {
	i.badgerIterator.Next()
}

func (i iterator) Valid() bool {
	return i.badgerIterator.ValidForPrefix(i.bucketName)
}

func (i iterator) Rewind() {
	i.badgerIterator.Seek(append(i.bucketName, bucketKeySeperator))
}

func (i iterator) Seek(key []byte) {
	i.badgerIterator.Seek(BucketAddKey(i.bucketName, key))
}
