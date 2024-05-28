// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package badgerkv

import (
	libkv "github.com/bborbe/kv"
	"github.com/dgraph-io/badger/v4"
)

func NewIteratorReverse(
	badgerTx *badger.Txn,
	bucketName libkv.BucketName,
) Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	opts.Reverse = true
	return &iteratorReverse{
		badgerIterator: badgerTx.NewIterator(opts),
		bucketName:     bucketName,
	}
}

type iteratorReverse struct {
	badgerIterator *badger.Iterator
	bucketName     libkv.BucketName
}

func (i iteratorReverse) BucketName() libkv.BucketName {
	return i.bucketName
}

func (i iteratorReverse) Iterator() *badger.Iterator {
	return i.badgerIterator
}

func (i iteratorReverse) Close() {
	i.badgerIterator.Close()
}

func (i iteratorReverse) Item() libkv.Item {
	return NewItem(
		i.bucketName,
		i.badgerIterator.Item(),
	)
}

func (i iteratorReverse) Next() {
	i.badgerIterator.Next()
}

func (i iteratorReverse) Valid() bool {
	return i.badgerIterator.ValidForPrefix(i.bucketName)
}

func (i iteratorReverse) Rewind() {
	i.badgerIterator.Seek(append(i.bucketName, bucketKeySeperatorPlusone))
}

func (i iteratorReverse) Seek(key []byte) {
	i.badgerIterator.Seek(BucketAddKey(i.bucketName, key))
}
