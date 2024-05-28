// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package badgerkv

import (
	libkv "github.com/bborbe/kv"
	"github.com/dgraph-io/badger/v4"
)

type Item interface {
	libkv.Item
	BucketName() libkv.BucketName
	Item() *badger.Item
}

func NewItem(
	bucketName libkv.BucketName,
	badgerItem *badger.Item,
) Item {
	return &item{
		badgerItem: badgerItem,
		bucketName: bucketName,
	}
}

type item struct {
	badgerItem *badger.Item
	bucketName libkv.BucketName
}

func (i *item) BucketName() libkv.BucketName {
	return i.bucketName
}

func (i *item) Item() *badger.Item {
	return i.badgerItem
}

func (i *item) Exists() bool {
	return true
}

func (i *item) Key() []byte {
	return BucketRemoveKey(i.bucketName, i.badgerItem.Key())
}

func (i *item) Value(fn func(val []byte) error) error {
	return i.badgerItem.Value(fn)
}
