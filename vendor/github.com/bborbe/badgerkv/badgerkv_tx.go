// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package badgerkv

import (
	"bytes"
	"context"
	"sync"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	"github.com/dgraph-io/badger/v4"
	"github.com/golang/glog"
)

type Tx interface {
	libkv.Tx
	Tx() *badger.Txn
}

func NewTx(badgerTx *badger.Txn) Tx {
	return &tx{
		badgerTx:   badgerTx,
		bucketName: libkv.NewBucketName("__bucket"),

		cache: make(map[string]libkv.Bucket),
	}
}

type tx struct {
	badgerTx   *badger.Txn
	bucketName libkv.BucketName

	mux   sync.Mutex
	cache map[string]libkv.Bucket
}

func (t *tx) Tx() *badger.Txn {
	return t.badgerTx
}

func (t *tx) Bucket(ctx context.Context, name libkv.BucketName) (libkv.Bucket, error) {
	t.mux.Lock()
	defer t.mux.Unlock()

	bucket, ok := t.cache[name.String()]
	if ok {
		return bucket, nil
	}

	exists, err := t.existsBucket(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "check exists failed")
	}
	if exists == false {
		return nil, errors.Wrapf(ctx, libkv.BucketNotFoundError, "bucket %s not found", name)
	}
	bucket = NewBucket(t.badgerTx, name)
	t.cache[name.String()] = bucket
	return bucket, nil
}

func (t *tx) CreateBucket(ctx context.Context, name libkv.BucketName) (libkv.Bucket, error) {
	t.mux.Lock()
	defer t.mux.Unlock()

	exists, err := t.existsBucket(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "check exists failed")
	}
	if exists {
		return nil, errors.Wrapf(ctx, libkv.BucketAlreadyExistsError, "bucket %s already exists", name)
	}
	if err := t.createBucket(ctx, name); err != nil {
		return nil, errors.Wrapf(ctx, err, "create bucket failed")
	}
	bucket := NewBucket(t.badgerTx, name)
	t.cache[name.String()] = bucket
	return bucket, nil
}

func (t *tx) CreateBucketIfNotExists(ctx context.Context, name libkv.BucketName) (libkv.Bucket, error) {
	t.mux.Lock()
	defer t.mux.Unlock()

	bucket, ok := t.cache[name.String()]
	if ok {
		return bucket, nil
	}

	exists, err := t.existsBucket(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "check exists failed")
	}
	if exists == false {
		if err := t.createBucket(ctx, name); err != nil {
			return nil, errors.Wrapf(ctx, err, "create bucket failed")
		}
	}
	bucket = NewBucket(t.badgerTx, name)
	t.cache[name.String()] = bucket
	return bucket, nil
}

func (t *tx) DeleteBucket(ctx context.Context, name libkv.BucketName) error {
	t.mux.Lock()
	defer t.mux.Unlock()

	exists, err := t.existsBucket(ctx, name)
	if err != nil {
		return errors.Wrapf(ctx, err, "check exists failed")
	}
	if exists == false {
		return errors.Wrapf(ctx, libkv.BucketNotFoundError, "bucket %s not found", name)
	}
	if err := t.deleteBucket(ctx, name); err != nil {
		return errors.Wrapf(ctx, err, "delete bucket failed")
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := t.badgerTx.NewIterator(opts)
	defer it.Close()
	for it.Seek(name.Bytes()); it.Valid(); it.Next() {
		key := it.Item().Key()
		if bytes.HasPrefix(key, name.Bytes()) == false {
			glog.V(3).Infof("delete all key of bucket %s completed", name)
			break
		}
		if err := t.badgerTx.Delete(key); err != nil {
			return errors.Wrapf(ctx, err, "delete bucket failed")
		}
	}

	delete(t.cache, name.String())
	return nil
}

func (t *tx) existsBucket(ctx context.Context, name libkv.BucketName) (bool, error) {
	bucket := NewBucket(t.badgerTx, t.bucketName)
	var exists bool
	value, err := bucket.Get(ctx, name.Bytes())
	if err != nil {
		return false, errors.Wrapf(ctx, err, "get failed")
	}
	err = value.Value(func(val []byte) error {
		exists = bytes.Compare(val, []byte("true")) == 0
		return nil
	})
	if err != nil {
		return false, errors.Wrapf(ctx, err, "value failed")
	}
	return exists, nil
}

func (t *tx) createBucket(ctx context.Context, name libkv.BucketName) error {
	bucket := NewBucket(t.badgerTx, t.bucketName)
	if err := bucket.Put(ctx, name.Bytes(), []byte("true")); err != nil {
		return errors.Wrapf(ctx, err, "put failed")
	}
	return nil
}

func (t *tx) deleteBucket(ctx context.Context, name libkv.BucketName) error {
	bucket := NewBucket(t.badgerTx, t.bucketName)
	return bucket.Delete(ctx, name.Bytes())
}
