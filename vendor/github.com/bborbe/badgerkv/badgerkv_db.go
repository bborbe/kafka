// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package badgerkv

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	"github.com/dgraph-io/badger/v4"
	"github.com/golang/glog"
)

type contextKey string

const stateCtxKey contextKey = "state"

type DB interface {
	libkv.DB
	DB() *badger.DB
}

type ChangeOptions func(opts *badger.Options)

func MinMemoryUsageOptions(opts *badger.Options) {
	opts.MemTableSize = 16 << 20
	opts.NumMemtables = 3
	opts.NumLevelZeroTables = 3
	opts.NumLevelZeroTablesStall = 8
}

func OpenPath(ctx context.Context, path string, fn ...ChangeOptions) (libkv.DB, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	for _, f := range fn {
		f(&opts)
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "open badger db failed")
	}
	return NewDB(db), nil
}

func OpenMemory(ctx context.Context, fn ...ChangeOptions) (libkv.DB, error) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	for _, f := range fn {
		f(&opts)
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "open badger db failed")
	}
	return NewDB(db), nil
}

func NewDB(db *badger.DB) DB {
	return &badgerdb{
		db: db,
	}
}

type badgerdb struct {
	db *badger.DB
}

func (b *badgerdb) Sync() error {
	return b.db.Sync()
}

func (b *badgerdb) DB() *badger.DB {
	return b.db
}

func (b *badgerdb) Close() error {
	return b.db.Close()
}

func (b *badgerdb) Update(ctx context.Context, fn func(ctx context.Context, tx libkv.Tx) error) error {
	glog.V(4).Infof("db update started")
	if IsTransactionOpen(ctx) {
		return errors.Wrapf(ctx, libkv.TransactionAlreadyOpenError, "transaction already open")
	}
	err := b.db.Update(func(tx *badger.Txn) error {
		glog.V(4).Infof("db update started")
		ctx = SetOpenState(ctx)
		if err := fn(ctx, NewTx(tx)); err != nil {
			return errors.Wrapf(ctx, err, "db update failed")
		}
		glog.V(4).Infof("db update completed")
		return nil
	})
	if err != nil {
		return errors.Wrapf(ctx, err, "db update failed")
	}
	glog.V(4).Infof("db update completed")
	return nil
}

func (b *badgerdb) View(ctx context.Context, fn func(ctx context.Context, tx libkv.Tx) error) error {
	glog.V(4).Infof("db view started")
	if IsTransactionOpen(ctx) {
		return errors.Wrapf(ctx, libkv.TransactionAlreadyOpenError, "transaction already open")
	}
	err := b.db.View(func(tx *badger.Txn) error {
		glog.V(4).Infof("db view started")
		ctx = SetOpenState(ctx)
		if err := fn(ctx, NewTx(tx)); err != nil {
			return errors.Wrapf(ctx, err, "db view failed")
		}
		glog.V(4).Infof("db view completed")
		return nil
	})
	if err != nil {
		return errors.Wrapf(ctx, err, "db view failed")
	}
	glog.V(4).Infof("db view completed")
	return nil
}

func IsTransactionOpen(ctx context.Context) bool {
	return ctx.Value(stateCtxKey) != nil
}

func SetOpenState(ctx context.Context) context.Context {
	return context.WithValue(ctx, stateCtxKey, "open")
}
