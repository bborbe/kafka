// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	libkv "github.com/bborbe/kv"
)

type UpdaterHandlerTx[KEY ~[]byte | ~string, OBJECT any] interface {
	Update(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error
	Delete(ctx context.Context, tx libkv.Tx, key KEY) error
}

func NewUpdaterHandlerTxView[KEY ~[]byte | ~string, OBJECT any](db libkv.DB, updaterHandlerTx UpdaterHandlerTx[KEY, OBJECT]) UpdaterHandler[KEY, OBJECT] {
	return UpdaterHandlerFunc[KEY, OBJECT](
		func(ctx context.Context, key KEY, object OBJECT) error {
			return db.View(ctx, func(ctx context.Context, tx libkv.Tx) error {
				return updaterHandlerTx.Update(ctx, tx, key, object)
			})
		},
		func(ctx context.Context, key KEY) error {
			return db.View(ctx, func(ctx context.Context, tx libkv.Tx) error {
				return updaterHandlerTx.Delete(ctx, tx, key)
			})
		},
	)
}

func NewUpdaterHandlerTxUpdate[KEY ~[]byte | ~string, OBJECT any](db libkv.DB, updaterHandlerTx UpdaterHandlerTx[KEY, OBJECT]) UpdaterHandler[KEY, OBJECT] {
	return UpdaterHandlerFunc[KEY, OBJECT](
		func(ctx context.Context, key KEY, object OBJECT) error {
			return db.Update(ctx, func(ctx context.Context, tx libkv.Tx) error {
				return updaterHandlerTx.Update(ctx, tx, key, object)
			})
		},
		func(ctx context.Context, key KEY) error {
			return db.Update(ctx, func(ctx context.Context, tx libkv.Tx) error {
				return updaterHandlerTx.Delete(ctx, tx, key)
			})
		},
	)
}
