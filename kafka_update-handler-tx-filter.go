// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

type FilterTx[KEY ~[]byte | ~string, OBJECT any] interface {
	// Filtered return true if should be filter out
	Filtered(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) (bool, error)
}

type FilterTxFunc[KEY ~[]byte | ~string, OBJECT any] func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) (bool, error)

func (f FilterTxFunc[KEY, OBJECT]) Filtered(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) (bool, error) {
	return f(ctx, tx, key, object)
}

func NewUpdaterHandlerTxilter[KEY ~[]byte | ~string, OBJECT any](
	filterTx Filter[KEY, OBJECT],
	updateHandlerTx UpdaterHandlerTx[KEY, OBJECT],
) UpdaterHandlerTx[KEY, OBJECT] {
	return UpdaterHandlerTxFunc[KEY, OBJECT](
		func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error {
			filtered, err := filterTx.Filtered(ctx, key, object)
			if err != nil {
				return errors.Wrapf(ctx, err, "filtered failed")
			}
			if filtered {
				return updateHandlerTx.Delete(ctx, tx, key)
			}
			return updateHandlerTx.Update(ctx, tx, key, object)
		},
		func(ctx context.Context, tx libkv.Tx, key KEY) error {
			return updateHandlerTx.Delete(ctx, tx, key)
		},
	)
}
