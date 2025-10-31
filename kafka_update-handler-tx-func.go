// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

// UpdaterHandlerTxFunc creates an UpdaterHandlerTx from separate updateFn and delete functions.
func UpdaterHandlerTxFunc[KEY ~[]byte | ~string, OBJECT any](
	updateFn func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error,
	deleteFn func(ctx context.Context, tx libkv.Tx, key KEY) error,
) UpdaterHandlerTx[KEY, OBJECT] {
	return &updaterHandlerTxFunc[KEY, OBJECT]{
		updateFn: updateFn,
		deleteFn: deleteFn,
	}
}

// updaterHandlerTxFunc implements UpdaterHandlerTx using function pointers.
type updaterHandlerTxFunc[KEY ~[]byte | ~string, OBJECT any] struct {
	updateFn func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error
	deleteFn func(ctx context.Context, tx libkv.Tx, OBJECT KEY) error
}

// Update executes the updateFn function within a transaction if provided.
func (e *updaterHandlerTxFunc[KEY, OBJECT]) Update(
	ctx context.Context,
	tx libkv.Tx,
	key KEY,
	object OBJECT,
) error {
	if e.updateFn == nil {
		return nil
	}
	if err := e.updateFn(ctx, tx, key, object); err != nil {
		return errors.Wrapf(ctx, err, "updateFn failed")
	}
	return nil
}

// Delete executes the delete function within a transaction if provided.
func (e *updaterHandlerTxFunc[KEY, OBJECT]) Delete(
	ctx context.Context,
	tx libkv.Tx,
	key KEY,
) error {
	if e.deleteFn == nil {
		return nil
	}
	if err := e.deleteFn(ctx, tx, key); err != nil {
		return errors.Wrapf(ctx, err, "delete failed")
	}
	return nil
}
