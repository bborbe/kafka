// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

// UpdaterHandlerTxFunc creates an UpdaterHandlerTx from separate update and delete functions.
func UpdaterHandlerTxFunc[KEY ~[]byte | ~string, OBJECT any](
	update func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error,
	delete func(ctx context.Context, tx libkv.Tx, key KEY) error,
) UpdaterHandlerTx[KEY, OBJECT] {
	return &updaterHandlerTxFunc[KEY, OBJECT]{
		update: update,
		delete: delete,
	}
}

// updaterHandlerTxFunc implements UpdaterHandlerTx using function pointers.
type updaterHandlerTxFunc[KEY ~[]byte | ~string, OBJECT any] struct {
	update func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error
	delete func(ctx context.Context, tx libkv.Tx, OBJECT KEY) error
}

// Update executes the update function within a transaction if provided.
func (e *updaterHandlerTxFunc[KEY, OBJECT]) Update(
	ctx context.Context,
	tx libkv.Tx,
	key KEY,
	object OBJECT,
) error {
	if e.update == nil {
		return nil
	}
	if err := e.update(ctx, tx, key, object); err != nil {
		return errors.Wrapf(ctx, err, "update failed")
	}
	return nil
}

// Delete executes the delete function within a transaction if provided.
func (e *updaterHandlerTxFunc[KEY, OBJECT]) Delete(
	ctx context.Context,
	tx libkv.Tx,
	key KEY,
) error {
	if e.delete == nil {
		return nil
	}
	if err := e.delete(ctx, tx, key); err != nil {
		return errors.Wrapf(ctx, err, "delete failed")
	}
	return nil
}
