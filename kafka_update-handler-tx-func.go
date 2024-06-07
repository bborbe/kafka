// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

func UpdaterHandlerTxFunc[OBJECT any, KEY ~[]byte | ~string](
	update func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error,
	delete func(ctx context.Context, tx libkv.Tx, key KEY) error,
) UpdaterHandlerTx[OBJECT, KEY] {
	return &updaterHandlerTxFunc[OBJECT, KEY]{
		update: update,
		delete: delete,
	}
}

type updaterHandlerTxFunc[OBJECT any, KEY ~[]byte | ~string] struct {
	update func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error
	delete func(ctx context.Context, tx libkv.Tx, OBJECT KEY) error
}

func (e *updaterHandlerTxFunc[OBJECT, KEY]) Update(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error {
	if e.update == nil {
		return nil
	}
	if err := e.update(ctx, tx, key, object); err != nil {
		return errors.Wrapf(ctx, err, "update failed")
	}
	return nil
}

func (e *updaterHandlerTxFunc[OBJECT, KEY]) Delete(ctx context.Context, tx libkv.Tx, key KEY) error {
	if e.delete == nil {
		return nil
	}
	if err := e.delete(ctx, tx, key); err != nil {
		return errors.Wrapf(ctx, err, "delete failed")
	}
	return nil
}
