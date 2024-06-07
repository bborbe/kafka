// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

type UpdaterHandlerTxList[KEY ~[]byte | ~string, OBJECT any] []UpdaterHandlerTx[KEY, OBJECT]

func (e UpdaterHandlerTxList[KEY, OBJECT]) Update(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error {
	for _, ee := range e {
		if err := ee.Update(ctx, tx, key, object); err != nil {
			return errors.Wrapf(ctx, err, "update failed")
		}
	}
	return nil
}

func (e UpdaterHandlerTxList[KEY, OBJECT]) Delete(ctx context.Context, tx libkv.Tx, key KEY) error {
	for _, ee := range e {
		if err := ee.Delete(ctx, tx, key); err != nil {
			return errors.Wrapf(ctx, err, "update failed")
		}
	}
	return nil
}
