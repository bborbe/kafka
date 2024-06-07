// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
)

type UpdaterHandlerList[OBJECT any, KEY ~[]byte | ~string] []UpdaterHandler[OBJECT, KEY]

func (e UpdaterHandlerList[OBJECT, KEY]) Update(ctx context.Context, key KEY, object OBJECT) error {
	for _, ee := range e {
		if err := ee.Update(ctx, key, object); err != nil {
			return errors.Wrapf(ctx, err, "update failed")
		}
	}
	return nil
}

func (e UpdaterHandlerList[OBJECT, KEY]) Delete(ctx context.Context, key KEY) error {
	for _, ee := range e {
		if err := ee.Delete(ctx, key); err != nil {
			return errors.Wrapf(ctx, err, "update failed")
		}
	}
	return nil
}
