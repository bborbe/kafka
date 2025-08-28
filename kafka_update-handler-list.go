// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
)

// UpdaterHandlerList is a list of UpdaterHandler that executes handlers sequentially.
type UpdaterHandlerList[KEY ~[]byte | ~string, OBJECT any] []UpdaterHandler[KEY, OBJECT]

// Update executes all handlers in the list sequentially for update operations.
func (e UpdaterHandlerList[KEY, OBJECT]) Update(ctx context.Context, key KEY, object OBJECT) error {
	for _, ee := range e {
		if err := ee.Update(ctx, key, object); err != nil {
			return errors.Wrapf(ctx, err, "update failed")
		}
	}
	return nil
}

// Delete executes all handlers in the list sequentially for delete operations.
func (e UpdaterHandlerList[KEY, OBJECT]) Delete(ctx context.Context, key KEY) error {
	for _, ee := range e {
		if err := ee.Delete(ctx, key); err != nil {
			return errors.Wrapf(ctx, err, "update failed")
		}
	}
	return nil
}
