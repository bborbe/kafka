// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
)

// UpdaterHandlerFunc creates an UpdaterHandler from separate update and delete functions.
func UpdaterHandlerFunc[KEY ~[]byte | ~string, OBJECT any](
	updateFn func(ctx context.Context, key KEY, object OBJECT) error,
	deleteFn func(ctx context.Context, key KEY) error,
) UpdaterHandler[KEY, OBJECT] {
	return &updaterHandlerFunc[KEY, OBJECT]{
		updateFn: updateFn,
		deleteFn: deleteFn,
	}
}

// updaterHandlerFunc implements UpdaterHandler using function pointers.
type updaterHandlerFunc[KEY ~[]byte | ~string, OBJECT any] struct {
	updateFn func(ctx context.Context, key KEY, object OBJECT) error
	deleteFn func(ctx context.Context, OBJECT KEY) error
}

// Update executes the update function if provided.
func (e *updaterHandlerFunc[KEY, OBJECT]) Update(
	ctx context.Context,
	key KEY,
	object OBJECT,
) error {
	if e.updateFn == nil {
		return nil
	}
	if err := e.updateFn(ctx, key, object); err != nil {
		return errors.Wrapf(ctx, err, "update failed")
	}
	return nil
}

// Delete executes the delete function if provided.
func (e *updaterHandlerFunc[KEY, OBJECT]) Delete(ctx context.Context, key KEY) error {
	if e.deleteFn == nil {
		return nil
	}
	if err := e.deleteFn(ctx, key); err != nil {
		return errors.Wrapf(ctx, err, "delete failed")
	}
	return nil
}
