// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
)

func UpdaterHandlerFunc[OBJECT any, KEY ~[]byte | ~string](
	update func(ctx context.Context, key KEY, object OBJECT) error,
	delete func(ctx context.Context, key KEY) error,
) UpdaterHandler[OBJECT, KEY] {
	return &updaterHandlerFunc[OBJECT, KEY]{
		update: update,
		delete: delete,
	}
}

type updaterHandlerFunc[OBJECT any, KEY ~[]byte | ~string] struct {
	update func(ctx context.Context, key KEY, object OBJECT) error
	delete func(ctx context.Context, OBJECT KEY) error
}

func (e *updaterHandlerFunc[OBJECT, KEY]) Update(ctx context.Context, key KEY, object OBJECT) error {
	if e.update == nil {
		return nil
	}
	if err := e.update(ctx, key, object); err != nil {
		return errors.Wrapf(ctx, err, "update failed")
	}
	return nil
}

func (e *updaterHandlerFunc[OBJECT, KEY]) Delete(ctx context.Context, key KEY) error {
	if e.delete == nil {
		return nil
	}
	if err := e.delete(ctx, key); err != nil {
		return errors.Wrapf(ctx, err, "delete failed")
	}
	return nil
}
