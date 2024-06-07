// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
)

type Filter[KEY ~[]byte | ~string, OBJECT any] interface {
	// Filtered return true if should be filter out
	Filtered(ctx context.Context, key KEY, object OBJECT) (bool, error)
}

type FilterFunc[KEY ~[]byte | ~string, OBJECT any] func(ctx context.Context, key KEY, object OBJECT) (bool, error)

func (f FilterFunc[KEY, OBJECT]) Filtered(ctx context.Context, key KEY, object OBJECT) (bool, error) {
	return f(ctx, key, object)
}

func NewUpdaterHandlerFilter[KEY ~[]byte | ~string, OBJECT any](
	filter Filter[KEY, OBJECT],
	updateHandler UpdaterHandler[KEY, OBJECT],
) UpdaterHandler[KEY, OBJECT] {
	return UpdaterHandlerFunc[KEY, OBJECT](
		func(ctx context.Context, key KEY, object OBJECT) error {
			filtered, err := filter.Filtered(ctx, key, object)
			if err != nil {
				return errors.Wrapf(ctx, err, "filtered failed")
			}
			if filtered {
				return updateHandler.Delete(ctx, key)
			}
			return updateHandler.Update(ctx, key, object)
		},
		func(ctx context.Context, key KEY) error {
			return updateHandler.Delete(ctx, key)
		},
	)
}
