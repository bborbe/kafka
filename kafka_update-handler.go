// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
)

// UpdaterHandler defines a generic interface for handling update and delete operations
// on objects identified by keys of type KEY.
type UpdaterHandler[KEY ~[]byte | ~string, OBJECT any] interface {
	Update(ctx context.Context, key KEY, object OBJECT) error
	Delete(ctx context.Context, key KEY) error
}
