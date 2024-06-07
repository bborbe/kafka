// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
)

type UpdaterHandler[KEY ~[]byte | ~string, OBJECT any] interface {
	Update(ctx context.Context, key KEY, object OBJECT) error
	Delete(ctx context.Context, key KEY) error
}
