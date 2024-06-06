// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	libkv "github.com/bborbe/kv"
)

type UpdaterHandlerTx[OBJECT any, KEY Key] interface {
	Update(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error
	Delete(ctx context.Context, tx libkv.Tx, key KEY) error
}
