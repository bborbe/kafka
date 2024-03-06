// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "context"

type Consumer interface {
	Consume(ctx context.Context) error
}

type ConsumerFunc func(ctx context.Context) error

func (c ConsumerFunc) Consume(ctx context.Context) error {
	return c(ctx)
}
