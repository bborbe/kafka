// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"strconv"

	"github.com/bborbe/errors"
	"github.com/bborbe/validation"
)

type BatchSize int

func (s BatchSize) String() string {
	return strconv.Itoa(s.Int())
}

func (s BatchSize) Int() int {
	return int(s)
}

func (s BatchSize) Validate(ctx context.Context) error {
	if s < 1 {
		return errors.Wrapf(ctx, validation.Error, "invalid batchSize")
	}
	return nil
}
