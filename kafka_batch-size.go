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

// BatchSize represents the number of messages to process in a single batch operation.
type BatchSize int

func (s BatchSize) String() string {
	return strconv.Itoa(s.Int())
}

// Int returns the batch size as an int value.
func (s BatchSize) Int() int {
	return int(s)
}

// Int64 returns the batch size as an int64 value.
func (s BatchSize) Int64() int64 {
	return int64(s)
}

// Validate ensures the batch size is greater than zero.
func (s BatchSize) Validate(ctx context.Context) error {
	if s < 1 {
		return errors.Wrapf(ctx, validation.Error, "invalid batchSize")
	}
	return nil
}
