// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package parse

import (
	"context"
	"time"

	"github.com/bborbe/errors"
)

func ParseTime(ctx context.Context, value string, format string) (time.Time, error) {
	t, err := time.Parse(format, value)
	if err != nil {
		return time.Time{}, errors.Wrapf(ctx, err, "parse '%s' with format '%s' failed", value, format)
	}
	return t, nil
}
