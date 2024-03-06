// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package parse

import (
	"context"

	"github.com/bborbe/errors"
)

func ParseBool(ctx context.Context, value interface{}) (bool, error) {
	v, ok := value.(bool)
	if !ok {
		return false, errors.Errorf(ctx, "invalid type")
	}
	return v, nil
}
