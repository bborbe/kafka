// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "strings"

// IsBrokenPipeError checks if the given error represents a broken pipe error.
// It returns true if the error message ends with "broken pipe".
func IsBrokenPipeError(err error) bool {
	return strings.HasSuffix(err.Error(), "broken pipe")
}
