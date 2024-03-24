// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "strconv"

type BatchSize int

func (s BatchSize) String() string {
	return strconv.Itoa(s.Int())
}

func (s BatchSize) Int() int {
	return s.Int()
}
