// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "github.com/bborbe/collection"

type Topics []Topic

func (t Topics) Contains(topic Topic) bool {
	return collection.Contains(t, topic)
}

func (t Topics) Unique() Topics {
	return collection.Unique(t)
}

func (t Topics) Interfaces() []interface{} {
	result := make([]interface{}, len(t))
	for i, ss := range t {
		result[i] = ss
	}
	return result
}

func (t Topics) Strings() []string {
	result := make([]string, len(t))
	for i, ss := range t {
		result[i] = ss.String()
	}
	return result
}
