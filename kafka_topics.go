// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"strings"

	"github.com/bborbe/collection"
)

func ParseTopicsFromString(value string) Topics {
	return ParseTopics(strings.FieldsFunc(value, func(r rune) bool {
		return r == ','
	}))
}

func ParseTopics(values []string) Topics {
	result := make(Topics, len(values))
	for i, value := range values {
		result[i] = Topic(value)
	}
	return result
}

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

func (t Topics) Len() int           { return len(t) }
func (t Topics) Less(i, j int) bool { return strings.Compare(t[i].String(), t[j].String()) < 1 }
func (t Topics) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
