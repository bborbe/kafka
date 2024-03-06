// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"strings"
)

type Brokers []Broker

func ParseBrokersFromString(value string) Brokers {
	return ParseBrokers(strings.FieldsFunc(value, func(r rune) bool {
		return r == ','
	}))
}

func ParseBrokers(values []string) Brokers {
	result := make(Brokers, len(values))
	for i, value := range values {
		result[i] = Broker(value)
	}
	return result
}

func (f Brokers) String() string {
	return strings.Join(f.Strings(), ",")
}

func (f Brokers) Strings() []string {
	result := make([]string, len(f))
	for i, b := range f {
		result[i] = b.String()
	}
	return result
}

type Broker string

func (f Broker) String() string {
	return string(f)
}
