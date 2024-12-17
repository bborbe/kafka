// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"strings"
)

func ParseBrokersFromString(value string) Brokers {
	return ParseBrokers(strings.FieldsFunc(value, func(r rune) bool {
		return r == ','
	}))
}

func ParseBrokers(values []string) Brokers {
	result := make(Brokers, len(values))
	for i, value := range values {
		result[i] = ParseBroker(value)
	}
	return result
}

type Brokers []Broker

func (b Brokers) Schemas() BrokerSchemas {
	result := make(BrokerSchemas, len(b))
	for i, value := range b {
		result[i] = value.Schema()
	}
	return result
}

func (b Brokers) Hosts() []string {
	result := make([]string, len(b))
	for i, value := range b {
		result[i] = value.Host()
	}
	return result
}

func (b Brokers) String() string {
	return strings.Join(b.Strings(), ",")
}

func (b Brokers) Strings() []string {
	result := make([]string, len(b))
	for i, b := range b {
		result[i] = b.String()
	}
	return result
}
