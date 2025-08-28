// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"strings"
)

// ParseBrokersFromString parses a comma-separated string of broker addresses into a Brokers slice.
func ParseBrokersFromString(value string) Brokers {
	return ParseBrokers(strings.FieldsFunc(value, func(r rune) bool {
		return r == ','
	}))
}

// ParseBrokers converts a slice of string broker addresses into a Brokers slice.
func ParseBrokers(values []string) Brokers {
	result := make(Brokers, len(values))
	for i, value := range values {
		result[i] = ParseBroker(value)
	}
	return result
}

// Brokers represents a collection of Kafka broker addresses.
type Brokers []Broker

// Schemas returns a slice of all broker schemas from the Brokers collection.
func (b Brokers) Schemas() BrokerSchemas {
	result := make(BrokerSchemas, len(b))
	for i, value := range b {
		result[i] = value.Schema()
	}
	return result
}

// Hosts returns a slice of all broker host addresses from the Brokers collection.
func (b Brokers) Hosts() []string {
	result := make([]string, len(b))
	for i, value := range b {
		result[i] = value.Host()
	}
	return result
}

// String returns a comma-separated string representation of all brokers.
func (b Brokers) String() string {
	return strings.Join(b.Strings(), ",")
}

// Strings returns a slice of string representations of all brokers.
func (b Brokers) Strings() []string {
	result := make([]string, len(b))
	for i, b := range b {
		result[i] = b.String()
	}
	return result
}
