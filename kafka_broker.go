// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"strings"
)

// ParseBroker parses a string value into a Broker, adding a default plain schema if none is specified.
func ParseBroker(value string) Broker {
	result := Broker(value)
	if result.Schema() == "" {
		return ParseBroker(strings.Join([]string{PlainSchema.String(), value}, "://"))
	}
	return result
}

// Broker represents a Kafka broker address with schema and host information.
type Broker string

// String returns the string representation of the Broker.
func (b Broker) String() string {
	return string(b)
}

// Schema extracts and returns the schema portion of the broker address.
func (b Broker) Schema() BrokerSchema {
	parts := strings.Split(b.String(), "://")
	if len(parts) != 2 {
		return ""
	}
	return BrokerSchema(parts[0])
}

// Host extracts and returns the host portion of the broker address.
func (b Broker) Host() string {
	parts := strings.Split(b.String(), "://")
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

// UnmarshalText implements encoding.TextUnmarshaler for Broker.
func (b *Broker) UnmarshalText(text []byte) error {
	*b = ParseBroker(string(text))
	return nil
}
