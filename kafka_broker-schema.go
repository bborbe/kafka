// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "github.com/bborbe/collection"

// BrokerSchemas represents a slice of BrokerSchema values.
type BrokerSchemas []BrokerSchema

// Contains checks if the given schema exists in the BrokerSchemas slice.
func (s BrokerSchemas) Contains(schema BrokerSchema) bool {
	return collection.Contains(s, schema)
}

// BrokerSchema represents the connection schema type for a Kafka broker (plain or tls).
type BrokerSchema string

// String returns the string representation of the BrokerSchema.
func (s BrokerSchema) String() string {
	return string(s)
}

const (
	// PlainSchema represents a plain text connection to Kafka broker.
	PlainSchema BrokerSchema = "plain"
	// TLSSchema represents a TLS-encrypted connection to Kafka broker.
	TLSSchema BrokerSchema = "tls"
)
