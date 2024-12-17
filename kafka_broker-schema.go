// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "github.com/bborbe/collection"

type BrokerSchemas []BrokerSchema

func (s BrokerSchemas) Contains(schema BrokerSchema) bool {
	return collection.Contains(s, schema)
}

type BrokerSchema string

func (s BrokerSchema) String() string {
	return string(s)
}

const (
	PlainSchema BrokerSchema = "plain"
	TLSSchema   BrokerSchema = "tls"
)
