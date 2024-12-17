// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"strings"
)

func ParseBroker(value string) Broker {
	result := Broker(value)
	if result.Schema() == "" {
		return ParseBroker(strings.Join([]string{PlainSchema.String(), value}, "://"))
	}
	return result
}

type Broker string

func (b Broker) String() string {
	return string(b)
}

func (b Broker) Schema() BrokerSchema {
	parts := strings.Split(b.String(), "://")
	if len(parts) != 2 {
		return ""
	}
	return BrokerSchema(parts[0])
}

func (b Broker) Host() string {
	parts := strings.Split(b.String(), "://")
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}
