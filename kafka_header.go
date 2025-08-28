// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"github.com/IBM/sarama"
)

// ParseHeader converts Sarama record headers into a Header map.
func ParseHeader(saramaHeaders []*sarama.RecordHeader) Header {
	header := Header{}
	for _, saramaHeader := range saramaHeaders {
		header.Add(string(saramaHeader.Key), string(saramaHeader.Value))
	}
	return header
}

// Header represents HTTP-style headers for Kafka messages with support for multiple values per key.
type Header map[string][]string

// Add appends a value to the header key, supporting multiple values per key.
func (h Header) Add(key string, value string) {
	h[key] = append(h[key], value)
}

// Set replaces all values for the given header key.
func (h Header) Set(key string, values []string) {
	h[key] = values
}

// Remove deletes all values for the given header key.
func (h Header) Remove(key string) {
	delete(h, key)
}

// Get returns the first value for the given header key, or empty string if not found.
func (h Header) Get(key string) string {
	values := h[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// AsSaramaHeaders converts the Header into Sarama record headers format.
func (h Header) AsSaramaHeaders() []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0, len(h))
	for key, values := range h {
		for _, value := range values {
			headers = append(headers, sarama.RecordHeader{
				Key:   []byte(key),
				Value: []byte(value),
			})
		}
	}
	return headers
}
