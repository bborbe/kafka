// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"bytes"

	"github.com/IBM/sarama"
)

// GzipHeaderKey is the Kafka message header key set by gzip-compressing
// producers and read by gzip-decompressing consumers. The value matches
// the legacy seibert-data/lib-kafka constant so v1 and v2 services stay
// wire-compatible during migration.
const GzipHeaderKey string = "gzip"

// GzipHeaderValue is the Kafka message header value indicating that the
// payload has been gzip-compressed. Wire-compatible with seibert-data/lib-kafka.
const GzipHeaderValue string = "active"

// GzipActive reports whether the given record headers carry the gzip
// "active" marker written by gzip-compressing producers.
func GzipActive(headers []*sarama.RecordHeader) bool {
	for _, header := range headers {
		if header == nil {
			continue
		}
		if bytes.Equal(header.Key, []byte(GzipHeaderKey)) &&
			bytes.Equal(header.Value, []byte(GzipHeaderValue)) {
			return true
		}
	}
	return false
}

// RemoveGzipHeader returns a copy of headers with the gzip "active" marker
// stripped out. Use after successfully decompressing a value so downstream
// handlers don't see a stale compression marker.
func RemoveGzipHeader(headers []*sarama.RecordHeader) []*sarama.RecordHeader {
	out := make([]*sarama.RecordHeader, 0, len(headers))
	for _, header := range headers {
		if header == nil {
			continue
		}
		if bytes.Equal(header.Key, []byte(GzipHeaderKey)) &&
			bytes.Equal(header.Value, []byte(GzipHeaderValue)) {
			continue
		}
		out = append(out, header)
	}
	return out
}
