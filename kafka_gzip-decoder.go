// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

// GzipActive reports whether the given record headers carry the gzip "active"
// marker (GzipHeaderKey / GzipHeaderValue) written by gzip-compressing producers.
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

// GzipDecoder decompresses gzip-compressed data.
// It takes compressed data as a byte slice, decompresses it using gzip,
// and returns the decompressed data as a byte slice.
// Returns an error if the data is nil, not valid gzip format, or decompression fails.
func GzipDecoder(
	ctx context.Context,
	compressedData []byte,
) ([]byte, error) {
	if compressedData == nil {
		return nil, errors.New(ctx, "compressed data cannot be nil")
	}
	if len(compressedData) == 0 {
		return nil, errors.New(ctx, "compressed data cannot be empty")
	}

	reader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create gzip reader failed")
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read decompressed data failed")
	}

	return decompressed, nil
}
