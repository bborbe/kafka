// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"github.com/bborbe/errors"
)

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
