// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"bytes"
	"compress/gzip"
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

// NewGzipEncoder compresses a byte slice using gzip compression with default compression level.
// It returns the compressed data as a sarama.ByteEncoder.
// Returns an error if compression fails.
func NewGzipEncoder(
	ctx context.Context,
	value []byte,
) (sarama.ByteEncoder, error) {
	return NewGzipEncoderWithLevel(
		ctx,
		value,
		gzip.DefaultCompression,
	)
}

// NewGzipEncoderWithLevel compresses a byte slice using gzip compression with a specified compression level.
// It returns the compressed data as a sarama.ByteEncoder.
// The level parameter should be one of: gzip.NoCompression, gzip.BestSpeed, gzip.BestCompression, or gzip.DefaultCompression.
// Returns an error if the compression level is invalid or compression fails.
func NewGzipEncoderWithLevel(
	ctx context.Context,
	value []byte,
	level int,
) (sarama.ByteEncoder, error) {
	gzipBuf := &bytes.Buffer{}
	compressWriter, err := gzip.NewWriterLevel(gzipBuf, level)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create gzip writer failed")
	}
	if _, err := compressWriter.Write(value); err != nil {
		return nil, errors.Wrapf(ctx, err, "write compressed data failed")
	}
	if err := compressWriter.Close(); err != nil {
		return nil, errors.Wrapf(ctx, err, "close gzip writer failed")
	}
	return gzipBuf.Bytes(), nil
}
