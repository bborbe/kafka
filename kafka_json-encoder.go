// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

// NewJSONEncoder creates a new JSON encoder that marshals the given value into a Sarama encoder.
func NewJSONEncoder(ctx context.Context, value interface{}) (sarama.Encoder, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "marshal failed")
	}
	return sarama.ByteEncoder(bytes), nil
}

// NewJsonEncoder creates a new JSON encoder that marshals the given value into a Sarama encoder.
//
// Deprecated: Use NewJSONEncoder instead.
//
//nolint:revive
func NewJsonEncoder(ctx context.Context, value interface{}) (sarama.Encoder, error) {
	return NewJSONEncoder(ctx, value)
}
