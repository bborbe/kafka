// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

// Offset in the Kafka topic.
type Offset int64

func (o Offset) String() string {
	switch o {
	case OffsetNewest:
		return "newest"
	case OffsetOldest:
		return "oldest"
	default:
		return strconv.FormatInt(o.Int64(), 10)
	}
}

// Int64 value for the offset.
func (o Offset) Int64() int64 {
	return int64(o)
}

// Bytes representation for the offset.
func (o Offset) Bytes() []byte {
	result := &bytes.Buffer{}
	binary.Write(result, binary.BigEndian, o.Int64())
	return result.Bytes()
}

func (o Offset) Ptr() *Offset {
	return &o
}

// OffsetFromBytes returns the offset for the given bytes.
func OffsetFromBytes(content []byte) Offset {
	var result int64
	_ = binary.Read(bytes.NewBuffer(content), binary.BigEndian, &result)
	return Offset(result)
}

const OffsetNewest = Offset(sarama.OffsetNewest)
const OffsetOldest = Offset(sarama.OffsetOldest)

// ParseOffset from a string
func ParseOffset(ctx context.Context, value string) (*Offset, error) {
	var offset Offset
	switch value {
	case "newest":
		offset = OffsetNewest
	case "oldest":
		offset = OffsetOldest
	default:
		parseInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "parse value '%s' as offset failed", value)
		}
		offset = Offset(parseInt)
	}
	return &offset, nil
}

// GetRealOffset get offset for newest or oldest
func GetRealOffset(
	ctx context.Context,
	saramaClient sarama.Client,
	topic Topic,
	offsets map[Partition]Offset,
) (map[Partition]Offset, error) {
	result := map[Partition]Offset{}
	for p, o := range offsets {
		if o >= 0 {
			result[p] = o
			continue
		}
		offset, err := saramaClient.GetOffset(topic.String(), p.Int32(), o.Int64())
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "get offset failed")
		}
		result[p] = Offset(offset)
	}
	return result, nil
}
