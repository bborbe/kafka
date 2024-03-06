// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"

	"github.com/bborbe/errors"
)

func PartitionsFromInt32(partitions []int32) Partitions {
	var result Partitions
	for _, partition := range partitions {
		result = append(result, Partition(partition))
	}
	return result
}

type Partitions []Partition

// ParsePartition from a string
func ParsePartition(ctx context.Context, value string) (*Partition, error) {
	parseInt, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "parse value '%s' as partition failed", value)
	}
	partition := Partition(parseInt)
	return &partition, nil
}

// Partition in Kafka.
type Partition int32

// Int32 value of the partition.
func (p Partition) Int32() int32 {
	return int32(p)
}

func (p Partition) String() string {
	return strconv.FormatInt(int64(p), 10)
}

// Bytes representation for the partion.
func (p Partition) Bytes() []byte {
	result := &bytes.Buffer{}
	binary.Write(result, binary.BigEndian, p.Int32())
	return result.Bytes()
}

// PartitionFromBytes returns the partition for the given bytes.
func PartitionFromBytes(content []byte) Partition {
	var result int32
	_ = binary.Read(bytes.NewBuffer(content), binary.BigEndian, &result)
	return Partition(result)
}
