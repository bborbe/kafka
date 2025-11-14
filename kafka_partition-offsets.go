// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"encoding/json"

	"github.com/bborbe/errors"
)

// ParsePartitionOffsetFromBytes parses partition offsets from JSON bytes.
func ParsePartitionOffsetFromBytes(
	ctx context.Context,
	offsetBytes []byte,
) (PartitionOffsets, error) {
	var partitionOffsetItems PartitionOffsetItems
	if err := json.Unmarshal(offsetBytes, &partitionOffsetItems); err != nil {
		return nil, errors.Wrapf(ctx, err, "parse partitionOffsetItems failed")
	}
	return partitionOffsetItems.Offsets(), nil
}

// PartitionOffsets represents a mapping of partitions to their corresponding offsets.
type PartitionOffsets map[Partition]Offset

// Clone creates a deep copy of the partition offsets.
func (o PartitionOffsets) Clone() PartitionOffsets {
	result := PartitionOffsets{}
	for k, v := range o {
		result[k] = v
	}
	return result
}

// Bytes serializes the partition offsets to JSON bytes.
func (o PartitionOffsets) Bytes() ([]byte, error) {
	return json.Marshal(o.OffsetPartitions())
}

// OffsetPartitions converts the map to a slice of PartitionOffsetItem.
func (o PartitionOffsets) OffsetPartitions() PartitionOffsetItems {
	result := make([]PartitionOffsetItem, 0, len(o))
	for partition, offset := range o {
		result = append(result, PartitionOffsetItem{
			Offset:    offset,
			Partition: partition,
		})
	}
	return result
}

// PartitionOffsetItems represents a slice of partition offset items.
type PartitionOffsetItems []PartitionOffsetItem

// PartitionOffsetItem represents a single partition and its offset.
type PartitionOffsetItem struct {
	Offset    Offset
	Partition Partition
}

// Offsets converts the slice to a PartitionOffsets map.
func (o PartitionOffsetItems) Offsets() PartitionOffsets {
	offsets := PartitionOffsets{}
	for _, item := range o {
		offsets[item.Partition] = item.Offset
	}
	return offsets
}
