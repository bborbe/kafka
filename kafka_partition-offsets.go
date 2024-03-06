// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"encoding/json"

	"github.com/bborbe/errors"
)

func ParsePartitionOffsetFromBytes(ctx context.Context, offsetBytes []byte) (PartitionOffsets, error) {
	var partitionOffsetItems PartitionOffsetItems
	if err := json.Unmarshal(offsetBytes, &partitionOffsetItems); err != nil {
		return nil, errors.Wrapf(ctx, err, "parse partitionOffsetItems failed")
	}
	return partitionOffsetItems.Offsets(), nil
}

type PartitionOffsets map[Partition]Offset

func (o PartitionOffsets) Clone() PartitionOffsets {
	result := PartitionOffsets{}
	for k, v := range o {
		result[k] = v
	}
	return result
}

func (o PartitionOffsets) Bytes() ([]byte, error) {
	return json.Marshal(o.OffsetPartitions())
}

func (o PartitionOffsets) OffsetPartitions() PartitionOffsetItems {
	var result []PartitionOffsetItem
	for partition, offset := range o {
		result = append(result, PartitionOffsetItem{
			Offset:    offset,
			Partition: partition,
		})
	}
	return result
}

type PartitionOffsetItems []PartitionOffsetItem

type PartitionOffsetItem struct {
	Offset    Offset
	Partition Partition
}

func (o PartitionOffsetItems) Offsets() PartitionOffsets {
	offsets := PartitionOffsets{}
	for _, item := range o {
		offsets[item.Partition] = item.Offset
	}
	return offsets
}
