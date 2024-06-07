// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

//counterfeiter:generate -o mocks/kafka-offset-store.go --fake-name KafkaOffsetStore . OffsetStore
type OffsetStore interface {
	Get(ctx context.Context, topic Topic, partition Partition) (Offset, error)
	Set(ctx context.Context, topic Topic, partition Partition, offset Offset) error
}

func NewOffsetStore(
	db libkv.DB,
) OffsetStore {
	return &offsetStore{
		db:     db,
		bucket: libkv.NewBucketName("offset-store"),
	}
}

func NewOffsetStoreGroup(
	db libkv.DB,
	group Group,
) OffsetStore {
	return &offsetStore{
		db:     db,
		bucket: libkv.BucketFromStrings("offset-store", group.String()),
	}
}

type offsetStore struct {
	bucket libkv.BucketName
	db     libkv.DB
}

func (o *offsetStore) Get(ctx context.Context, topic Topic, partition Partition) (Offset, error) {
	topicPartition := TopicPartition{
		Topic:     topic,
		Partition: partition,
	}
	var result Offset
	err := o.db.View(ctx, func(ctx context.Context, tx libkv.Tx) error {
		bucket, err := tx.Bucket(ctx, o.bucket)
		if err != nil {
			return errors.Wrapf(ctx, err, "get bucket failed")
		}
		item, err := bucket.Get(ctx, topicPartition.Bytes())
		if err != nil {
			return errors.Wrapf(ctx, err, "get failed")
		}
		err = item.Value(func(val []byte) error {
			if len(val) == 0 {
				return libkv.KeyNotFoundError
			}
			result = OffsetFromBytes(val)
			return nil
		})
		if err != nil {
			return errors.Wrapf(ctx, err, "value failed")
		}
		return nil
	})
	if err != nil {
		return 0, errors.Wrapf(ctx, err, "get offset failed")
	}
	return result, nil
}

func (o *offsetStore) Set(ctx context.Context, topic Topic, partition Partition, offset Offset) error {
	err := o.db.Update(ctx, func(ctx context.Context, tx libkv.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(ctx, o.bucket)
		if err != nil {
			return errors.Wrapf(ctx, err, "get bucket failed")
		}
		topicPartition := TopicPartition{
			Topic:     topic,
			Partition: partition,
		}
		return bucket.Put(ctx, topicPartition.Bytes(), offset.Bytes())
	})
	if err != nil {
		return errors.Wrapf(ctx, err, "update offset failed")
	}
	return nil
}
