// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"io"
	"sync"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/golang/glog"
)

type SaramaOffsetManager interface {
	OffsetManager
	io.Closer
}

func NewSaramaOffsetManager(
	saramaClient SaramaClient,
	group Group,
	initalOffset Offset,
) SaramaOffsetManager {
	return &saramaOffsetManager{
		saramaClient:            saramaClient,
		group:                   group,
		initalOffset:            initalOffset,
		partitionOffsetManagers: make(map[TopicPartition]sarama.PartitionOffsetManager),
	}
}

type saramaOffsetManager struct {
	saramaClient SaramaClient
	initalOffset Offset
	group        Group

	mux                     sync.Mutex
	offsetManager           sarama.OffsetManager
	partitionOffsetManagers map[TopicPartition]sarama.PartitionOffsetManager
}

func (s *saramaOffsetManager) InitialOffset() Offset {
	return s.initalOffset
}

func (s *saramaOffsetManager) NextOffset(ctx context.Context, topic Topic, partition Partition) (Offset, error) {
	partitionOffsetManager, err := s.getPartitionManager(ctx, topic, partition)
	if err != nil {
		return 0, errors.Wrapf(ctx, err, "get partition manager failed")
	}
	nextOffset, metadata := partitionOffsetManager.NextOffset()
	if metadata != DefaultMetadata.String() {
		glog.V(2).Infof("metadata missing => use inital offset %s", s.initalOffset)
		nextOffset = s.initalOffset.Int64()
	}
	return Offset(nextOffset), nil
}

func (s *saramaOffsetManager) MarkOffset(ctx context.Context, topic Topic, partition Partition, nextOffset Offset) error {
	partitionOffsetManager, err := s.getPartitionManager(ctx, topic, partition)
	if err != nil {
		return errors.Wrapf(ctx, err, "get partition manager failed")
	}
	partitionOffsetManager.MarkOffset(nextOffset.Int64(), DefaultMetadata.String())
	glog.V(3).Infof("mark offset to %d", nextOffset)
	return nil
}

func (s *saramaOffsetManager) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.offsetManager != nil {
		var errs []error
		for _, p := range s.partitionOffsetManagers {
			errs = append(errs, p.Close())
		}
		errs = append(errs, s.offsetManager.Close())
		return errors.Join(errs...)
	}
	return nil
}

func (s *saramaOffsetManager) getPartitionManager(ctx context.Context, topic Topic, partition Partition) (sarama.PartitionOffsetManager, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	topicPartition := TopicPartition{
		Topic:     topic,
		Partition: partition,
	}
	p, ok := s.partitionOffsetManagers[topicPartition]
	if ok {
		return p, nil
	}

	offsetManager, err := s.getOffsetManager(ctx)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get offsetManger failed")
	}
	partitionOffsetManager, err := offsetManager.ManagePartition(topic.String(), partition.Int32())
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create partition manager failed")
	}
	s.partitionOffsetManagers[topicPartition] = partitionOffsetManager
	return partitionOffsetManager, nil
}

func (s *saramaOffsetManager) getOffsetManager(ctx context.Context) (sarama.OffsetManager, error) {
	if s.offsetManager == nil {
		offsetManager, err := sarama.NewOffsetManagerFromClient(s.group.String(), s.saramaClient)
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "create offset manager failed")
		}
		s.offsetManager = offsetManager
	}
	return s.offsetManager, nil
}
