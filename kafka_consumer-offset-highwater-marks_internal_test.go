// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	stderrors "errors"
	"reflect"
	"testing"

	"github.com/IBM/sarama"
)

func TestCreateTriggerOffsets_RetriesTransientErrorThenSucceeds(t *testing.T) {
	backoff := triggerOffsetsBackoff
	backoff.Delay = 0

	client := &testSaramaClient{
		partitions:      []int32{0},
		getOffsetErrors: []error{sarama.ErrNotLeaderForPartition},
		getOffset:       10,
	}
	offsetManager := &fakeOffsetManager{nextOffset: Offset(5)}

	result, err := createTriggerOffsets(
		context.Background(),
		client,
		"test-topic",
		offsetManager,
		backoff,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client.getOffsetCalls != 2 {
		t.Errorf("expected 2 get offset calls, got %d", client.getOffsetCalls)
	}
	expected := map[Partition]Offset{0: 9}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected trigger offsets %v, got %v", expected, result)
	}
}

func TestCreateTriggerOffsets_ExhaustsRetriesOnPersistentTransientError(t *testing.T) {
	backoff := triggerOffsetsBackoff
	backoff.Delay = 0
	backoff.Retries = 2

	client := &testSaramaClient{
		partitions:   []int32{0},
		getOffsetErr: sarama.ErrNotLeaderForPartition,
	}
	offsetManager := &fakeOffsetManager{nextOffset: Offset(5)}

	result, err := createTriggerOffsets(
		context.Background(),
		client,
		"test-topic",
		offsetManager,
		backoff,
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if client.getOffsetCalls != 3 {
		t.Errorf("expected 3 get offset calls, got %d", client.getOffsetCalls)
	}
	if !IsTransientBrokerError(err) {
		t.Errorf("expected exhausted error to remain transient, got %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestCreateTriggerOffsets_DoesNotRetryNonTransientError(t *testing.T) {
	backoff := triggerOffsetsBackoff
	backoff.Delay = 0

	client := &testSaramaClient{
		partitions:   []int32{0},
		getOffsetErr: stderrors.New("boom"),
	}
	offsetManager := &fakeOffsetManager{nextOffset: Offset(5)}

	result, err := createTriggerOffsets(
		context.Background(),
		client,
		"test-topic",
		offsetManager,
		backoff,
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if client.getOffsetCalls != 1 {
		t.Errorf("expected 1 get offset call, got %d", client.getOffsetCalls)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestCreateTriggerOffsets_SucceedsFirstAttempt(t *testing.T) {
	backoff := triggerOffsetsBackoff
	backoff.Delay = 0

	client := &testSaramaClient{
		partitions: []int32{0},
		getOffset:  10,
	}
	offsetManager := &fakeOffsetManager{nextOffset: Offset(5)}

	result, err := createTriggerOffsets(
		context.Background(),
		client,
		"test-topic",
		offsetManager,
		backoff,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client.getOffsetCalls != 1 {
		t.Errorf("expected 1 get offset call, got %d", client.getOffsetCalls)
	}
	expected := map[Partition]Offset{0: 9}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected trigger offsets %v, got %v", expected, result)
	}
}
