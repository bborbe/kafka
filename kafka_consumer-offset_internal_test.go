// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

// fakePartitionConsumer is a minimal in-package fake implementing
// sarama.PartitionConsumer for tests that exercise channel-close behaviour.
type fakePartitionConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func (f *fakePartitionConsumer) AsyncClose() {}

func (f *fakePartitionConsumer) Close() error { return nil }

func (f *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage { return f.messages }

func (f *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError { return f.errors }

func (f *fakePartitionConsumer) HighWaterMarkOffset() int64 { return 0 }

func (f *fakePartitionConsumer) Pause() {}

func (f *fakePartitionConsumer) Resume() {}

func (f *fakePartitionConsumer) IsPaused() bool { return false }

func newOffsetConsumerForTest(batchSize int) *offsetConsumer {
	return &offsetConsumer{
		batchSize:    BatchSize(batchSize),
		errorHandler: NewConsumerErrorHandler(NewMetrics()),
	}
}

func TestConsumeMessages_OuterErrorsChannelClosed(t *testing.T) {
	pc := &fakePartitionConsumer{}
	errCh := make(chan *sarama.ConsumerError)
	close(errCh)
	pc.errors = errCh

	result, err := newOffsetConsumerForTest(1).consumeMessages(context.Background(), pc)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "errors channel closed") {
		t.Errorf("expected 'errors channel closed' in error, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestConsumeMessages_OuterMessagesChannelClosed(t *testing.T) {
	pc := &fakePartitionConsumer{}
	msgCh := make(chan *sarama.ConsumerMessage)
	close(msgCh)
	pc.messages = msgCh

	result, err := newOffsetConsumerForTest(1).consumeMessages(context.Background(), pc)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "messages channel closed") {
		t.Errorf("expected 'messages channel closed' in error, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestConsumeMessages_InnerErrorsChannelClosed(t *testing.T) {
	pc := &fakePartitionConsumer{}
	msgCh := make(chan *sarama.ConsumerMessage, 1)
	msgCh <- &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 0}
	pc.messages = msgCh

	errCh := make(chan *sarama.ConsumerError)
	close(errCh)
	pc.errors = errCh

	_, err := newOffsetConsumerForTest(5).consumeMessages(context.Background(), pc)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "errors channel closed") {
		t.Errorf("expected 'errors channel closed' in error, got: %v", err)
	}
}

func TestConsumeMessages_InnerMessagesChannelClosed(t *testing.T) {
	pc := &fakePartitionConsumer{}
	msgCh := make(chan *sarama.ConsumerMessage, 1)
	msgCh <- &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 0}
	close(msgCh)
	pc.messages = msgCh

	_, err := newOffsetConsumerForTest(5).consumeMessages(context.Background(), pc)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "messages channel closed") {
		t.Errorf("expected 'messages channel closed' in error, got: %v", err)
	}
}

func TestConsumeMessages_ContextCancelled(t *testing.T) {
	pc := &fakePartitionConsumer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newOffsetConsumerForTest(1).consumeMessages(ctx, pc)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestConsumeMessages_SingleMessageThenDefault(t *testing.T) {
	pc := &fakePartitionConsumer{}
	msgCh := make(chan *sarama.ConsumerMessage, 1)
	msgCh <- &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 0}
	pc.messages = msgCh

	result, err := newOffsetConsumerForTest(5).consumeMessages(context.Background(), pc)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 message, got %d", len(result))
	}
}

func TestConsumeMessages_PacketDecodingError_SkipOff_PropagatesError(t *testing.T) {
	pc := &fakePartitionConsumer{}
	errCh := make(chan *sarama.ConsumerError, 1)
	errCh <- &sarama.ConsumerError{
		Topic:     "test-topic",
		Partition: 0,
		Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
	}
	pc.errors = errCh

	consumer := newOffsetConsumerForTest(1)
	consumer.consumerOptions.SkipCorruptBatches = false

	result, err := consumer.consumeMessages(context.Background(), pc)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestConsumeMessages_PacketDecodingError_SkipOn_ReturnsSentinel(t *testing.T) {
	pc := &fakePartitionConsumer{}
	errCh := make(chan *sarama.ConsumerError, 1)
	errCh <- &sarama.ConsumerError{
		Topic:     "test-topic",
		Partition: 0,
		Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
	}
	pc.errors = errCh

	consumer := newOffsetConsumerForTest(1)
	consumer.consumerOptions.SkipCorruptBatches = true

	result, err := consumer.consumeMessages(context.Background(), pc)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "skip corrupt batch") {
		t.Errorf("expected sentinel error 'skip corrupt batch', got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestConsumerErrorHandler_HandleError_ReturnsNonNil(t *testing.T) {
	handler := NewConsumerErrorHandler(NewMetrics())

	err := handler.HandleError(&sarama.ConsumerError{
		Topic:     "test-topic",
		Partition: 0,
		Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
	})

	if err == nil {
		t.Errorf("expected non-nil error, got nil")
	}
}

func TestIsCorruptionError_PacketDecodingError(t *testing.T) {
	err := sarama.PacketDecodingError{Info: "CRC mismatch"}
	if !IsCorruptionError(&err) {
		t.Errorf("expected IsCorruptionError to return true for PacketDecodingError")
	}
}

func TestIsCorruptionError_CRCInMessage(t *testing.T) {
	err := sarama.PacketDecodingError{Info: "message contents does not match data"}
	if !IsCorruptionError(&err) {
		t.Errorf("expected IsCorruptionError to return true for message contents mismatch")
	}
}

func TestIsCorruptionError_NilError(t *testing.T) {
	if IsCorruptionError(nil) {
		t.Errorf("expected IsCorruptionError to return false for nil")
	}
}

func TestIsCorruptionError_GenericError(t *testing.T) {
	err := sarama.PacketDecodingError{Info: "some generic error"}
	if IsCorruptionError(&err) {
		t.Errorf("expected IsCorruptionError to return false for generic error")
	}
}

func TestWithSkipCorruptBatches_Option(t *testing.T) {
	opts := ConsumerOptions{}
	WithSkipCorruptBatches(true)(&opts)
	if !opts.SkipCorruptBatches {
		t.Errorf("expected SkipCorruptBatches to be true")
	}

	opts = ConsumerOptions{}
	WithSkipCorruptBatches(false)(&opts)
	if opts.SkipCorruptBatches {
		t.Errorf("expected SkipCorruptBatches to be false")
	}
}
