// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	stderrors "errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	libtime "github.com/bborbe/time"
)

// fakeOffsetManager is a minimal fake for OffsetManager.
type fakeOffsetManager struct {
	nextOffset        Offset
	nextOffsetErr     error
	fallbackOffset    Offset
	markOffsetCalled  int32
	markOffsetTopic   Topic
	markOffsetPart    Partition
	markOffsetOffset  Offset
	markOffsetErr     error
	resetOffsetCalled int32
}

func (f *fakeOffsetManager) InitialOffset() Offset { return OffsetOldest }

func (f *fakeOffsetManager) FallbackOffset() Offset { return f.fallbackOffset }

func (f *fakeOffsetManager) NextOffset(context.Context, Topic, Partition) (Offset, error) {
	return f.nextOffset, f.nextOffsetErr
}

func (f *fakeOffsetManager) MarkOffset(
	_ context.Context,
	topic Topic,
	partition Partition,
	offset Offset,
) error {
	atomic.AddInt32(&f.markOffsetCalled, 1)
	f.markOffsetTopic = topic
	f.markOffsetPart = partition
	f.markOffsetOffset = offset
	return f.markOffsetErr
}

func (f *fakeOffsetManager) ResetOffset(context.Context, Topic, Partition, Offset) error {
	atomic.AddInt32(&f.resetOffsetCalled, 1)
	return nil
}

func (f *fakeOffsetManager) Close() error { return nil }

// fakeMessageHandlerBatch is a minimal fake for MessageHandlerBatch.
type fakeMessageHandlerBatch struct {
	err error
}

func (f *fakeMessageHandlerBatch) ConsumeMessages(
	context.Context,
	[]*sarama.ConsumerMessage,
) error {
	return f.err
}

// fakeSkipper implements corruptionSkipper by returning a fixed healthy offset.
type fakeSkipper struct {
	healthyOffset Offset
	err           error
}

func (f *fakeSkipper) FindNextHealthyOffset(
	context.Context,
	sarama.Consumer,
	Topic,
	Partition,
	Offset,
	int64,
) (Offset, error) {
	return f.healthyOffset, f.err
}

// fakeMetricsConsumer is a hand fake for MetricsConsumer + MetricsPartitionConsumer.
type fakeMetricsConsumer struct {
	corruptBatchSkippedCount int32
}

func (f *fakeMetricsConsumer) CurrentOffset(Topic, Partition, Offset) {}

func (f *fakeMetricsConsumer) HighWaterMarkOffset(Topic, Partition, Offset) {}

func (f *fakeMetricsConsumer) ErrorCounterInc(Topic, Partition) {}

func (f *fakeMetricsConsumer) CorruptBatchSkippedCounterInc(Topic, Partition) {
	atomic.AddInt32(&f.corruptBatchSkippedCount, 1)
}

func (f *fakeMetricsConsumer) ConsumePartitionCreateOutOfRangeErrorInitialize(Topic, Partition) {}

func (f *fakeMetricsConsumer) ConsumePartitionCreateOutOfRangeErrorInc(Topic, Partition) {}

func (f *fakeMetricsConsumer) ConsumePartitionCreateFailureInc(Topic, Partition) {}

func (f *fakeMetricsConsumer) ConsumePartitionCreateSuccessInc(Topic, Partition) {}

func (f *fakeMetricsConsumer) ConsumePartitionCreateTotalInc(Topic, Partition) {}

// testSaramaConsumer is a minimal fake implementing sarama.Consumer for testing.
// It only implements the methods actually called by offsetConsumer in the skip path.
type testSaramaConsumer struct {
	consumePartitionCalls []struct {
		topic     string
		partition int32
		offset    int64
	}
	consumePartitionFn func(string, int32, int64) (sarama.PartitionConsumer, error)
	highWaterMarks     map[string]map[int32]int64
}

func (f *testSaramaConsumer) ConsumePartition(
	topic string,
	partition int32,
	offset int64,
) (sarama.PartitionConsumer, error) {
	f.consumePartitionCalls = append(f.consumePartitionCalls, struct {
		topic     string
		partition int32
		offset    int64
	}{topic, partition, offset})
	if f.consumePartitionFn != nil {
		return f.consumePartitionFn(topic, partition, offset)
	}
	return nil, nil
}

func (f *testSaramaConsumer) Close() error { return nil }

func (f *testSaramaConsumer) Topics() ([]string, error) { return nil, nil }

func (f *testSaramaConsumer) Partitions(topic string) ([]int32, error) { return nil, nil }

func (f *testSaramaConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return f.highWaterMarks
}

func (f *testSaramaConsumer) Pause(map[string][]int32) {}

func (f *testSaramaConsumer) PauseAll() {}

func (f *testSaramaConsumer) Resume(map[string][]int32) {}

func (f *testSaramaConsumer) ResumeAll() {}

// testSaramaClient is a minimal fake implementing SaramaClient (which embeds sarama.Client).
// It only implements the methods actually called by offsetConsumer.
type testSaramaClient struct {
	partitions []int32
}

func (f *testSaramaClient) Partitions(topic string) ([]int32, error) { return f.partitions, nil }

func (f *testSaramaClient) Topics() ([]string, error) { return nil, nil }

func (f *testSaramaClient) Close() error { return nil }

func (f *testSaramaClient) Config() *sarama.Config { return nil }

func (f *testSaramaClient) Controller() (*sarama.Broker, error) { return nil, nil }

func (f *testSaramaClient) RefreshController() (*sarama.Broker, error) { return nil, nil }

func (f *testSaramaClient) Brokers() []*sarama.Broker { return nil }

func (f *testSaramaClient) Broker(int32) (*sarama.Broker, error) { return nil, nil }

func (f *testSaramaClient) WritablePartitions(topic string) ([]int32, error) { return nil, nil }

func (f *testSaramaClient) Leader(topic string, partition int32) (*sarama.Broker, error) {
	return nil, nil
}

func (f *testSaramaClient) Replicas(
	topic string,
	partition int32,
) ([]int32, error) {
	return nil, nil
}

func (f *testSaramaClient) InSyncReplicas(topic string, partition int32) ([]int32, error) {
	return nil, nil
}

func (f *testSaramaClient) OfflineReplicas(topic string, partition int32) ([]int32, error) {
	return nil, nil
}

func (f *testSaramaClient) RefreshBrokers(addrs []string) error { return nil }

func (f *testSaramaClient) RefreshMetadata(topics ...string) error { return nil }

func (f *testSaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return 0, nil
}

func (f *testSaramaClient) RefreshCoordinator(consumerGroup string) error { return nil }

func (f *testSaramaClient) TransactionCoordinator(transactionID string) (*sarama.Broker, error) {
	return nil, nil
}

func (f *testSaramaClient) RefreshTransactionCoordinator(transactionID string) error { return nil }

func (f *testSaramaClient) InitProducerID() (*sarama.InitProducerIDResponse, error) { return nil, nil }

func (f *testSaramaClient) LeastLoadedBroker() *sarama.Broker { return nil }

func (f *testSaramaClient) PartitionNotReadable(string, int32) bool { return false }

func (f *testSaramaClient) Closed() bool { return false }

func (f *testSaramaClient) Coordinator(
	consumerGroup string,
) (*sarama.Broker, error) {
	return nil, nil
}

func (f *testSaramaClient) LeaderAndEpoch(
	topic string,
	partition int32,
) (*sarama.Broker, int32, error) {
	return nil, 0, nil
}

// testSaramaClientProvider is a minimal fake for SaramaClientProvider.
type testSaramaClientProvider struct {
	client *testSaramaClient
}

func (f *testSaramaClientProvider) Client(context.Context) (SaramaClient, error) {
	return f.client, nil
}

func (f *testSaramaClientProvider) Close() error { return nil }

// fakeLogSampler is a minimal fake for log.Sampler.
type fakeLogSampler struct{}

func (f *fakeLogSampler) IsSample() bool { return true }

// fakeWaiter is a minimal fake for libtime.WaiterDuration.
type fakeWaiter struct{}

func (f *fakeWaiter) Wait(context.Context, libtime.Duration) error { return nil }

func TestSkipAndAdvance_RecreatesPartitionConsumerAtAdvancedOffset(t *testing.T) {
	// This test verifies that skipAndAdvance:
	// 1. Creates a NEW partition consumer at the advanced (healthy) offset
	// 2. Returns the new consumer so the Consume loop can use it
	// 3. Increments the CorruptBatchSkipped metric once
	// It FAILS against the original broken implementation (which never recreates).

	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)
	healthyOffset := Offset(200)

	corruptPC, goodPC := setupPartitionConsumers(topic, partition, healthyOffset)

	var callCount int32
	var offsets []int64
	consumer := buildSkipTestConsumer(
		topic,
		partition,
		corruptOffset,
		healthyOffset,
		corruptPC,
		goodPC,
		&callCount,
		&offsets,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = consumer.Consume(ctx)
		close(done)
	}()

	waitForCalls(&callCount, 2)
	cancel()
	<-done

	assertConsumePartitionCalls(t, callCount, offsets, corruptOffset, healthyOffset)
	assertMetricIncremented(t, fakeMetricsForConsumer(consumer), 1)
}

func setupPartitionConsumers(
	topic Topic,
	partition Partition,
	healthyOffset Offset,
) (*fakePartitionConsumer, *fakePartitionConsumer) {
	corruptPC := &fakePartitionConsumer{
		errors: make(chan *sarama.ConsumerError, 10),
	}
	corruptPC.errors <- &sarama.ConsumerError{
		Topic:     topic.String(),
		Partition: partition.Int32(),
		Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
	}

	goodMsg := &sarama.ConsumerMessage{
		Topic:     topic.String(),
		Partition: partition.Int32(),
		Offset:    healthyOffset.Int64(),
	}
	goodPC := &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, 10),
	}
	goodPC.messages <- goodMsg
	return corruptPC, goodPC
}

func buildSkipTestConsumer(
	topic Topic,
	partition Partition,
	corruptOffset, healthyOffset Offset,
	corruptPC *fakePartitionConsumer,
	goodPC *fakePartitionConsumer,
	callCount *int32,
	offsets *[]int64,
) *offsetConsumer {
	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
			atomic.AddInt32(callCount, 1)
			*offsets = append(*offsets, offset)
			if atomic.LoadInt32(callCount) == 1 {
				return corruptPC, nil
			}
			return goodPC, nil
		},
	}

	return &offsetConsumer{
		saramaClientProvider: &testSaramaClientProvider{
			client: &testSaramaClient{partitions: []int32{partition.Int32()}},
		},
		topic: topic,
		offsetManager: &fakeOffsetManager{
			nextOffset:     corruptOffset,
			fallbackOffset: OffsetOldest,
		},
		messageHandlerBatch: &fakeMessageHandlerBatch{},
		metrics:             &fakeMetricsConsumer{},
		batchSize:           BatchSize(10),
		logSampler:          &fakeLogSampler{},
		consumerOptions:     ConsumerOptions{SkipCorruptBatches: true},
		skipper:             &fakeSkipper{healthyOffset: healthyOffset},
		saramaConsumerFunc:  func(sarama.Client) (sarama.Consumer, error) { return fakeConsumer, nil },
		waiter:              &fakeWaiter{},
	}
}

func fakeMetricsForConsumer(c *offsetConsumer) *fakeMetricsConsumer {
	m, ok := c.metrics.(*fakeMetricsConsumer)
	if !ok {
		panic("expected *fakeMetricsConsumer")
	}
	return m
}

func waitForCalls(count *int32, target int32) {
	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(count) >= target {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertConsumePartitionCalls(
	t *testing.T,
	callCount int32,
	offsets []int64,
	corruptOffset, healthyOffset Offset,
) {
	if callCount != 2 {
		t.Errorf("expected ConsumePartition called 2 times, got %d", callCount)
	}
	if len(offsets) != 2 {
		t.Errorf("expected 2 consumed offsets, got %d: %v", len(offsets), offsets)
	}
	if offsets[0] != corruptOffset.Int64() {
		t.Errorf("expected first at offset %d, got %d", corruptOffset.Int64(), offsets[0])
	}
	if offsets[1] != healthyOffset.Int64() {
		t.Errorf("expected second at healthy offset %d, got %d", healthyOffset.Int64(), offsets[1])
	}
}

func assertMetricIncremented(t *testing.T, m *fakeMetricsConsumer, expected int32) {
	if atomic.LoadInt32(&m.corruptBatchSkippedCount) != expected {
		t.Errorf(
			"expected CorruptBatchSkippedCounterInc called %d time(s), got %d",
			expected,
			m.corruptBatchSkippedCount,
		)
	}
}

// fakePartitionConsumer is a minimal in-package fake implementing
// sarama.PartitionConsumer for tests that exercise channel-close behaviour.
type fakePartitionConsumer struct {
	messages            chan *sarama.ConsumerMessage
	errors              chan *sarama.ConsumerError
	highWaterMarkOffset int64
}

func (f *fakePartitionConsumer) AsyncClose() {}

func (f *fakePartitionConsumer) Close() error { return nil }

func (f *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage { return f.messages }

func (f *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError { return f.errors }

func (f *fakePartitionConsumer) HighWaterMarkOffset() int64 {
	if f.highWaterMarkOffset != 0 {
		return f.highWaterMarkOffset
	}
	return 0
}

func (f *fakePartitionConsumer) Pause() {}

func (f *fakePartitionConsumer) Resume() {}

func (f *fakePartitionConsumer) IsPaused() bool { return false }

func newOffsetConsumerForTest(batchSize int) *offsetConsumer {
	return &offsetConsumer{
		batchSize:    BatchSize(batchSize),
		errorHandler: NewConsumerErrorHandler(NewMetrics()),
		metrics:      &fakeMetricsConsumer{},
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

// TestSkipAndAdvance_FindNextHealthyOffsetError tests that when FindNextHealthyOffset
// returns an error, skipAndAdvance propagates it without leaking the old consumer.
func TestSkipAndAdvance_FindNextHealthyOffsetError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)

	oldPC := &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, 10),
	}

	skipErr := stderrors.New("find next healthy offset failed")
	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(string, int32, int64) (sarama.PartitionConsumer, error) {
			return oldPC, nil
		},
	}

	c := &offsetConsumer{
		saramaClientProvider: &testSaramaClientProvider{
			client: &testSaramaClient{partitions: []int32{partition.Int32()}},
		},
		topic: topic,
		offsetManager: &fakeOffsetManager{
			nextOffset:     corruptOffset,
			fallbackOffset: OffsetOldest,
		},
		messageHandlerBatch: &fakeMessageHandlerBatch{},
		metrics:             &fakeMetricsConsumer{},
		batchSize:           BatchSize(10),
		logSampler:          &fakeLogSampler{},
		consumerOptions:     ConsumerOptions{SkipCorruptBatches: true},
		skipper:             &fakeSkipper{healthyOffset: 0, err: skipErr},
		saramaConsumerFunc:  func(sarama.Client) (sarama.Consumer, error) { return fakeConsumer, nil },
		waiter:              &fakeWaiter{},
	}

	newPC, _, err := c.skipAndAdvance(
		context.Background(),
		fakeConsumer,
		oldPC,
		partition,
		corruptOffset,
	)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if newPC != nil {
		t.Errorf("expected nil newPC on error, got %v", newPC)
	}
}

// TestSkipAndAdvance_CreatePartitionConsumerError tests that when CreatePartitionConsumer
// fails at the healthy offset, skipAndAdvance returns an error without closing the old consumer.
func TestSkipAndAdvance_CreatePartitionConsumerError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)
	healthyOffset := Offset(200)

	oldPC := &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, 10),
	}

	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
			if offset == healthyOffset.Int64() {
				return nil, stderrors.New("create partition consumer failed")
			}
			return oldPC, nil
		},
	}

	c := &offsetConsumer{
		saramaClientProvider: &testSaramaClientProvider{
			client: &testSaramaClient{partitions: []int32{partition.Int32()}},
		},
		topic: topic,
		offsetManager: &fakeOffsetManager{
			nextOffset:     corruptOffset,
			fallbackOffset: OffsetOldest,
		},
		messageHandlerBatch: &fakeMessageHandlerBatch{},
		metrics:             &fakeMetricsConsumer{},
		batchSize:           BatchSize(10),
		logSampler:          &fakeLogSampler{},
		consumerOptions:     ConsumerOptions{SkipCorruptBatches: true},
		skipper:             &fakeSkipper{healthyOffset: healthyOffset},
		saramaConsumerFunc:  func(sarama.Client) (sarama.Consumer, error) { return fakeConsumer, nil },
		waiter:              &fakeWaiter{},
	}

	newPC, _, err := c.skipAndAdvance(
		context.Background(),
		fakeConsumer,
		oldPC,
		partition,
		corruptOffset,
	)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if newPC != nil {
		t.Errorf("expected nil newPC on error, got %v", newPC)
	}
}

// TestSkipAndAdvance_NewOffsetNegative_UsesHighWaterMark tests that when FindNextHealthyOffset
// returns -1 (corruption to end), skipAndAdvance recreates the consumer at the high water mark.
func TestSkipAndAdvance_NewOffsetNegative_UsesHighWaterMark(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)
	highWaterMark := int64(500)

	oldPC := &fakePartitionConsumer{
		messages:            make(chan *sarama.ConsumerMessage, 10),
		highWaterMarkOffset: highWaterMark,
	}

	newPC := &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, 10),
	}

	var consumedOffsets []int64
	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
			consumedOffsets = append(consumedOffsets, offset)
			if offset == highWaterMark {
				return newPC, nil
			}
			return oldPC, nil
		},
	}

	c := &offsetConsumer{
		saramaClientProvider: &testSaramaClientProvider{
			client: &testSaramaClient{partitions: []int32{partition.Int32()}},
		},
		topic: topic,
		offsetManager: &fakeOffsetManager{
			nextOffset:     corruptOffset,
			fallbackOffset: OffsetOldest,
		},
		messageHandlerBatch: &fakeMessageHandlerBatch{},
		metrics:             &fakeMetricsConsumer{},
		batchSize:           BatchSize(10),
		logSampler:          &fakeLogSampler{},
		consumerOptions:     ConsumerOptions{SkipCorruptBatches: true},
		skipper:             &fakeSkipper{healthyOffset: Offset(-1)},
		saramaConsumerFunc:  func(sarama.Client) (sarama.Consumer, error) { return fakeConsumer, nil },
		waiter:              &fakeWaiter{},
	}

	resultPC, resultOffset, err := c.skipAndAdvance(
		context.Background(),
		fakeConsumer,
		oldPC,
		partition,
		corruptOffset,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resultPC == nil {
		t.Fatalf("expected non-nil newPC")
	}
	if resultOffset != Offset(highWaterMark) {
		t.Errorf("expected offset %d, got %d", highWaterMark, resultOffset)
	}
	// With fakeSkipper returning -1, skipAndAdvance uses HWM directly without probing
	// so only one ConsumePartition call happens (at HWM)
	if len(consumedOffsets) != 1 || consumedOffsets[0] != highWaterMark {
		t.Errorf("expected consumed offset [500], got %v", consumedOffsets)
	}
}

// TestDefaultCorruptionSkipper_IsOffsetGood tests isOffsetGood directly.
func TestDefaultCorruptionSkipper_IsOffsetGood(t *testing.T) {
	t.Run("good message returns true", testIsOffsetGoodGoodMessage)
	t.Run("corruption error returns false", testIsOffsetGoodCorruptionError)
	t.Run("non-corruption error propagates", testIsOffsetGoodNonCorruptionError)
}

func testIsOffsetGoodGoodMessage(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	pc := &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, 1),
	}
	pc.messages <- &sarama.ConsumerMessage{Topic: topic.String(), Partition: partition.Int32(), Offset: 100}

	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(string, int32, int64) (sarama.PartitionConsumer, error) { return pc, nil },
	}

	skipper := &defaultCorruptionSkipper{}
	good, err := skipper.isOffsetGood(
		context.Background(),
		fakeConsumer,
		topic,
		partition,
		Offset(100),
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !good {
		t.Errorf("expected good=true for valid message")
	}
}

func testIsOffsetGoodCorruptionError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	pc := &fakePartitionConsumer{
		errors: make(chan *sarama.ConsumerError, 1),
	}
	pc.errors <- &sarama.ConsumerError{
		Topic:     topic.String(),
		Partition: partition.Int32(),
		Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
	}

	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(string, int32, int64) (sarama.PartitionConsumer, error) { return pc, nil },
	}

	skipper := &defaultCorruptionSkipper{}
	good, err := skipper.isOffsetGood(
		context.Background(),
		fakeConsumer,
		topic,
		partition,
		Offset(100),
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if good {
		t.Errorf("expected good=false for corruption error")
	}
}

func testIsOffsetGoodNonCorruptionError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	innerErr := stderrors.New("some other error")
	pc := &fakePartitionConsumer{
		errors: make(chan *sarama.ConsumerError, 1),
	}
	pc.errors <- &sarama.ConsumerError{
		Topic:     topic.String(),
		Partition: partition.Int32(),
		Err:       innerErr,
	}

	fakeConsumer := &testSaramaConsumer{
		consumePartitionFn: func(string, int32, int64) (sarama.PartitionConsumer, error) { return pc, nil },
	}

	skipper := &defaultCorruptionSkipper{}
	_, err := skipper.isOffsetGood(
		context.Background(),
		fakeConsumer,
		topic,
		partition,
		Offset(100),
	)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
