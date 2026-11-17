// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	stderrors "errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/IBM/sarama"
)

type exclusiveTopicPartition struct {
	topic     string
	partition int32
}

// exclusiveSaramaConsumer is a sarama.Consumer fake that enforces the real client's
// one-open-consumer-per-topic/partition rule. ConsumePartition returns a
// ConfigurationError containing "That topic/partition is already being consumed" while a
// previous consumer for the same pair has not been closed.
//
// Probe behaviour is driven by offset sets so the real defaultCorruptionSkipper can run
// against it: offsets listed in healthyOffsets yield a buffered message, every other offset
// yields a buffered PacketDecodingError. failOffsets makes ConsumePartition itself return an
// error. Every partition consumer handed out is buffered, so a probe never blocks.
type exclusiveSaramaConsumer struct {
	mu             sync.Mutex
	open           map[exclusiveTopicPartition]bool
	offsets        []int64
	consumers      []*exclusivePartitionConsumer
	healthyOffsets map[int64]bool
	failOffsets    map[int64]error
	conflictCount  int32
}

func newExclusiveSaramaConsumer() *exclusiveSaramaConsumer {
	return &exclusiveSaramaConsumer{
		open:           make(map[exclusiveTopicPartition]bool),
		healthyOffsets: make(map[int64]bool),
		failOffsets:    make(map[int64]error),
	}
}

func (f *exclusiveSaramaConsumer) ConsumePartition(
	topic string,
	partition int32,
	offset int64,
) (sarama.PartitionConsumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.offsets = append(f.offsets, offset)

	key := exclusiveTopicPartition{topic: topic, partition: partition}
	if f.open[key] {
		atomic.AddInt32(&f.conflictCount, 1)
		return nil, sarama.ConfigurationError("That topic/partition is already being consumed")
	}
	if err, ok := f.failOffsets[offset]; ok {
		return nil, err
	}

	pc := &exclusivePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, 1),
		errors:   make(chan *sarama.ConsumerError, 1),
	}
	if f.healthyOffsets[offset] {
		pc.messages <- &sarama.ConsumerMessage{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		}
	} else {
		pc.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
		}
	}
	pc.onClose = func() {
		f.mu.Lock()
		delete(f.open, key)
		f.mu.Unlock()
	}
	f.open[key] = true
	f.consumers = append(f.consumers, pc)
	return pc, nil
}

func (f *exclusiveSaramaConsumer) Close() error { return nil }

func (f *exclusiveSaramaConsumer) Topics() ([]string, error) { return nil, nil }

func (f *exclusiveSaramaConsumer) Partitions(string) ([]int32, error) { return nil, nil }

func (f *exclusiveSaramaConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func (f *exclusiveSaramaConsumer) Pause(map[string][]int32) {}

func (f *exclusiveSaramaConsumer) PauseAll() {}

func (f *exclusiveSaramaConsumer) Resume(map[string][]int32) {}

func (f *exclusiveSaramaConsumer) ResumeAll() {}

func (f *exclusiveSaramaConsumer) openCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.open)
}

func (f *exclusiveSaramaConsumer) recordedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.offsets...)
}

func (f *exclusiveSaramaConsumer) consumer(index int) *exclusivePartitionConsumer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.consumers[index]
}

func (f *exclusiveSaramaConsumer) consumerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.consumers)
}

// exclusivePartitionConsumer releases its topic/partition pair on Close. Close is idempotent
// because Consume's deferred close runs again on a consumer a failed recovery already closed.
type exclusivePartitionConsumer struct {
	messages            chan *sarama.ConsumerMessage
	errors              chan *sarama.ConsumerError
	highWaterMarkOffset int64
	onClose             func()
	closeOnce           sync.Once
	closed              int32
}

func (f *exclusivePartitionConsumer) AsyncClose() { _ = f.Close() }

func (f *exclusivePartitionConsumer) Close() error {
	f.closeOnce.Do(func() {
		atomic.StoreInt32(&f.closed, 1)
		if f.onClose != nil {
			f.onClose()
		}
	})
	return nil
}

func (f *exclusivePartitionConsumer) isClosed() bool {
	return atomic.LoadInt32(&f.closed) == 1
}

func (f *exclusivePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return f.messages
}

func (f *exclusivePartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return f.errors
}

func (f *exclusivePartitionConsumer) HighWaterMarkOffset() int64 {
	return f.highWaterMarkOffset
}

func (f *exclusivePartitionConsumer) Pause() {}

func (f *exclusivePartitionConsumer) Resume() {}

func (f *exclusivePartitionConsumer) IsPaused() bool { return false }

// newExclusiveOffsetConsumer builds an offsetConsumer wired to an exclusiveSaramaConsumer.
func newExclusiveOffsetConsumer(
	fake *exclusiveSaramaConsumer,
	topic Topic,
	partition Partition,
	offsetManager *fakeOffsetManager,
	options ConsumerOptions,
	skipper corruptionSkipper,
) *offsetConsumer {
	return &offsetConsumer{
		saramaClientProvider: &testSaramaClientProvider{
			client: &testSaramaClient{partitions: []int32{partition.Int32()}},
		},
		topic:               topic,
		offsetManager:       offsetManager,
		messageHandlerBatch: &fakeMessageHandlerBatch{},
		metrics:             &fakeMetricsConsumer{},
		batchSize:           BatchSize(10),
		logSampler:          &fakeLogSampler{},
		consumerOptions:     options,
		skipper:             skipper,
		saramaConsumerFunc:  func(sarama.Client) (sarama.Consumer, error) { return fake, nil },
		waiter:              &fakeWaiter{},
	}
}

func openExclusiveConsumer(
	t *testing.T,
	fake *exclusiveSaramaConsumer,
	topic Topic,
	partition Partition,
	offset Offset,
) *exclusivePartitionConsumer {
	t.Helper()
	pc, err := fake.ConsumePartition(topic.String(), partition.Int32(), offset.Int64())
	if err != nil {
		t.Fatalf("open exclusive consumer at offset %d: %v", offset, err)
	}
	exclusivePC, ok := pc.(*exclusivePartitionConsumer)
	if !ok {
		t.Fatalf("expected *exclusivePartitionConsumer, got %T", pc)
	}
	return exclusivePC
}

// TestSkipAndAdvance_Exclusive_ClosesOldConsumerBeforeProbing runs skipAndAdvance against a
// sarama.Consumer fake that rejects a second open consumer for the same topic/partition. The
// pre-fix ordering probed and recreated the consumer while the old one was still open, so this
// test fails with "That topic/partition is already being consumed".
func TestSkipAndAdvance_Exclusive_ClosesOldConsumerBeforeProbing(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)
	healthyOffset := Offset(200)

	fake := newExclusiveSaramaConsumer()
	fake.healthyOffsets[healthyOffset.Int64()] = true

	oldPC := openExclusiveConsumer(t, fake, topic, partition, corruptOffset)
	oldPC.highWaterMarkOffset = 1000

	c := newExclusiveOffsetConsumer(
		fake,
		topic,
		partition,
		&fakeOffsetManager{nextOffset: corruptOffset, fallbackOffset: OffsetOldest},
		ConsumerOptions{SkipCorruptBatches: true},
		&defaultCorruptionSkipper{},
	)

	newPC, newOffset, err := c.skipAndAdvance(
		context.Background(),
		fake,
		oldPC,
		partition,
		corruptOffset,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newOffset != healthyOffset {
		t.Errorf("expected offset %d, got %d", healthyOffset, newOffset)
	}
	if newPC == nil {
		t.Fatalf("expected non-nil replacement consumer")
	}
	if !oldPC.isClosed() {
		t.Errorf("expected old partition consumer to be closed")
	}
	if got := atomic.LoadInt32(&fake.conflictCount); got != 0 {
		t.Errorf("expected no ConsumePartition conflicts, got %d", got)
	}
	if got := fake.openCount(); got != 1 {
		t.Errorf("expected exactly one open topic/partition pair, got %d", got)
	}
	assertMetricIncremented(t, fakeMetricsForConsumer(c), 1)

	if err := newPC.Close(); err != nil {
		t.Errorf("close replacement consumer: %v", err)
	}
}

// TestResetOnOffsetOutOfRange_Exclusive_ClosesOldConsumerBeforeReopening verifies the reset path
// releases the stuck consumer before opening the replacement on the same partition.
func TestResetOnOffsetOutOfRange_Exclusive_ClosesOldConsumerBeforeReopening(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	stuckOffset := Offset(100)
	fallbackOffset := OffsetOldest

	fake := newExclusiveSaramaConsumer()
	oldPC := openExclusiveConsumer(t, fake, topic, partition, stuckOffset)

	c := newExclusiveOffsetConsumer(
		fake,
		topic,
		partition,
		&fakeOffsetManager{fallbackOffset: fallbackOffset},
		ConsumerOptions{AutoResetOffsetOutOfRange: true},
		&fakeSkipper{},
	)

	newPC, newOffset, err := c.resetOnOffsetOutOfRange(
		context.Background(),
		fake,
		oldPC,
		partition,
		stuckOffset,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newOffset != fallbackOffset {
		t.Errorf("expected offset %d, got %d", fallbackOffset, newOffset)
	}
	if newPC == nil {
		t.Fatalf("expected non-nil replacement consumer")
	}
	if !oldPC.isClosed() {
		t.Errorf("expected old partition consumer to be closed")
	}
	if got := atomic.LoadInt32(&fake.conflictCount); got != 0 {
		t.Errorf("expected no ConsumePartition conflicts, got %d", got)
	}
	if got := fake.openCount(); got != 1 {
		t.Errorf("expected exactly one open topic/partition pair, got %d", got)
	}
	offsets := fake.recordedOffsets()
	if len(offsets) != 2 || offsets[0] != stuckOffset.Int64() ||
		offsets[1] != fallbackOffset.Int64() {
		t.Errorf(
			"expected offsets [%d %d], got %v",
			stuckOffset.Int64(),
			fallbackOffset.Int64(),
			offsets,
		)
	}

	if err := newPC.Close(); err != nil {
		t.Errorf("close replacement consumer: %v", err)
	}
}

func TestSkipAndAdvance_Exclusive_FindNextHealthyOffsetError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)

	fake := newExclusiveSaramaConsumer()
	oldPC := openExclusiveConsumer(t, fake, topic, partition, corruptOffset)

	c := newExclusiveOffsetConsumer(
		fake,
		topic,
		partition,
		&fakeOffsetManager{nextOffset: corruptOffset, fallbackOffset: OffsetOldest},
		ConsumerOptions{SkipCorruptBatches: true},
		&fakeSkipper{err: stderrors.New("find next healthy offset failed")},
	)

	newPC, _, err := c.skipAndAdvance(
		context.Background(),
		fake,
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
	if !oldPC.isClosed() {
		t.Errorf("expected old partition consumer to be closed on error")
	}
	if got := fake.openCount(); got != 0 {
		t.Errorf("expected no open topic/partition pair, got %d", got)
	}
}

func TestSkipAndAdvance_Exclusive_CreatePartitionConsumerError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)
	healthyOffset := Offset(200)

	fake := newExclusiveSaramaConsumer()
	fake.failOffsets[healthyOffset.Int64()] = stderrors.New("create partition consumer failed")
	oldPC := openExclusiveConsumer(t, fake, topic, partition, corruptOffset)

	c := newExclusiveOffsetConsumer(
		fake,
		topic,
		partition,
		&fakeOffsetManager{nextOffset: corruptOffset, fallbackOffset: OffsetOldest},
		ConsumerOptions{SkipCorruptBatches: true},
		&fakeSkipper{healthyOffset: healthyOffset},
	)

	newPC, _, err := c.skipAndAdvance(
		context.Background(),
		fake,
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
	if !oldPC.isClosed() {
		t.Errorf("expected old partition consumer to be closed on error")
	}
	if got := fake.openCount(); got != 0 {
		t.Errorf("expected no open topic/partition pair, got %d", got)
	}
}

func TestResetOnOffsetOutOfRange_Exclusive_ResetOffsetError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	stuckOffset := Offset(100)

	fake := newExclusiveSaramaConsumer()
	oldPC := openExclusiveConsumer(t, fake, topic, partition, stuckOffset)

	c := newExclusiveOffsetConsumer(
		fake,
		topic,
		partition,
		&fakeOffsetManager{
			fallbackOffset: OffsetOldest,
			resetOffsetErr: stderrors.New("reset offset failed"),
		},
		ConsumerOptions{AutoResetOffsetOutOfRange: true},
		&fakeSkipper{},
	)

	newPC, _, err := c.resetOnOffsetOutOfRange(
		context.Background(),
		fake,
		oldPC,
		partition,
		stuckOffset,
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if newPC != nil {
		t.Errorf("expected nil newPC on error, got %v", newPC)
	}
	if !oldPC.isClosed() {
		t.Errorf("expected old partition consumer to be closed on error")
	}
	if got := fake.openCount(); got != 0 {
		t.Errorf("expected no open topic/partition pair, got %d", got)
	}
}

func TestResetOnOffsetOutOfRange_Exclusive_CreatePartitionConsumerError(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	stuckOffset := Offset(100)
	fallbackOffset := OffsetOldest

	fake := newExclusiveSaramaConsumer()
	fake.failOffsets[fallbackOffset.Int64()] = stderrors.New("create partition consumer failed")
	oldPC := openExclusiveConsumer(t, fake, topic, partition, stuckOffset)

	c := newExclusiveOffsetConsumer(
		fake,
		topic,
		partition,
		&fakeOffsetManager{fallbackOffset: fallbackOffset},
		ConsumerOptions{AutoResetOffsetOutOfRange: true},
		&fakeSkipper{},
	)

	newPC, _, err := c.resetOnOffsetOutOfRange(
		context.Background(),
		fake,
		oldPC,
		partition,
		stuckOffset,
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if newPC != nil {
		t.Errorf("expected nil newPC on error, got %v", newPC)
	}
	if !oldPC.isClosed() {
		t.Errorf("expected old partition consumer to be closed on error")
	}
	if got := fake.openCount(); got != 0 {
		t.Errorf("expected no open topic/partition pair, got %d", got)
	}
}

// TestConsume_Exclusive_ReplacementClosedOnExit skips a corrupt batch, processes a message from
// the replacement consumer, then cancels the context. The replacement consumer must be closed by
// Consume's deferred close, leaving no open topic/partition pair.
func TestConsume_Exclusive_ReplacementClosedOnExit(t *testing.T) {
	topic := Topic("test-topic")
	partition := Partition(0)
	corruptOffset := Offset(100)
	healthyOffset := Offset(200)

	fake := newExclusiveSaramaConsumer()
	fake.healthyOffsets[healthyOffset.Int64()] = true

	offsetManager := &fakeOffsetManager{
		nextOffset:     corruptOffset,
		fallbackOffset: OffsetOldest,
	}
	c := newExclusiveOffsetConsumer(
		fake,
		topic,
		partition,
		offsetManager,
		ConsumerOptions{SkipCorruptBatches: true},
		&fakeSkipper{healthyOffset: healthyOffset},
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = c.Consume(ctx)
		close(done)
	}()

	waitForCalls(&offsetManager.markOffsetCalled, 1)
	cancel()
	<-done

	if got := fake.consumerCount(); got != 2 {
		t.Fatalf("expected 2 partition consumers, got %d", got)
	}
	if !fake.consumer(0).isClosed() {
		t.Errorf("expected the corrupt consumer to be closed")
	}
	if !fake.consumer(1).isClosed() {
		t.Errorf("expected the replacement consumer to be closed on exit")
	}
	if got := fake.openCount(); got != 0 {
		t.Errorf("expected no open topic/partition pair after exit, got %d", got)
	}
}
