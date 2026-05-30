// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	stderrors "errors"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

// ConsumerOptions configures optional parameters for offset consumers.
type ConsumerOptions struct {
	TargetLag          int64
	Delay              libtime.Duration
	SkipCorruptBatches bool
}

// WithSkipCorruptBatches enables skipping corrupt record batches (CRC decode failures) and resuming at the next healthy offset.
func WithSkipCorruptBatches(skip bool) func(*ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.SkipCorruptBatches = skip
	}
}

// IsCorruptionError reports whether err indicates Kafka message corruption (CRC mismatch / decode failure).
func IsCorruptionError(err error) bool {
	if err == nil {
		return false
	}
	var packetErr sarama.PacketDecodingError
	if errors.As(err, &packetErr) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "CRC") ||
		strings.Contains(msg, "CorruptRecord") ||
		strings.Contains(msg, "message contents does not match")
}

// corruptionSkipper handles finding the next healthy offset after detecting corruption.
type corruptionSkipper interface {
	FindNextHealthyOffset(
		ctx context.Context,
		consumer sarama.Consumer,
		topic Topic,
		partition Partition,
		corruptOffset Offset,
		maxOffset int64,
	) (Offset, error)
}

// errSkipCorruptBatch is a sentinel error indicating a corrupt batch was detected and skipped.
var errSkipCorruptBatch = stderrors.New("skip corrupt batch")

const probeTimeout = 5 * time.Second

// defaultCorruptionSkipper implements corruptionSkipper using exponential probe + binary search.
type defaultCorruptionSkipper struct{}

// FindNextHealthyOffset finds the first good offset after the corrupt range using exponential probe + binary search.
func (s *defaultCorruptionSkipper) FindNextHealthyOffset(
	ctx context.Context,
	consumer sarama.Consumer,
	topic Topic,
	partition Partition,
	corruptOffset Offset,
	maxOffset int64,
) (Offset, error) {
	jumps := []int64{10, 100, 1000, 10000, 100000}

	for _, jump := range jumps {
		candidate := corruptOffset.Int64() + jump
		if candidate >= maxOffset {
			return Offset(-1), nil
		}
		good, err := s.isOffsetGood(ctx, consumer, topic, partition, Offset(candidate))
		if err != nil {
			return Offset(-1), err
		}
		if good {
			result, err := s.binarySearchEndOfCorruption(
				ctx,
				corruptOffset.Int64(),
				candidate,
				consumer,
				topic,
				partition,
			)
			if err != nil {
				return Offset(-1), err
			}
			return Offset(result), nil
		}
	}

	return Offset(-1), nil
}

func (s *defaultCorruptionSkipper) binarySearchEndOfCorruption(
	ctx context.Context,
	corruptOffset int64,
	goodOffset int64,
	consumer sarama.Consumer,
	topic Topic,
	partition Partition,
) (int64, error) {
	low := corruptOffset + 1
	high := goodOffset

	for low < high {
		mid := (low + high) / 2
		good, err := s.isOffsetGood(ctx, consumer, topic, partition, Offset(mid))
		if err != nil || !good {
			low = mid + 1
		} else {
			high = mid
		}
	}
	good, err := s.isOffsetGood(ctx, consumer, topic, partition, Offset(low))
	if err != nil {
		return -1, err
	}
	if !good {
		return -1, nil
	}
	return low, nil
}

func (s *defaultCorruptionSkipper) isOffsetGood(
	ctx context.Context,
	consumer sarama.Consumer,
	topic Topic,
	partition Partition,
	offset Offset,
) (bool, error) {
	pc, err := consumer.ConsumePartition(topic.String(), partition.Int32(), offset.Int64())
	if err != nil {
		if IsCorruptionError(err) {
			return false, nil
		}
		return false, err
	}
	defer pc.Close()

	timer := time.NewTimer(probeTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
		return false, nil
	case msg, ok := <-pc.Messages():
		if !ok {
			return false, nil
		}
		_ = msg
		return true, nil
	case err, ok := <-pc.Errors():
		if !ok {
			return false, nil
		}
		if IsCorruptionError(err.Err) {
			return false, nil
		}
		return false, err.Err
	}
}

// NewOffsetConsumer creates a new offset-based consumer that processes messages one at a time.
func NewOffsetConsumer(
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
	messageHandler MessageHandler,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	saramaClientProvider := NewSaramaClientProviderExisting(saramaClient)
	return NewOffsetConsumerWithProvider(
		saramaClientProvider,
		topic,
		offsetManager,
		messageHandler,
		logSamplerFactory,
		options...,
	)
}

// NewOffsetConsumerWithProvider creates a new offset-based consumer that processes messages one at a time.
func NewOffsetConsumerWithProvider(
	saramaClientProvider SaramaClientProvider,
	topic Topic,
	offsetManager OffsetManager,
	messageHandler MessageHandler,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	return NewOffsetConsumerBatchWithProvider(
		saramaClientProvider,
		topic,
		offsetManager,
		NewMessageHandlerBatch(messageHandler),
		1,
		logSamplerFactory,
		options...,
	)
}

// NewOffsetConsumerBatch creates a new offset-based consumer that processes messages in batches.
func NewOffsetConsumerBatch(
	saramaClient sarama.Client,
	topic Topic,
	offsetManager OffsetManager,
	messageHandlerBatch MessageHandlerBatch,
	batchSize BatchSize,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	saramaClientProvider := NewSaramaClientProviderExisting(saramaClient)
	return NewOffsetConsumerBatchWithProvider(
		saramaClientProvider,
		topic,
		offsetManager,
		messageHandlerBatch,
		batchSize,
		logSamplerFactory,
		options...,
	)
}

// NewOffsetConsumerBatchWithProvider creates a new offset-based consumer that processes messages in batches.
func NewOffsetConsumerBatchWithProvider(
	saramaClientProvider SaramaClientProvider,
	topic Topic,
	offsetManager OffsetManager,
	messageHandlerBatch MessageHandlerBatch,
	batchSize BatchSize,
	logSamplerFactory log.SamplerFactory,
	options ...func(*ConsumerOptions),
) Consumer {
	consumerOptions := ConsumerOptions{
		TargetLag: 0,
		Delay:     0,
	}
	for _, option := range options {
		option(&consumerOptions)
	}
	return &offsetConsumer{
		batchSize:            batchSize,
		saramaClientProvider: saramaClientProvider,
		offsetManager:        offsetManager,
		messageHandlerBatch:  messageHandlerBatch,
		topic:                topic,
		logSampler:           logSamplerFactory.Sampler(),
		consumerOptions:      consumerOptions,
		waiter:               libtime.NewWaiterDuration(),
		metrics:              NewMetrics(),
		errorHandler: NewConsumerErrorHandler(
			NewMetrics(),
		),
		skipper:            &defaultCorruptionSkipper{},
		saramaConsumerFunc: sarama.NewConsumerFromClient,
	}
}

type offsetConsumer struct {
	saramaClientProvider SaramaClientProvider
	topic                Topic
	offsetManager        OffsetManager
	messageHandlerBatch  MessageHandlerBatch
	batchSize            BatchSize
	logSampler           log.Sampler
	metrics              interface {
		MetricsConsumer
		MetricsPartitionConsumer
	}
	waiter             libtime.WaiterDuration
	consumerOptions    ConsumerOptions
	errorHandler       ConsumerErrorHandler
	skipper            corruptionSkipper
	saramaConsumerFunc func(sarama.Client) (sarama.Consumer, error)
}

//nolint:gocognit,funlen // Complex consumer logic, refactoring would reduce readability
func (c *offsetConsumer) Consume(ctx context.Context) error {
	saramaClient, err := c.saramaClientProvider.Client(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "get saramaClient from saramaClientProvider failed")
	}

	consumerFromClient, err := c.saramaConsumerFunc(saramaClient)
	if err != nil {
		return errors.Wrapf(ctx, err, "create consumer failed")
	}
	defer consumerFromClient.Close()

	partitions, err := saramaClient.Partitions(c.topic.String())
	if err != nil {
		return errors.Wrapf(ctx, err, "get partition for topic %s failed", c.topic)
	}

	glog.V(2).
		Infof("consume topic %s with %d partitions %+v started", c.topic, len(partitions), c.consumerOptions)

	runs := make([]run.Func, 0, len(partitions))
	for _, partition := range partitions {
		runs = append(runs, func(ctx context.Context) error {

			nextOffset, err := c.offsetManager.NextOffset(ctx, c.topic, Partition(partition))
			if err != nil {
				return errors.Wrapf(
					ctx,
					err,
					"get next offset  topic(%s) with partition(%d) failed",
					c.topic,
					Partition(partition),
				)
			}

			glog.V(2).
				Infof("consume topic(%s) with partition(%d) and offset(%s) started", c.topic, partition, nextOffset)
			consumePartition, err := CreatePartitionConsumer(
				ctx,
				consumerFromClient,
				c.metrics,
				c.topic,
				Partition(partition),
				c.offsetManager.FallbackOffset(),
				nextOffset,
			)
			if err != nil {
				return errors.Wrapf(
					ctx,
					err,
					"create partition consumer for topic(%s) with partition(%d) and offset(%s) failed",
					c.topic,
					partition,
					nextOffset,
				)
			}
			defer consumePartition.Close()
			for {
				messages, err := c.consumeMessages(ctx, consumePartition)
				if err != nil {
					if c.consumerOptions.SkipCorruptBatches && errors.Is(err, errSkipCorruptBatch) {
						newPC, newOff, skipErr := c.skipAndAdvance(
							ctx,
							consumerFromClient,
							consumePartition,
							Partition(partition),
							nextOffset,
						)
						if skipErr != nil {
							return errors.Wrapf(ctx, skipErr, "skip and advance failed")
						}
						consumePartition = newPC
						nextOffset = newOff
						continue
					}
					return errors.Wrapf(ctx, err, "consume failed")
				}
				msg := messages[len(messages)-1]
				glog.V(4).
					Infof("consume %d messages in topic %s with offset %d partition %d started", len(messages), msg.Topic, msg.Offset, msg.Partition)

				highWaterMarketOffset := consumePartition.HighWaterMarkOffset()
				lag := highWaterMarketOffset - msg.Offset

				c.metrics.CurrentOffset(c.topic, Partition(partition), Offset(msg.Offset))
				c.metrics.HighWaterMarkOffset(
					c.topic,
					Partition(partition),
					Offset(highWaterMarketOffset),
				)

				if err := c.messageHandlerBatch.ConsumeMessages(ctx, messages); err != nil {
					return errors.Wrapf(ctx, err, "consume message failed")
				}
				nextOffset = Offset(msg.Offset + 1)
				if err := c.offsetManager.MarkOffset(ctx, c.topic, Partition(partition), Offset(nextOffset)); err != nil {
					return errors.Wrapf(ctx, err, "mark offset failed")
				}

				// wait if lag is low, this allow batch consumer get more messages next time
				if lag < c.consumerOptions.TargetLag && c.consumerOptions.TargetLag > 0 &&
					c.consumerOptions.Delay > 0 {
					glog.V(4).
						Infof("topic(%s) partition(%d) lag(%d) < targetLag(%d) => wait for %v", c.topic, partition, lag, c.consumerOptions.TargetLag, c.consumerOptions.Delay)
					if err := c.waiter.Wait(ctx, c.consumerOptions.Delay); err != nil {
						return errors.Wrapf(
							ctx,
							err,
							"topic(%s) partition(%d) wait for %v failed",
							c.topic,
							partition,
							c.consumerOptions.Delay,
						)
					}
					glog.V(4).
						Infof("topic(%s) partition(%d) wait for %v completed", c.topic, partition, c.consumerOptions.Delay)
				}

				if c.logSampler.IsSample() {
					glog.V(2).
						Infof("consume %d messages in topic(%s), partition(%d) and offset(%d) completed (highwatermark: %d lag: %d) (sample)",
							len(messages),
							msg.Topic,
							msg.Partition,
							msg.Offset,
							highWaterMarketOffset,
							lag,
						)
				}
			}
		})
	}

	if err := run.CancelOnFirstError(ctx, runs...); err != nil {
		return errors.Wrapf(ctx, err, "run failed")
	}
	glog.V(2).Infof("consume topic(%s) with %d partitions completed", c.topic, len(partitions))
	return nil
}

// consume
//
//nolint:gocognit // Channel-close detection adds branches; refactoring would reduce readability
func (c *offsetConsumer) consumeMessages(
	ctx context.Context,
	consumePartition sarama.PartitionConsumer,
) ([]*sarama.ConsumerMessage, error) {
	var result []*sarama.ConsumerMessage

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err, ok := <-consumePartition.Errors():
		if !ok {
			return nil, errors.Errorf(ctx, "partition consumer errors channel closed")
		}
		if c.consumerOptions.SkipCorruptBatches && IsCorruptionError(err.Err) {
			if len(result) > 0 {
				return result, nil
			}
			return nil, errSkipCorruptBatch
		}
		if err := c.errorHandler.HandleError(err); err != nil {
			return nil, errors.Wrapf(ctx, err, "parition consumer returns error")
		}
	case msg, ok := <-consumePartition.Messages():
		if !ok {
			return nil, errors.Errorf(ctx, "partition consumer messages channel closed")
		}
		result = append(result, msg)
	}

	for {
		if len(result) == c.batchSize.Int() {
			glog.V(4).Infof("reached batch size => return %d messages", len(result))
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err, ok := <-consumePartition.Errors():
			if !ok {
				return nil, errors.Errorf(ctx, "partition consumer errors channel closed")
			}
			if c.consumerOptions.SkipCorruptBatches && IsCorruptionError(err.Err) {
				if len(result) > 0 {
					return result, nil
				}
				return nil, errSkipCorruptBatch
			}
			if err := c.errorHandler.HandleError(err); err != nil {
				return nil, errors.Wrapf(ctx, err, "parition consumer returns error")
			}
		case msg, ok := <-consumePartition.Messages():
			if !ok {
				return nil, errors.Errorf(ctx, "partition consumer messages channel closed")
			}
			result = append(result, msg)
		default:
			glog.V(4).Infof("no more messages => return %d messages", len(result))
			return result, nil
		}
	}
}

// skipAndAdvance closes the current partition consumer, finds the next healthy offset, and recreates the consumer at that offset.
func (c *offsetConsumer) skipAndAdvance(
	ctx context.Context,
	consumer sarama.Consumer,
	oldPartitionConsumer sarama.PartitionConsumer,
	partition Partition,
	corruptOffset Offset,
) (sarama.PartitionConsumer, Offset, error) {
	highWaterMark := oldPartitionConsumer.HighWaterMarkOffset()

	glog.V(1).Infof(
		"detected corrupt batch at offset %s in topic %s partition %d, searching for next healthy offset",
		corruptOffset,
		c.topic,
		partition,
	)

	newOffset, err := c.skipper.FindNextHealthyOffset(
		ctx,
		consumer,
		c.topic,
		partition,
		corruptOffset,
		highWaterMark,
	)
	if err != nil {
		return nil, Offset(0), errors.Wrapf(ctx, err, "find next healthy offset failed")
	}

	if newOffset < 0 {
		glog.V(1).Infof(
			"corrupt range extends to end of partition %s (topic %s, partition %d), resuming at high water mark %d",
			c.topic,
			partition,
			highWaterMark,
		)
		newOffset = Offset(highWaterMark)
	}

	newPC, err := CreatePartitionConsumer(
		ctx,
		consumer,
		c.metrics,
		c.topic,
		partition,
		c.offsetManager.FallbackOffset(),
		newOffset,
	)
	if err != nil {
		return nil, Offset(
				0,
			), errors.Wrapf(
				ctx,
				err,
				"create partition consumer at offset %s failed",
				newOffset,
			)
	}

	if err := oldPartitionConsumer.Close(); err != nil {
		glog.V(4).Infof("closing old partition consumer returned error: %v", err)
	}

	c.metrics.CorruptBatchSkippedCounterInc(c.topic, partition)

	glog.V(1).Infof(
		"skipped corrupt batch in topic %s partition %d: range %s -> %s",
		c.topic,
		partition,
		corruptOffset,
		newOffset,
	)
	return newPC, newOffset, nil
}
