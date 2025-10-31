// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/bborbe/log"
	"github.com/bborbe/parse"
	"github.com/bborbe/validation"
	"github.com/golang/glog"
)

// Entries represents a collection of Entry items for batch operations.
type Entries []Entry

// Entry represents a single Kafka message entry with topic, headers, key and value.
type Entry struct {
	Topic   Topic                 `json:"topic"`
	Headers []sarama.RecordHeader `json:"headers"`
	Key     Key                   `json:"key"`
	Value   Value                 `json:"value"`
}

// Keys represents a collection of Key items.
type Keys []Key

// Key represents a Kafka message key that can be converted to bytes or string.
type Key interface {
	Bytes() []byte
	String() string
}

type key []byte

func (f key) String() string {
	return string(f)
}

func (f key) Bytes() []byte {
	return f
}

// ParseKey parses an interface value into a Key by converting it to a string.
func ParseKey(ctx context.Context, value interface{}) (*Key, error) {
	str, err := parse.ParseString(ctx, value)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "parse value as string failed")
	}
	key := NewKey(str)
	return &key, nil
}

// NewKey creates a new Key from a byte slice or string value.
func NewKey[K ~[]byte | ~string](value K) Key {
	return key(value)
}

// Value represents a Kafka message value that supports validation.
type Value interface {
	validation.HasValidation
}

// JSONSenderOptions defines configuration options for JSONSender behavior.
type JSONSenderOptions struct {
	ValidationDisabled bool
}

// JsonSenderOptions defines configuration options for JsonSender behavior.
//
// Deprecated: Use JSONSenderOptions instead.
//
//nolint:revive
type JsonSenderOptions = JSONSenderOptions

//counterfeiter:generate -o mocks/kafka-json-sender.go --fake-name KafkaJSONSender . JSONSender

// JSONSender provides methods for sending JSON-encoded messages to Kafka topics.
type JSONSender interface {
	SendUpdate(
		ctx context.Context,
		topic Topic,
		key Key,
		value Value,
		headers ...sarama.RecordHeader,
	) error
	SendUpdates(ctx context.Context, entries Entries) error
	SendDelete(ctx context.Context, topic Topic, key Key, headers ...sarama.RecordHeader) error
	SendDeletes(ctx context.Context, entries Entries) error
}

// JsonSender provides methods for sending JSON-encoded messages to Kafka topics.
//
// Deprecated: Use JSONSender instead.
//
//nolint:revive
type JsonSender = JSONSender

// NewJSONSender creates a new JSONSender with the provided producer and options.
func NewJSONSender(
	producer SyncProducer,
	logSamplerFactory log.SamplerFactory,
	optionsFns ...func(options *JSONSenderOptions),
) JSONSender {
	options := JSONSenderOptions{}
	for _, fn := range optionsFns {
		fn(&options)
	}
	return &jsonSender{
		producer:         producer,
		options:          options,
		logSamplerUpdate: logSamplerFactory.Sampler(),
		logSamplerDelete: logSamplerFactory.Sampler(),
	}
}

// NewJsonSender creates a new JSONSender with the provided producer and options.
//
// Deprecated: Use NewJSONSender instead.
//
//nolint:revive
func NewJsonSender(
	producer SyncProducer,
	logSamplerFactory log.SamplerFactory,
	optionsFns ...func(options *JSONSenderOptions),
) JSONSender {
	return NewJSONSender(producer, logSamplerFactory, optionsFns...)
}

type jsonSender struct {
	producer         SyncProducer
	logSamplerUpdate log.Sampler
	logSamplerDelete log.Sampler
	options          JSONSenderOptions
}

func (j *jsonSender) SendUpdate(
	ctx context.Context,
	topic Topic,
	key Key,
	value Value,
	headers ...sarama.RecordHeader,
) error {
	glog.V(4).Infof("sendUpdate %s to %s started", key, topic)

	if glog.V(4) {
		v, _ := json.Marshal(value)
		glog.Infof(
			"send update message to %s key %s value %s",
			topic,
			string(key.Bytes()),
			string(v),
		)
	}

	msg, err := j.createUpdateMessage(ctx, topic, key, value, headers...)
	if err != nil {
		return errors.Wrapf(ctx, err, "create update message failed")
	}

	partition, offset, err := j.producer.SendMessage(ctx, msg)
	if err != nil {
		return errors.Wrapf(ctx, err, "send update message failed")
	}
	if j.logSamplerUpdate.IsSample() {
		glog.V(3).
			Infof("send update message successful to %s with partition %d offset %d (sample)", topic, partition, offset)
	}
	glog.V(4).Infof("sendUpdate %s to %s completed", key, topic)
	return nil
}

func (j *jsonSender) SendUpdates(ctx context.Context, entries Entries) error {
	glog.V(4).Infof("send %d updates started", len(entries))
	msgs := make([]*sarama.ProducerMessage, len(entries))
	var err error
	for i, entry := range entries {
		msgs[i], err = j.createUpdateMessage(
			ctx,
			entry.Topic,
			entry.Key,
			entry.Value,
			entry.Headers...)
		if err != nil {
			return errors.Wrapf(ctx, err, "create update message failed")
		}
	}
	if err := j.producer.SendMessages(ctx, msgs); err != nil {
		return errors.Wrapf(ctx, err, "send update message failed")
	}
	if j.logSamplerDelete.IsSample() {
		glog.V(3).Infof("send %d update messages successful (sample)", len(entries))
	}
	glog.V(4).Infof("send %d updates completed", len(entries))
	return nil
}

func (j *jsonSender) createUpdateMessage(
	ctx context.Context,
	topic Topic,
	key Key,
	value Value,
	headers ...sarama.RecordHeader,
) (*sarama.ProducerMessage, error) {
	if err := j.validateValue(ctx, value); err != nil {
		if glog.V(4) {
			content := &bytes.Buffer{}
			encoder := json.NewEncoder(content)
			encoder.SetIndent("", "   ")
			_ = encoder.Encode(value)
			glog.Infof(
				"validate value failed for topic %s with key %s and value %s",
				topic,
				key,
				content.String(),
			)
		}
		return nil, errors.Wrapf(
			ctx,
			err,
			"validate value failed for topic %s with key %s failed",
			topic,
			key,
		)
	}

	valueEncoder, err := NewJSONEncoder(ctx, value)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "encode value failed")
	}

	return &sarama.ProducerMessage{
		Headers: headers,
		Topic:   topic.String(),
		Key:     sarama.ByteEncoder(key.Bytes()),
		Value:   valueEncoder,
	}, nil
}

func (j *jsonSender) SendDelete(
	ctx context.Context,
	topic Topic,
	key Key,
	headers ...sarama.RecordHeader,
) error {
	glog.V(3).Infof("SendDelete %s to %s started", key, topic)

	if glog.V(4) {
		glog.Infof("send delete message to %s key %s", topic, string(key.Bytes()))
	}

	partition, offset, err := j.producer.SendMessage(
		ctx,
		j.createDeleteMessage(topic, key, headers),
	)
	if err != nil {
		return errors.Wrapf(ctx, err, "send delete message failed")
	}
	if j.logSamplerDelete.IsSample() {
		glog.V(3).
			Infof("send delete message to %s with partition %d offset %d successful (sample)", topic, partition, offset)
	}
	return nil
}

func (j *jsonSender) SendDeletes(ctx context.Context, entries Entries) error {
	glog.V(3).Infof("send %d deletes started", len(entries))

	msgs := make([]*sarama.ProducerMessage, len(entries))
	for i, entry := range entries {
		msgs[i] = j.createDeleteMessage(entry.Topic, entry.Key, entry.Headers)
	}
	if err := j.producer.SendMessages(ctx, msgs); err != nil {
		return errors.Wrapf(ctx, err, "send delete messages failed")
	}
	if j.logSamplerDelete.IsSample() {
		glog.V(3).Infof("send %d deletes messages successful (sample)", len(entries))
	}
	return nil
}

func (j *jsonSender) createDeleteMessage(
	topic Topic,
	key Key,
	headers []sarama.RecordHeader,
) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Headers: headers,
		Topic:   topic.String(),
		Key:     sarama.ByteEncoder(key.Bytes()),
	}
}

func (j *jsonSender) validateValue(ctx context.Context, value Value) error {
	if j.options.ValidationDisabled {
		return nil
	}
	return value.Validate(ctx)
}
