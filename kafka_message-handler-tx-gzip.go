// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
)

// NewGzipMessageHandlerTx returns a MessageHandlerTx that gunzips msg.Value
// when the GzipHeaderKey / GzipHeaderValue marker is present, then delegates
// to the inner handler. The gzip header is stripped from msg.Headers after a
// successful decompression so downstream handlers don't see a stale marker.
// Empty values and messages without the gzip marker pass through unmodified.
//
// Use this on the consumer side when the producer wraps with
// NewSyncProducerGzipValue — the on-the-wire header convention is identical
// to seibert-data/lib-kafka.
func NewGzipMessageHandlerTx(
	inner MessageHandlerTx,
) MessageHandlerTx {
	return MessageHandlerTxFunc(
		func(ctx context.Context, tx libkv.Tx, msg *sarama.ConsumerMessage) error {
			if len(msg.Value) > 0 && GzipActive(msg.Headers) {
				decompressed, err := GzipDecoder(ctx, msg.Value)
				if err != nil {
					return errors.Wrapf(ctx, err, "gzip decompress failed")
				}
				msg.Value = decompressed
				msg.Headers = RemoveGzipHeader(msg.Headers)
			}
			return inner.ConsumeMessage(ctx, tx, msg)
		},
	)
}
