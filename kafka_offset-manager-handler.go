// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"net/http"

	"github.com/bborbe/errors"
	libhttp "github.com/bborbe/http"
	"github.com/golang/glog"
)

// NewOffsetManagerHandler creates an HTTP handler for managing Kafka consumer offsets.
func NewOffsetManagerHandler(
	offsetManager OffsetManager,
	cancel context.CancelFunc,
) libhttp.WithError {
	return libhttp.WithErrorFunc(
		func(ctx context.Context, resp http.ResponseWriter, req *http.Request) error {
			partition, err := ParsePartition(ctx, req.FormValue("partition"))
			if err != nil {
				return errors.Wrapf(ctx, err, "parse partition failed")
			}
			topic := Topic(req.FormValue("topic"))
			if topic == "" {
				return errors.Errorf(ctx, "parameter topic missing")
			}
			offset, err := ParseOffset(ctx, req.FormValue("offset"))
			if err != nil {
				offset, err := offsetManager.NextOffset(ctx, topic, *partition)
				if err != nil {
					return errors.Wrapf(ctx, err, "get offset failed")
				}
				libhttp.WriteAndGlog(
					resp,
					"next offset is %s for topic(%s) and partition(%s)",
					offset,
					topic,
					partition,
				)
				return nil
			}
			if offset.Int64() < 0 {
				glog.V(2).Infof("offset is negative: %d", offset)
				highWaterMark, err := offsetManager.NextOffset(ctx, topic, *partition)
				if err != nil {
					return errors.Wrapf(ctx, err, "get offset failed")
				}
				glog.V(2).Infof("highWaterMark: %s", highWaterMark)
				newOffset := Offset(offset.Int64() + highWaterMark.Int64())
				glog.V(2).Infof("offset(%d) < 0 => use %d", offset.Int64(), newOffset)
				offset = newOffset.Ptr()
			}
			if err := offsetManager.MarkOffset(ctx, topic, *partition, *offset); err != nil {
				return errors.Wrapf(ctx, err, "set offset failed")
			}
			_ = offsetManager.Close()
			defer cancel()
			libhttp.WriteAndGlog(
				resp,
				"set offset(%d) for topic(%s) and partition(%s) completed",
				offset.Int64(),
				topic,
				partition,
			)

			return nil
		},
	)
}
