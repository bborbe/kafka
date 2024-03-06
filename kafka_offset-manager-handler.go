// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"net/http"

	"github.com/bborbe/errors"
	libhttp "github.com/bborbe/http"
)

func NewOffsetManagerHandler(offsetManager OffsetManager, cancel context.CancelFunc) libhttp.WithError {
	return libhttp.WithErrorFunc(func(ctx context.Context, resp http.ResponseWriter, req *http.Request) error {
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
			libhttp.WriteAndGlog(resp, "next offset is %s", offset)
			return nil
		}
		if err := offsetManager.MarkOffset(ctx, topic, *partition, *offset); err != nil {
			return errors.Wrapf(ctx, err, "set offset failed")
		}
		defer cancel()
		libhttp.WriteAndGlog(resp, "set offset completed")
		return nil
	})
}
