// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	"github.com/bborbe/log"
	"github.com/golang/glog"
)

func NewUpdaterHandlerSkipErrors[OBJECT any, KEY Key](
	handler UpdaterHandler[OBJECT, KEY],
	logSamplerFactory log.SamplerFactory,
) UpdaterHandler[OBJECT, KEY] {
	logSampler := logSamplerFactory.Sampler()
	return UpdaterHandlerFunc[OBJECT, KEY](
		func(ctx context.Context, key KEY, object OBJECT) error {
			if err := handler.Update(ctx, key, object); err != nil {
				if logSampler.IsSample() {
					data := errors.DataFromError(
						errors.AddDataToError(
							err,
							map[string]string{
								"key": key.String(),
							},
						),
					)
					glog.Warningf("update %s failed: %v %+v (sample)", key, err, data)
				}
			}
			return nil
		},
		func(ctx context.Context, key KEY) error {
			if err := handler.Delete(ctx, key); err != nil {
				if logSampler.IsSample() {
					data := errors.DataFromError(
						errors.AddDataToError(
							err,
							map[string]string{
								"key": key.String(),
							},
						),
					)
					glog.Warningf("update %s failed: %v %+v (sample)", key, err, data)
				}
			}
			return nil
		},
	)
}
