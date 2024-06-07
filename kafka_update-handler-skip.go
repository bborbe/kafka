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

func NewUpdaterHandlerSkipErrors[KEY ~[]byte | ~string, OBJECT any](
	handler UpdaterHandler[KEY, OBJECT],
	logSamplerFactory log.SamplerFactory,
) UpdaterHandler[KEY, OBJECT] {
	logSampler := logSamplerFactory.Sampler()
	return UpdaterHandlerFunc[KEY, OBJECT](
		func(ctx context.Context, key KEY, object OBJECT) error {
			if err := handler.Update(ctx, key, object); err != nil {
				if logSampler.IsSample() {
					data := errors.DataFromError(
						errors.AddDataToError(
							err,
							map[string]string{
								"key": string(key),
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
								"key": string(key),
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
