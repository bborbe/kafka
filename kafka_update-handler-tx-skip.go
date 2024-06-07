// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	libkv "github.com/bborbe/kv"
	"github.com/bborbe/log"
	"github.com/golang/glog"
)

func NewUpdaterHandlerTxSkipErrors[KEY ~[]byte | ~string, OBJECT any](
	handler UpdaterHandlerTx[KEY, OBJECT],
	logSamplerFactory log.SamplerFactory,
) UpdaterHandlerTx[KEY, OBJECT] {
	logSampler := logSamplerFactory.Sampler()
	return UpdaterHandlerTxFunc[KEY, OBJECT](
		func(ctx context.Context, tx libkv.Tx, key KEY, object OBJECT) error {
			if err := handler.Update(ctx, tx, key, object); err != nil {
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
		func(ctx context.Context, tx libkv.Tx, key KEY) error {
			if err := handler.Delete(ctx, tx, key); err != nil {
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
