// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kv

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/bborbe/errors"
	libhttp "github.com/bborbe/http"
)

//counterfeiter:generate -o mocks/json-handler-tx.go --fake-name JsonHandlerTx . JsonHandlerTx
type JsonHandlerTx interface {
	ServeHTTP(ctx context.Context, tx Tx, req *http.Request) (interface{}, error)
}

type JsonHandlerTxFunc func(ctx context.Context, tx Tx, req *http.Request) (interface{}, error)

func (j JsonHandlerTxFunc) ServeHTTP(ctx context.Context, tx Tx, req *http.Request) (interface{}, error) {
	return j(ctx, tx, req)
}

func NewJsonHandlerViewTx(db DB, jsonHandler JsonHandlerTx) libhttp.WithError {
	return libhttp.WithErrorFunc(func(ctx context.Context, resp http.ResponseWriter, req *http.Request) error {
		return db.View(ctx, func(ctx context.Context, tx Tx) error {
			result, err := jsonHandler.ServeHTTP(ctx, tx, req)
			if err != nil {
				return errors.Wrapf(ctx, err, "json handler failed")
			}
			resp.Header().Add(libhttp.ContentTypeHeaderName, libhttp.ApplicationJsonContentType)
			if err := json.NewEncoder(resp).Encode(result); err != nil {
				return errors.Wrapf(ctx, err, "encode json failed")
			}
			return nil
		})
	})
}

func NewJsonHandlerUpdateTx(db DB, jsonHandler JsonHandlerTx) libhttp.WithError {
	return libhttp.WithErrorFunc(func(ctx context.Context, resp http.ResponseWriter, req *http.Request) error {
		return db.Update(ctx, func(ctx context.Context, tx Tx) error {
			result, err := jsonHandler.ServeHTTP(ctx, tx, req)
			if err != nil {
				return errors.Wrapf(ctx, err, "json handler failed")
			}
			resp.Header().Add(libhttp.ContentTypeHeaderName, libhttp.ApplicationJsonContentType)
			if err := json.NewEncoder(resp).Encode(result); err != nil {
				return errors.Wrapf(ctx, err, "encode json failed")
			}
			return nil
		})
	})
}
