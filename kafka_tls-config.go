// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/bborbe/errors"
	"github.com/golang/glog"
)

func NewTLSConfig(ctx context.Context, clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "load clientCert(%s) and clientKey(%s) failed", clientCertFile, clientKeyFile)
	}

	// Load CA cert
	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read file %s failed", caCertFile)
	}
	glog.V(3).Infof("read %d bytes from %s completed", len(caCert), caCertFile)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}
