// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
)

// NewSaramaClientProviderPool creates a SaramaClientProvider that uses a connection pool.
// The pool manages client lifecycle with health checks and automatic reconnection.
// Pool configuration can be customized via poolOpts.
func NewSaramaClientProviderPool(
	brokers Brokers,
	poolOpts SaramaClientPoolOptions,
	opts ...SaramaConfigOptions,
) SaramaClientProvider {
	factory := func(ctx context.Context) (SaramaClient, error) {
		return CreateSaramaClient(ctx, brokers, opts...)
	}

	return &saramaClientProviderPool{
		pool: NewSaramaClientPool(factory, poolOpts),
	}
}

type saramaClientProviderPool struct {
	pool SaramaClientPool
}

func (s *saramaClientProviderPool) Client(ctx context.Context) (SaramaClient, error) {
	client, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "acquire client from pool failed")
	}

	return &pooledClient{
		SaramaClient: client,
		pool:         s.pool,
	}, nil
}

func (s *saramaClientProviderPool) Close() error {
	return s.pool.Close()
}

// pooledClient wraps a SaramaClient and returns it to the pool when closed.
type pooledClient struct {
	SaramaClient
	pool     SaramaClientPool
	released bool
}

func (p *pooledClient) Close() error {
	if p.released {
		return nil
	}
	p.released = true

	// Check if client is healthy before returning to pool
	healthy := !p.SaramaClient.Closed() && len(p.SaramaClient.Brokers()) > 0
	p.pool.Release(p.SaramaClient, healthy)
	return nil
}
