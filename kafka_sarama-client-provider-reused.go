// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"

	"github.com/bborbe/errors"
)

// NewSaramaClientProviderReused creates a SaramaClientProvider that reuses a single client for all calls.
// The client is created lazily on the first call to Client().
// Default options can be provided which will be applied to all clients unless overridden.
func NewSaramaClientProviderReused(
	brokers Brokers,
	opts ...SaramaConfigOptions,
) SaramaClientProvider {
	return &saramaClientProviderReused{
		brokers:     brokers,
		defaultOpts: opts,
	}
}

type saramaClientProviderReused struct {
	brokers     Brokers
	defaultOpts []SaramaConfigOptions
	client      SaramaClient
	mu          sync.Mutex
	closed      bool
}

func (s *saramaClientProviderReused) Client(
	ctx context.Context,
	opts ...SaramaConfigOptions,
) (SaramaClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.Errorf(ctx, "provider is closed")
	}

	if s.client != nil {
		return s.client, nil
	}

	// Merge default options with provided options
	allOpts := append([]SaramaConfigOptions{}, s.defaultOpts...)
	allOpts = append(allOpts, opts...)

	client, err := CreateSaramaClient(ctx, s.brokers, allOpts...)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create sarama client failed")
	}

	s.client = client
	return s.client, nil
}

func (s *saramaClientProviderReused) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	if s.client != nil {
		if err := s.client.Close(); err != nil {
			return err
		}
	}

	return nil
}
