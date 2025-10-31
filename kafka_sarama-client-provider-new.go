// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"

	"github.com/bborbe/errors"
)

// NewSaramaClientProviderNew creates a SaramaClientProvider that creates a new client for each call.
// All created clients are tracked and closed when Close() is called.
// The provided options will be applied to all created clients.
func NewSaramaClientProviderNew(
	brokers Brokers,
	opts ...SaramaConfigOptions,
) SaramaClientProvider {
	return &saramaClientProviderNew{
		brokers: brokers,
		opts:    opts,
		clients: make([]SaramaClient, 0),
	}
}

type saramaClientProviderNew struct {
	brokers Brokers
	opts    []SaramaConfigOptions
	clients []SaramaClient
	mu      sync.Mutex
	closed  bool
}

func (s *saramaClientProviderNew) Client(
	ctx context.Context,
) (SaramaClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.Errorf(ctx, "provider is closed")
	}

	client, err := CreateSaramaClient(ctx, s.brokers, s.opts...)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "create sarama client failed")
	}

	s.clients = append(s.clients, client)
	return client, nil
}

func (s *saramaClientProviderNew) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	var errs []error
	for _, client := range s.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
