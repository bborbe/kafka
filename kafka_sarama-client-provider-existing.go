// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
)

// NewSaramaClientProviderExisting creates a SaramaClientProvider that wraps an existing Sarama client.
// This adapter allows existing Sarama clients to be used with the provider pattern without breaking backward compatibility.
// The provider returns the same client instance on every call to Client() and delegates Close() to the wrapped client.
func NewSaramaClientProviderExisting(
	saramaClient SaramaClient,
) SaramaClientProvider {
	return &saramaClientProviderExisting{
		saramaClient: saramaClient,
	}
}

type saramaClientProviderExisting struct {
	saramaClient SaramaClient
}

func (s *saramaClientProviderExisting) Client(_ context.Context) (SaramaClient, error) {
	return s.saramaClient, nil
}

func (s *saramaClientProviderExisting) Close() error {
	return s.saramaClient.Close()
}
