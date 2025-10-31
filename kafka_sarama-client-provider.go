// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
)

//counterfeiter:generate -o mocks/kafka-sarama-client-provider.go --fake-name KafkaSaramaClientProvider . SaramaClientProvider

// SaramaClientProvider provides Sarama Kafka clients and manages their lifecycle.
// It creates clients on demand and tracks them for proper cleanup via Close().
type SaramaClientProvider interface {
	// Client creates and returns a Sarama client.
	// The behavior depends on the implementation - it may return a new client or reuse an existing one.
	Client(ctx context.Context) (SaramaClient, error)
	// Close closes all Sarama clients that were created by this provider.
	// This method is safe to call multiple times and safe to defer immediately after creation.
	Close() error
}

// NewSaramaClientProviderByType creates a SaramaClientProvider based on the specified type.
func NewSaramaClientProviderByType(
	ctx context.Context,
	providerType SaramaClientProviderType,
	brokers Brokers,
	opts ...SaramaConfigOptions,
) (SaramaClientProvider, error) {
	switch providerType {
	case SaramaClientProviderTypeReused:
		return NewSaramaClientProviderReused(brokers, opts...), nil
	case SaramaClientProviderTypeNew:
		return NewSaramaClientProviderNew(brokers, opts...), nil
	default:
		return nil, errors.Errorf(ctx, "unknown sarama client provider type: %s", providerType)
	}
}
