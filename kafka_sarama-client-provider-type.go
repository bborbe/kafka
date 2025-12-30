// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/bborbe/errors"
	"github.com/bborbe/parse"
	"github.com/bborbe/validation"
)

const (
	// SaramaClientProviderTypeReused creates a provider that reuses a single client for all calls.
	SaramaClientProviderTypeReused SaramaClientProviderType = "reused"
	// SaramaClientProviderTypeNew creates a provider that creates a new client for each call.
	SaramaClientProviderTypeNew SaramaClientProviderType = "new"
	// SaramaClientProviderTypePool creates a provider that uses a connection pool with health checks.
	SaramaClientProviderTypePool SaramaClientProviderType = "pool"
)

// AvailableSaramaClientProviderTypes contains all valid SaramaClientProviderType values.
var AvailableSaramaClientProviderTypes = SaramaClientProviderTypes{
	SaramaClientProviderTypeReused,
	SaramaClientProviderTypeNew,
	SaramaClientProviderTypePool,
}

// ParseSaramaClientProviderType parses a value into a SaramaClientProviderType.
func ParseSaramaClientProviderType(
	ctx context.Context,
	value interface{},
) (*SaramaClientProviderType, error) {
	switch v := value.(type) {
	case SaramaClientProviderType:
		return v.Ptr(), nil
	case *SaramaClientProviderType:
		return v, nil
	default:
		str, err := parse.ParseString(ctx, value)
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "parse value as string failed")
		}
		return SaramaClientProviderType(str).Ptr(), nil
	}
}

// SaramaClientProviderType defines the type of Sarama client provider to use.
type SaramaClientProviderType string

// String returns the string representation of the SaramaClientProviderType.
func (s SaramaClientProviderType) String() string {
	return string(s)
}

// Ptr returns a pointer to the SaramaClientProviderType.
func (s SaramaClientProviderType) Ptr() *SaramaClientProviderType {
	return &s
}

// Validate checks if the SaramaClientProviderType is valid.
func (s SaramaClientProviderType) Validate(ctx context.Context) error {
	if !AvailableSaramaClientProviderTypes.Contains(s) {
		return errors.Wrapf(ctx, validation.Error, "unknown sarama client provider type '%s'", s)
	}
	return nil
}

// SaramaClientProviderTypes is a slice of SaramaClientProviderType values.
type SaramaClientProviderTypes []SaramaClientProviderType

// Contains checks if the given SaramaClientProviderType is in the slice.
func (s SaramaClientProviderTypes) Contains(providerType SaramaClientProviderType) bool {
	for _, t := range s {
		if t == providerType {
			return true
		}
	}
	return false
}
