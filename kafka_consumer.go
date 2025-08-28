// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import "context"

// Consumer defines the interface for consuming messages from Kafka.
type Consumer interface {
	// Consume starts consuming messages and blocks until the context is cancelled or an error occurs.
	Consume(ctx context.Context) error
}

// ConsumerFunc is a function type that implements the Consumer interface.
type ConsumerFunc func(ctx context.Context) error

// Consume implements the Consumer interface for ConsumerFunc.
func (c ConsumerFunc) Consume(ctx context.Context) error {
	return c(ctx)
}
