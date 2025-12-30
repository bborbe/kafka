// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/bborbe/errors"
	"github.com/golang/glog"
)

//counterfeiter:generate -o mocks/kafka-sarama-client-pool.go --fake-name KafkaSaramaClientPool . SaramaClientPool

// SaramaClientPool manages a pool of Sarama clients with health checks.
// It reuses healthy clients and discards unhealthy ones automatically.
type SaramaClientPool interface {
	// Acquire returns a healthy client from pool or creates new one.
	// The client is health-checked before being returned.
	Acquire(ctx context.Context) (SaramaClient, error)

	// Release returns client to pool (discards if unhealthy).
	// The healthy parameter indicates whether the client is still healthy.
	Release(client SaramaClient, healthy bool)

	// Close closes all pooled connections.
	Close() error
}

// SaramaClientPoolOptions configures the client pool behavior.
type SaramaClientPoolOptions struct {
	// MaxPoolSize is the maximum number of clients to keep in the pool.
	// Default: 10
	MaxPoolSize int

	// HealthCheckTimeout is the timeout for health checks.
	// Default: 5s
	HealthCheckTimeout time.Duration
}

// DefaultSaramaClientPoolOptions returns default pool configuration.
var DefaultSaramaClientPoolOptions = SaramaClientPoolOptions{
	MaxPoolSize:        10,
	HealthCheckTimeout: 5 * time.Second,
}

// NewSaramaClientPool creates a new client pool with the specified factory function.
// Use DefaultSaramaClientPoolOptions for default configuration.
func NewSaramaClientPool(
	factory func(context.Context) (SaramaClient, error),
	opts SaramaClientPoolOptions,
) SaramaClientPool {
	return &saramaClientPool{
		available:          make(chan SaramaClient, opts.MaxPoolSize),
		factory:            factory,
		maxSize:            opts.MaxPoolSize,
		healthCheckTimeout: opts.HealthCheckTimeout,
	}
}

type saramaClientPool struct {
	available          chan SaramaClient
	factory            func(context.Context) (SaramaClient, error)
	maxSize            int
	healthCheckTimeout time.Duration
	mu                 sync.Mutex
	closed             bool
	allClients         []SaramaClient // Track all clients for cleanup
}

func (p *saramaClientPool) Acquire(ctx context.Context) (SaramaClient, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.Errorf(ctx, "pool is closed")
	}
	p.mu.Unlock()

	// Try to get a client from the pool
	select {
	case client := <-p.available:
		// Health check the pooled client
		if p.isHealthy(ctx, client) {
			glog.V(3).Info("reusing healthy client from pool")
			return client, nil
		}
		// Client is unhealthy, close and discard it
		glog.V(2).Info("discarding unhealthy client from pool")
		if err := client.Close(); err != nil {
			glog.Warningf("failed to close unhealthy client: %v", err)
		}
		p.removeClient(client)
		// Fall through to create new client
	default:
		// Pool is empty, create new client
	}

	// Create new client
	client, err := p.factory(ctx)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "create client failed")
	}

	p.mu.Lock()
	p.allClients = append(p.allClients, client)
	p.mu.Unlock()

	glog.V(3).Infof("created new client (pool size: %d/%d)", len(p.available), p.maxSize)
	return client, nil
}

func (p *saramaClientPool) Release(client SaramaClient, healthy bool) {
	if client == nil {
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		// Pool is closed, just close the client
		if err := client.Close(); err != nil {
			glog.Warningf("failed to close client: %v", err)
		}
		return
	}
	p.mu.Unlock()

	if !healthy {
		// Client is unhealthy, close and discard it
		glog.V(2).Info("discarding unhealthy client")
		if err := client.Close(); err != nil {
			glog.Warningf("failed to close unhealthy client: %v", err)
		}
		p.removeClient(client)
		return
	}

	// Try to return healthy client to pool
	select {
	case p.available <- client:
		glog.V(3).Infof("returned client to pool (pool size: %d/%d)", len(p.available), p.maxSize)
	default:
		// Pool is full, close the client
		glog.V(3).Info("pool full, closing client")
		if err := client.Close(); err != nil {
			glog.Warningf("failed to close client: %v", err)
		}
		p.removeClient(client)
	}
}

func (p *saramaClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	// Close all clients in the pool
	close(p.available)
	var errs []error
	for client := range p.available {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close any clients not in the pool
	for _, client := range p.allClients {
		if client != nil && !client.Closed() {
			if err := client.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (p *saramaClientPool) isHealthy(ctx context.Context, client SaramaClient) bool {
	if client == nil || client.Closed() {
		return false
	}

	healthCtx, cancel := context.WithTimeout(ctx, p.healthCheckTimeout)
	defer cancel()

	// Check if we can get brokers (indicates connection is alive)
	brokers := client.Brokers()
	if len(brokers) == 0 {
		glog.V(3).Info("client unhealthy: no brokers available")
		return false
	}

	// Additional check: ensure context hasn't timed out
	select {
	case <-healthCtx.Done():
		glog.V(3).Info("client health check timed out")
		return false
	default:
		return true
	}
}

func (p *saramaClientPool) removeClient(client SaramaClient) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, c := range p.allClients {
		if c == client {
			p.allClients = append(p.allClients[:i], p.allClients[i+1:]...)
			break
		}
	}
}
