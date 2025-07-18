# Kafka

A Go library providing utilities and abstractions for Apache Kafka, built on top of the Sarama client library.

## Features

- **Consumer & Producer interfaces** - Simple abstractions for Kafka consumers and producers
- **Message handlers** - Flexible message processing with support for batching, transactions, and metrics
- **Offset management** - Built-in offset tracking and storage mechanisms
- **JSON encoding/decoding** - Convenient JSON message serialization
- **Metrics integration** - Prometheus metrics support for monitoring
- **TLS configuration** - Secure connection support

## Usage

```go
import "github.com/bborbe/kafka"

// Create a simple consumer
consumer := kafka.ConsumerFunc(func(ctx context.Context) error {
    // Your consumer logic here
    return nil
})

// Consume messages
err := consumer.Consume(ctx)
```

## Installation

```bash
go get github.com/bborbe/kafka
```
