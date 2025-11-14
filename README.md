# Kafka Library

[![CI](https://github.com/bborbe/kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/bborbe/kafka/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/bborbe/kafka)](https://goreportcard.com/report/github.com/bborbe/kafka)
[![Go Reference](https://pkg.go.dev/badge/github.com/bborbe/kafka.svg)](https://pkg.go.dev/github.com/bborbe/kafka)
[![License](https://img.shields.io/badge/License-BSD%202--Clause-blue.svg)](LICENSE)

A production-ready Kafka abstraction library for Go, built on top of IBM's Sarama client. Provides a clean interface for Kafka operations while adding essential features like metrics, batch processing, transaction support, and comprehensive message handling patterns.

## Features

- 🚀 **Production Ready**: Built for high-throughput, low-latency applications
- 🔧 **Interface-Driven Architecture**: Composition-friendly with extensive interface support
- 📊 **Built-in Metrics**: Prometheus integration throughout all components
- 🔄 **Batch Processing**: Efficient batch message handling with configurable parameters
- 🔒 **Transaction Support**: Atomic message processing with transaction decorators
- 🎯 **Flexible Patterns**: Function types, decorators, and composition patterns
- 🛡️ **TLS Support**: Built-in TLS configuration for secure connections
- ⚡ **Offset Management**: Multiple offset tracking strategies and fallback behavior

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Basic Producer](#basic-producer)
  - [JSON Message Sender](#json-message-sender)
  - [Basic Consumer](#basic-consumer)
- [Advanced Usage](#advanced-usage)
  - [Batch Processing](#batch-processing)
  - [Metrics Integration](#metrics-integration)
  - [Transaction Support](#transaction-support)
  - [Performance Tuning](#performance-tuning)
- [Architecture](#architecture)
- [Testing](#testing)
- [Development](#development)
- [Dependencies](#dependencies)
- [License](#license)

---

## Installation

```bash
go get github.com/bborbe/kafka
```

## Quick Start

### Basic Producer

```go
package main

import (
    "context"
    "log"

    "github.com/bborbe/kafka"
    "github.com/IBM/sarama"
)

func main() {
    ctx := context.Background()
    brokers := kafka.ParseBrokers("localhost:9092")

    // Create producer with optional Sarama config
    producer, err := kafka.NewSyncProducer(ctx, brokers)
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // Send message
    msg := &sarama.ProducerMessage{
        Topic: "my-topic",
        Value: sarama.StringEncoder("Hello Kafka!"),
    }

    partition, offset, err := producer.SendMessage(ctx, msg)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Message sent to partition %d at offset %d", partition, offset)
}
```

### JSON Message Sender

```go
package main

import (
    "context"
    "log"

    "github.com/bborbe/kafka"
    "github.com/bborbe/log"
)

func main() {
    ctx := context.Background()
    brokers := kafka.ParseBrokers("localhost:9092")

    // Create producer
    producer, err := kafka.NewSyncProducer(ctx, brokers)
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // Create JSON sender with producer and log sampler factory
    logSamplerFactory := log.NewLogSamplerFactory()
    sender := kafka.NewJsonSender(producer, logSamplerFactory)

    // Define a value type for your data
    type EventValue struct {
        UserID    int    `json:"user_id"`
        Action    string `json:"action"`
        Timestamp string `json:"timestamp"`
    }

    // Create and send update
    value := EventValue{
        UserID:    123,
        Action:    "login",
        Timestamp: "2023-01-01T00:00:00Z",
    }

    key := kafka.NewKey("user-123")
    err = sender.SendUpdate(ctx, kafka.Topic("events"), key, value)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Basic Consumer

```go
package main

import (
    "context"
    "log"

    "github.com/bborbe/kafka"
    "github.com/bborbe/log"
    "github.com/IBM/sarama"
)

func main() {
    ctx := context.Background()
    brokers := kafka.ParseBrokers("localhost:9092")

    // Create Sarama client
    config := sarama.NewConfig()
    config.Version = sarama.V2_6_0_0
    saramaClient, err := sarama.NewClient(brokers.Hosts(), config)
    if err != nil {
        log.Fatal(err)
    }
    defer saramaClient.Close()

    // Create message handler
    handler := kafka.MessageHandlerFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
        log.Printf("Received message: %s", string(msg.Value))
        return nil
    })

    // Create consumer with log sampler factory
    logSamplerFactory := log.NewLogSamplerFactory()
    consumer := kafka.NewSimpleConsumer(
        saramaClient,
        kafka.Topic("my-topic"),
        kafka.OffsetNewest,
        handler,
        logSamplerFactory,
    )

    // Start consuming
    if err := consumer.Consume(ctx); err != nil {
        log.Fatal(err)
    }
}
```

## Advanced Usage

### Batch Processing

```go
// Create batch handler from your existing handler
batchHandler := kafka.NewMessageHandlerBatch(
    handler,
    kafka.ParseBatchSize(100),                    // Process 100 messages at once
    kafka.NewMessageHandlerBatchDelay(time.Second), // Or wait 1 second
)

// Use with NewSimpleConsumerBatch for batch processing
consumer := kafka.NewSimpleConsumerBatch(
    saramaClient,
    kafka.Topic("my-topic"),
    kafka.OffsetNewest,
    batchHandler,
    100, // batch size
    logSamplerFactory,
)
```

### Metrics Integration

```go
// Wrap producer with metrics
producer = kafka.NewSyncProducerMetrics(producer, metrics)

// Wrap message handler with metrics  
handler = kafka.NewMessageHandlerMetrics(handler, metrics)
```

### Transaction Support

```go
// Create transactional message handler
txHandler := kafka.NewMessageHandlerTx(ctx, handler, db)

consumer, err := kafka.NewSimpleConsumer(ctx, brokers, "my-topic", txHandler)
```

### Performance Tuning

For high-throughput scenarios, customize the Sarama configuration when creating the client provider:

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/bborbe/kafka"
    "github.com/bborbe/errors"
    "github.com/IBM/sarama"
)

func main() {
    ctx := context.Background()
    brokers := kafka.ParseBrokers("localhost:9092")

    // Create high-performance client provider with tuned configuration
    saramaClientProvider, err := kafka.NewSaramaClientProviderByType(
        ctx,
        kafka.SaramaClientProviderTypeReused,
        brokers,
        func(config *sarama.Config) {
            config.Consumer.MaxWaitTime = 1000 * time.Millisecond  // Increase from 500ms for larger batches
            config.Consumer.Fetch.Default = 10 * 1024 * 1024       // 10MB (from 1MB default)
            config.Consumer.Fetch.Max = 50 * 1024 * 1024           // 50MB max fetch size
            config.ChannelBufferSize = 1000                         // Buffer 1000 messages (from 256 default)
        },
    )
    if err != nil {
        log.Fatal(errors.Wrapf(ctx, err, "create sarama client provider failed"))
    }
    defer saramaClientProvider.Close()

    // Use the provider with your consumer...
}
```

**Configuration Parameters:**

- **`Consumer.MaxWaitTime`**: Maximum time broker waits before responding (higher = larger batches but higher latency; default: 500ms)
- **`Consumer.Fetch.Default`**: Target data to fetch per request (higher = fewer requests, better throughput; default: 1MB)
- **`Consumer.Fetch.Max`**: Maximum data broker can return (protects against excessive memory usage; default: varies)
- **`ChannelBufferSize`**: Internal channel buffer size (higher = better throughput under load, more memory; default: 256)

**Trade-offs:** This configuration optimizes for **throughput over latency**. Larger fetch sizes, longer wait times, and bigger buffers increase memory usage and per-message latency but significantly improve overall throughput. Use for high-volume scenarios where batch processing efficiency matters more than individual message latency.

## Architecture

### Core Interfaces

- **`Consumer`** - Basic message consumption interface
- **`SyncProducer`** - Synchronous message production interface  
- **`MessageHandler`** - Message processing logic interface
- **`OffsetManager`** - Offset tracking strategies interface

### Design Patterns

- **Interface-Driven**: All major components are interface-based for easy composition and testing
- **Decorator Pattern**: Wrap components with additional functionality (metrics, transactions, filtering)
- **Function Types**: Functional programming support via `ConsumerFunc`, `MessageHandlerFunc`

## Testing

This library uses [Ginkgo](https://github.com/onsi/ginkgo) and [Gomega](https://github.com/onsi/gomega) for testing.

```bash
# Run all tests
make test

# Run tests with verbose output
ginkgo -v

# Run specific test
go test -run TestSpecific
```

## Development

```bash
# Install dependencies and run all checks
make precommit

# Format code
make format

# Generate mocks
make generate

# Run linting and security checks
make check
```

## Dependencies

- **Kafka Client**: [IBM Sarama v1.45.2](https://github.com/IBM/sarama)
- **Testing**: [Ginkgo v2](https://github.com/onsi/ginkgo) & [Gomega](https://github.com/onsi/gomega)
- **Metrics**: [Prometheus Client](https://github.com/prometheus/client_golang)

## License

This project is licensed under the BSD 2-Clause License - see the [LICENSE](LICENSE) file for details.
