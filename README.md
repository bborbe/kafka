# Kafka Library

[![Go Report Card](https://goreportcard.com/badge/github.com/bborbe/kafka)](https://goreportcard.com/report/github.com/bborbe/kafka)
[![GoDoc](https://godoc.org/github.com/bborbe/kafka?status.svg)](https://godoc.org/github.com/bborbe/kafka)
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
    
    // Create producer
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
)

func main() {
    ctx := context.Background()
    brokers := kafka.ParseBrokers("localhost:9092")
    
    // Create JSON sender
    sender, err := kafka.NewJsonSender(ctx, brokers)
    if err != nil {
        log.Fatal(err)
    }
    defer sender.Close()
    
    // Send JSON message
    data := map[string]interface{}{
        "user_id": 123,
        "action":  "login",
        "timestamp": "2023-01-01T00:00:00Z",
    }
    
    err = sender.SendJson(ctx, kafka.Topic("events"), data)
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
    "github.com/IBM/sarama"
)

func main() {
    ctx := context.Background()
    brokers := kafka.ParseBrokers("localhost:9092")
    
    // Create message handler
    handler := kafka.MessageHandlerFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
        log.Printf("Received message: %s", string(msg.Value))
        return nil
    })
    
    // Create consumer
    consumer, err := kafka.NewSimpleConsumer(ctx, brokers, "my-topic", handler)
    if err != nil {
        log.Fatal(err)
    }
    
    // Start consuming
    if err := consumer.Consume(ctx); err != nil {
        log.Fatal(err)
    }
}
```

## Advanced Usage

### Batch Processing

```go
// Create batch handler
batchHandler := kafka.NewMessageHandlerBatch(
    handler,
    kafka.ParseBatchSize(100),                    // Process 100 messages at once
    kafka.NewMessageHandlerBatchDelay(time.Second), // Or wait 1 second
)

consumer, err := kafka.NewSimpleConsumer(ctx, brokers, "my-topic", batchHandler)
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
