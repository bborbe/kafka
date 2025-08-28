// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package kafka provides a production-ready Kafka abstraction library built on top of IBM's Sarama client.
//
// This package offers a clean interface for Kafka operations while adding essential features like
// metrics integration, batch processing, transaction support, and comprehensive message handling patterns.
//
// The library follows interface-driven architecture principles, making it composition-friendly
// with extensive interface support for testing and modularity.
//
// Key Features:
//   - Interface-driven design with Consumer, SyncProducer, MessageHandler interfaces
//   - Built-in Prometheus metrics integration
//   - Batch processing with configurable parameters and delays
//   - Transaction support with atomic message processing
//   - TLS support for secure connections
//   - Multiple offset management strategies
//   - Decorator patterns for extending functionality
//   - Function types for functional programming support
//
// Basic usage example:
//
//	ctx := context.Background()
//	brokers := kafka.ParseBrokers("localhost:9092")
//
//	// Create producer
//	producer, err := kafka.NewSyncProducer(ctx, brokers)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer producer.Close()
//
//	// Send message
//	msg := &sarama.ProducerMessage{
//		Topic: "my-topic",
//		Value: sarama.StringEncoder("Hello Kafka!"),
//	}
//
//	partition, offset, err := producer.SendMessage(ctx, msg)
//	if err != nil {
//		log.Fatal(err)
//	}
package kafka
