// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = DescribeTable(
	"ParseBroker",
	func(input string, expectedBroker libkafka.Broker) {
		result := libkafka.ParseBroker(input)
		Expect(result).To(Equal(expectedBroker))
	},
	Entry("plain without schema", "localhost:9092", libkafka.Broker("plain://localhost:9092")),
	Entry("plain with schema", "plain://localhost:9092", libkafka.Broker("plain://localhost:9092")),
	Entry("tls with schema", "tls://localhost:9093", libkafka.Broker("tls://localhost:9093")),
	Entry("ip without schema", "127.0.0.1:9092", libkafka.Broker("plain://127.0.0.1:9092")),
	Entry("ip with schema", "plain://127.0.0.1:9092", libkafka.Broker("plain://127.0.0.1:9092")),
	Entry(
		"hostname with port",
		"kafka.example.com:9092",
		libkafka.Broker("plain://kafka.example.com:9092"),
	),
)

var _ = DescribeTable("Broker String",
	func(broker libkafka.Broker, expectedString string) {
		Expect(broker.String()).To(Equal(expectedString))
	},
	Entry("plain", libkafka.Broker("plain://localhost:9092"), "plain://localhost:9092"),
	Entry("tls", libkafka.Broker("tls://localhost:9093"), "tls://localhost:9093"),
	Entry("without schema", libkafka.Broker("localhost:9092"), "localhost:9092"),
)

var _ = DescribeTable("Broker Schema",
	func(broker libkafka.Broker, expectedSchema libkafka.BrokerSchema) {
		Expect(broker.Schema()).To(Equal(expectedSchema))
	},
	Entry("plain", libkafka.Broker("plain://localhost:9092"), libkafka.PlainSchema),
	Entry("tls", libkafka.Broker("tls://localhost:9093"), libkafka.TLSSchema),
	Entry("without schema", libkafka.Broker("localhost:9092"), libkafka.BrokerSchema("")),
	Entry("invalid format", libkafka.Broker("invalid"), libkafka.BrokerSchema("")),
	Entry("empty", libkafka.Broker(""), libkafka.BrokerSchema("")),
)

var _ = DescribeTable("Broker Host",
	func(broker libkafka.Broker, expectedHost string) {
		Expect(broker.Host()).To(Equal(expectedHost))
	},
	Entry("plain", libkafka.Broker("plain://localhost:9092"), "localhost:9092"),
	Entry("tls", libkafka.Broker("tls://localhost:9093"), "localhost:9093"),
	Entry("ip address", libkafka.Broker("plain://127.0.0.1:9092"), "127.0.0.1:9092"),
	Entry("hostname", libkafka.Broker("tls://kafka.example.com:9092"), "kafka.example.com:9092"),
	Entry("without schema", libkafka.Broker("localhost:9092"), ""),
	Entry("invalid format", libkafka.Broker("invalid"), ""),
	Entry("empty", libkafka.Broker(""), ""),
)

var _ = DescribeTable(
	"Broker UnmarshalText",
	func(input string, expectedBroker libkafka.Broker) {
		var broker libkafka.Broker
		err := broker.UnmarshalText([]byte(input))
		Expect(err).To(BeNil())
		Expect(broker).To(Equal(expectedBroker))
	},
	Entry("without schema", "localhost:9092", libkafka.Broker("plain://localhost:9092")),
	Entry("plain schema", "plain://localhost:9092", libkafka.Broker("plain://localhost:9092")),
	Entry("tls schema", "tls://localhost:9093", libkafka.Broker("tls://localhost:9093")),
	Entry("ip without schema", "127.0.0.1:9092", libkafka.Broker("plain://127.0.0.1:9092")),
	Entry("ip with schema", "plain://127.0.0.1:9092", libkafka.Broker("plain://127.0.0.1:9092")),
	Entry(
		"hostname with port",
		"kafka.example.com:9092",
		libkafka.Broker("plain://kafka.example.com:9092"),
	),
)
