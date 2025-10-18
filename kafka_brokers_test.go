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
	"ParseBrokersFromString",
	func(input string, expectedBrokers libkafka.Brokers) {
		Expect(libkafka.ParseBrokersFromString(input)).To(Equal(expectedBrokers))
	},
	Entry("without schema", "my-cluster:9092", libkafka.Brokers{"plain://my-cluster:9092"}),
	Entry("plain", "plain://my-cluster:9092", libkafka.Brokers{"plain://my-cluster:9092"}),
	Entry("tls", "tls://my-cluster:9092", libkafka.Brokers{"tls://my-cluster:9092"}),
	Entry(
		"multi without schema",
		"my-cluster-a:9092,my-cluster-b:9092",
		libkafka.Brokers{"plain://my-cluster-a:9092", "plain://my-cluster-b:9092"},
	),
	Entry(
		"multi plain",
		"plain://my-cluster-a:9092,plain://my-cluster-b:9092",
		libkafka.Brokers{"plain://my-cluster-a:9092", "plain://my-cluster-b:9092"},
	),
	Entry(
		"multi tls",
		"tls://my-cluster-a:9092,tls://my-cluster-b:9092",
		libkafka.Brokers{"tls://my-cluster-a:9092", "tls://my-cluster-b:9092"},
	),
)

var _ = DescribeTable("Brokers UnmarshalText",
	func(input string, expectedBrokers libkafka.Brokers) {
		var brokers libkafka.Brokers
		err := brokers.UnmarshalText([]byte(input))
		Expect(err).To(BeNil())
		Expect(brokers).To(Equal(expectedBrokers))
	},
	Entry("single without schema", "my-cluster:9092", libkafka.Brokers{"plain://my-cluster:9092"}),
	Entry("single plain", "plain://my-cluster:9092", libkafka.Brokers{"plain://my-cluster:9092"}),
	Entry("single tls", "tls://my-cluster:9092", libkafka.Brokers{"tls://my-cluster:9092"}),
	Entry(
		"multi without schema",
		"my-cluster-a:9092,my-cluster-b:9092",
		libkafka.Brokers{"plain://my-cluster-a:9092", "plain://my-cluster-b:9092"},
	),
	Entry(
		"multi plain",
		"plain://my-cluster-a:9092,plain://my-cluster-b:9092",
		libkafka.Brokers{"plain://my-cluster-a:9092", "plain://my-cluster-b:9092"},
	),
	Entry(
		"multi tls",
		"tls://my-cluster-a:9092,tls://my-cluster-b:9092",
		libkafka.Brokers{"tls://my-cluster-a:9092", "tls://my-cluster-b:9092"},
	),
	Entry(
		"multi mixed schemas",
		"plain://my-cluster-a:9092,tls://my-cluster-b:9092,my-cluster-c:9092",
		libkafka.Brokers{
			"plain://my-cluster-a:9092",
			"tls://my-cluster-b:9092",
			"plain://my-cluster-c:9092",
		},
	),
)
