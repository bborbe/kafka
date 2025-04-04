// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = DescribeTable("CreateCandlePartitionTopic",
	func(expectedTopic libkafka.Topic, expectError bool) {
		result := expectedTopic.Validate(context.Background())
		if expectError {
			Expect(result).NotTo(BeNil())
		} else {
			Expect(result).To(BeNil())
		}
	},
	Entry("simple", libkafka.Topic("test"), false),
	Entry("with underscore", libkafka.Topic("hello_world"), false),
	Entry("with dash", libkafka.Topic("hello-world"), false),
	Entry("with dot", libkafka.Topic("hello.world"), false),
	Entry("with numbers", libkafka.Topic("helloworld1337"), false),
	Entry("with invalid char", libkafka.Topic("helloworld!"), true),
	Entry("with invalid char", libkafka.Topic("hello\\world"), true),
)

var _ = DescribeTable("TopicFromStrings",
	func(strings []string, expectedTopic libkafka.Topic) {
		Expect(libkafka.TopicFromStrings(strings...)).To(Equal(expectedTopic))
	},
	Entry("simple", []string{"test"}, libkafka.Topic("test")),
	Entry("upper", []string{"TEST"}, libkafka.Topic("test")),
	Entry("with underscore", []string{"hello_world"}, libkafka.Topic("hello_world")),
	Entry("with dash", []string{"hello-world"}, libkafka.Topic("hello-world")),
	Entry("with dot", []string{"hello.world"}, libkafka.Topic("hello.world")),
	Entry("with numbers", []string{"helloworld1337"}, libkafka.Topic("helloworld1337")),
	Entry("with invalid char", []string{"hello\\world"}, libkafka.Topic("hello-world")),
	Entry("with invalid char end", []string{"helloworld!"}, libkafka.Topic("helloworld")),
)
