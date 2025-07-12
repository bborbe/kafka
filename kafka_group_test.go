// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = DescribeTable("Group String",
	func(group libkafka.Group, expectedString string) {
		Expect(group.String()).To(Equal(expectedString))
	},
	Entry("simple", libkafka.Group("test-group"), "test-group"),
	Entry("with underscore", libkafka.Group("test_group"), "test_group"),
	Entry("with dash", libkafka.Group("test-group"), "test-group"),
	Entry("with dot", libkafka.Group("test.group"), "test.group"),
	Entry("with numbers", libkafka.Group("testgroup123"), "testgroup123"),
	Entry("empty", libkafka.Group(""), ""),
)
