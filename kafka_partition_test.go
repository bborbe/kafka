// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	"context"

	"github.com/bborbe/collection"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	libkafka "github.com/bborbe/kafka"
)

var _ = DescribeTable("Partition String",
	func(partition libkafka.Partition, expectedString string) {
		Expect(partition.String()).To(Equal(expectedString))
	},
	Entry("zero", libkafka.Partition(0), "0"),
	Entry("positive", libkafka.Partition(123), "123"),
	Entry("negative", libkafka.Partition(-1), "-1"),
)

var _ = DescribeTable("Partition Int32",
	func(partition libkafka.Partition, expectedInt32 int32) {
		Expect(partition.Int32()).To(Equal(expectedInt32))
	},
	Entry("zero", libkafka.Partition(0), int32(0)),
	Entry("positive", libkafka.Partition(123), int32(123)),
	Entry("negative", libkafka.Partition(-1), int32(-1)),
	Entry("max int32", libkafka.Partition(2147483647), int32(2147483647)),
	Entry("min int32", libkafka.Partition(-2147483648), int32(-2147483648)),
)

var _ = DescribeTable("Partition Bytes and FromBytes",
	func(partition libkafka.Partition) {
		bytes := partition.Bytes()
		Expect(bytes).To(HaveLen(4))

		restored := libkafka.PartitionFromBytes(bytes)
		Expect(restored).To(Equal(partition))
	},
	Entry("zero", libkafka.Partition(0)),
	Entry("positive", libkafka.Partition(123)),
	Entry("negative", libkafka.Partition(-1)),
	Entry("max int32", libkafka.Partition(2147483647)),
	Entry("min int32", libkafka.Partition(-2147483648)),
)

var _ = DescribeTable("ParsePartition",
	func(input string, expectedPartition *libkafka.Partition, expectError bool) {
		result, err := libkafka.ParsePartition(context.Background(), input)
		if expectError {
			Expect(err).NotTo(BeNil())
			Expect(result).To(BeNil())
		} else {
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(*expectedPartition))
		}
	},
	Entry("zero", "0", collection.Ptr(libkafka.Partition(0)), false),
	Entry("positive number", "123", collection.Ptr(libkafka.Partition(123)), false),
	Entry("negative number", "-1", collection.Ptr(libkafka.Partition(-1)), false),
	Entry("max int32", "2147483647", collection.Ptr(libkafka.Partition(2147483647)), false),
	Entry("min int32", "-2147483648", collection.Ptr(libkafka.Partition(-2147483648)), false),
	Entry("invalid string", "invalid", (*libkafka.Partition)(nil), true),
	Entry("float", "123.456", (*libkafka.Partition)(nil), true),
	Entry("empty string", "", (*libkafka.Partition)(nil), true),
	Entry("too large", "2147483648", (*libkafka.Partition)(nil), true),
	Entry("too small", "-2147483649", (*libkafka.Partition)(nil), true),
)

var _ = DescribeTable("PartitionsFromInt32",
	func(input []int32, expectedPartitions libkafka.Partitions) {
		result := libkafka.PartitionsFromInt32(input)
		Expect(result).To(Equal(expectedPartitions))
	},
	Entry("empty", []int32{}, libkafka.Partitions(nil)),
	Entry("single", []int32{0}, libkafka.Partitions{libkafka.Partition(0)}),
	Entry("multiple", []int32{0, 1, 2}, libkafka.Partitions{
		libkafka.Partition(0),
		libkafka.Partition(1),
		libkafka.Partition(2),
	}),
	Entry("negative", []int32{-1, 0, 1}, libkafka.Partitions{
		libkafka.Partition(-1),
		libkafka.Partition(0),
		libkafka.Partition(1),
	}),
)
