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

var _ = DescribeTable("Offset String",
	func(offset libkafka.Offset, expectedString string) {
		Expect(offset.String()).To(Equal(expectedString))
	},
	Entry("newest", libkafka.OffsetNewest, "newest"),
	Entry("oldest", libkafka.OffsetOldest, "oldest"),
	Entry("zero", libkafka.Offset(0), "0"),
	Entry("positive", libkafka.Offset(123), "123"),
	Entry("negative", libkafka.Offset(-100), "-100"),
)

var _ = DescribeTable("Offset Int64",
	func(offset libkafka.Offset, expectedInt64 int64) {
		Expect(offset.Int64()).To(Equal(expectedInt64))
	},
	Entry("newest", libkafka.OffsetNewest, int64(-1)),
	Entry("oldest", libkafka.OffsetOldest, int64(-2)),
	Entry("zero", libkafka.Offset(0), int64(0)),
	Entry("positive", libkafka.Offset(123), int64(123)),
	Entry("negative", libkafka.Offset(-100), int64(-100)),
)

var _ = DescribeTable("Offset Bytes and FromBytes",
	func(offset libkafka.Offset) {
		bytes := offset.Bytes()
		Expect(bytes).To(HaveLen(8))

		restored := libkafka.OffsetFromBytes(bytes)
		Expect(restored).To(Equal(offset))
	},
	Entry("newest", libkafka.OffsetNewest),
	Entry("oldest", libkafka.OffsetOldest),
	Entry("zero", libkafka.Offset(0)),
	Entry("positive", libkafka.Offset(123)),
	Entry("negative", libkafka.Offset(-100)),
	Entry("large positive", libkafka.Offset(9223372036854775807)),
)

var _ = DescribeTable("ParseOffset",
	func(input string, expectedOffset *libkafka.Offset, expectError bool) {
		result, err := libkafka.ParseOffset(context.Background(), input)
		if expectError {
			Expect(err).NotTo(BeNil())
			Expect(result).To(BeNil())
		} else {
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(*expectedOffset))
		}
	},
	Entry("newest", "newest", libkafka.OffsetNewest.Ptr(), false),
	Entry("oldest", "oldest", libkafka.OffsetOldest.Ptr(), false),
	Entry("zero", "0", libkafka.Offset(0).Ptr(), false),
	Entry("positive number", "123", libkafka.Offset(123).Ptr(), false),
	Entry("negative number", "-100", libkafka.Offset(-100).Ptr(), false),
	Entry("large number", "9223372036854775807", libkafka.Offset(9223372036854775807).Ptr(), false),
	Entry("invalid string", "invalid", (*libkafka.Offset)(nil), true),
	Entry("float", "123.456", (*libkafka.Offset)(nil), true),
	Entry("empty string", "", (*libkafka.Offset)(nil), true),
)

var _ = Describe("Offset Ptr", func() {
	It("should return pointer to offset", func() {
		offset := libkafka.Offset(123)
		ptr := offset.Ptr()
		Expect(ptr).NotTo(BeNil())
		Expect(*ptr).To(Equal(offset))
	})
})
