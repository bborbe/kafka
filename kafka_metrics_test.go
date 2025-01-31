// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka_test

import (
	. "github.com/onsi/ginkgo/v2"

	"github.com/bborbe/kafka"
)

var _ = Describe("Metrics", func() {
	var metrics kafka.Metrics
	BeforeEach(func() {
		metrics = kafka.NewMetrics()
	})
	JustBeforeEach(func() {
		metrics.SyncProducerTotalCounterInc("my-topic")
	})
})
