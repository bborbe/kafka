// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

// DefaultMetadata defines the default metadata value used for offset manager operations.
const DefaultMetadata Metadata = "offsetmanager"

// Metadata represents a string-based metadata identifier used in Kafka operations.
type Metadata string

func (m Metadata) String() string {
	return string(m)
}
