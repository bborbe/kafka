// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

const (
	// ErrorClassCorruption is a corrupt record batch or an undecodable packet.
	ErrorClassCorruption ErrorClass = "corruption"
	// ErrorClassBrokerSide is an error the Kafka broker itself reported.
	ErrorClassBrokerSide ErrorClass = "broker_side"
	// ErrorClassOther is any consumer error that is neither of the above.
	ErrorClassOther ErrorClass = "other"
)

// AvailableErrorClasses lists every ErrorClass value the system emits.
var AvailableErrorClasses = ErrorClasses{
	ErrorClassCorruption,
	ErrorClassBrokerSide,
	ErrorClassOther,
}

// ErrorClass classifies a consumer error for the error_class metric label.
type ErrorClass string

// String returns the string representation of the ErrorClass.
func (e ErrorClass) String() string {
	return string(e)
}

// ErrorClasses is a slice of ErrorClass values.
type ErrorClasses []ErrorClass

// Contains checks if the given ErrorClass is in the slice.
func (e ErrorClasses) Contains(class ErrorClass) bool {
	for _, c := range e {
		if c == class {
			return true
		}
	}
	return false
}

// ClassifyError derives the ErrorClass from the error's type. It never inspects the message text
// except where IsCorruptionError already does.
func ClassifyError(err error) ErrorClass {
	if IsCorruptionError(err) {
		return ErrorClassCorruption
	}
	var kerr sarama.KError
	if errors.As(err, &kerr) {
		return ErrorClassBrokerSide
	}
	return ErrorClassOther
}
