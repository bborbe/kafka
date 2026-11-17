// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{
			name: "packet decoding error",
			err:  sarama.PacketDecodingError{Info: "CRC mismatch"},
			want: ErrorClassCorruption,
		},
		{
			name: "consumer error wrapping packet decoding error",
			err: &sarama.ConsumerError{
				Topic:     "t",
				Partition: 0,
				Err:       sarama.PacketDecodingError{Info: "CRC mismatch"},
			},
			want: ErrorClassCorruption,
		},
		{
			name: "not leader for partition",
			err:  sarama.ErrNotLeaderForPartition,
			want: ErrorClassBrokerSide,
		},
		{
			name: "consumer error wrapping not leader for partition",
			err: &sarama.ConsumerError{
				Topic:     "t",
				Partition: 0,
				Err:       sarama.ErrNotLeaderForPartition,
			},
			want: ErrorClassBrokerSide,
		},
		{
			name: "wrapped not leader for partition",
			err: errors.Wrapf(
				context.Background(),
				sarama.ErrNotLeaderForPartition,
				"broker said no",
			),
			want: ErrorClassBrokerSide,
		},
		{
			name: "message says leader but type is not a broker error",
			err:  stderrors.New("not a leader"),
			want: ErrorClassOther,
		},
		{
			name: "consumer error wrapping unrelated error",
			err: &sarama.ConsumerError{
				Topic:     "t",
				Partition: 0,
				Err:       stderrors.New("not a leader"),
			},
			want: ErrorClassOther,
		},
		{
			name: "unrelated error",
			err:  stderrors.New("something else entirely"),
			want: ErrorClassOther,
		},
		{
			name: "nil error",
			err:  nil,
			want: ErrorClassOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyError(tt.err)
			if got != tt.want {
				t.Errorf("ClassifyError() = %q, want %q", got, tt.want)
			}
			if !AvailableErrorClasses.Contains(got) {
				t.Errorf("ClassifyError() = %q is not a member of AvailableErrorClasses", got)
			}
		})
	}
}

func TestClassifyError_ThreeDistinctClasses(t *testing.T) {
	corruption := ClassifyError(sarama.PacketDecodingError{Info: "CRC mismatch"})
	brokerSide := ClassifyError(sarama.ErrNotLeaderForPartition)
	other := ClassifyError(stderrors.New("something else entirely"))

	if corruption == brokerSide {
		t.Errorf("corruption and broker_side share the value %q", corruption)
	}
	if corruption == other {
		t.Errorf("corruption and other share the value %q", corruption)
	}
	if brokerSide == other {
		t.Errorf("broker_side and other share the value %q", brokerSide)
	}
}
