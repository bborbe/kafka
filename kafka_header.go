// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"github.com/IBM/sarama"
)

func ParseHeader(saramaHeaders []*sarama.RecordHeader) Header {
	header := Header{}
	for _, saramaHeader := range saramaHeaders {
		header.Add(string(saramaHeader.Key), string(saramaHeader.Value))
	}
	return header
}

type Header map[string][]string

func (h Header) Add(key string, value string) {
	h[key] = append(h[key], value)
}

func (h Header) Set(key string, values []string) {
	h[key] = values
}

func (h Header) Remove(key string) {
	delete(h, key)
}

func (h Header) Get(key string) string {
	values := h[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (h Header) AsSaramaHeaders() []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0, len(h))
	for key, values := range h {
		for _, value := range values {
			headers = append(headers, sarama.RecordHeader{
				Key:   []byte(key),
				Value: []byte(value),
			})
		}
	}
	return headers
}
