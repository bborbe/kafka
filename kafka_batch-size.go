package kafka

import "strconv"

type BatchSize int

func (s BatchSize) String() string {
	return strconv.Itoa(s.Int())
}

func (s BatchSize) Int() int {
	return s.Int()
}
