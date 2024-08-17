package collection

import "strings"

func Compare[T ~string](a, b T) int {
	return strings.Compare(string(a), string(b))
}
