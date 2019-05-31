package common

import "math/rand"

// RandomByteStringSliceChoice returns a random byte slice from the provided slice of byte slices.
func RandomByteStringSliceChoice(s [][]byte) []byte {
	return s[rand.Intn(len(s))]
}

// RandomInt64SliceChoice returns a random int64 from an int64 slice.
func RandomInt64SliceChoice(s []int64) int64 {
	return s[rand.Intn(len(s))]
}
