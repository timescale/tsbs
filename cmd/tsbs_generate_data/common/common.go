package common

import "math/rand"

// RandomStringSliceChoice returns a random string  from the provided slice of string slices.
func RandomStringSliceChoice(s []string) string {
	return s[rand.Intn(len(s))]
}

// RandomByteStringSliceChoice returns a random byte string slice  from the provided slice of byte string slices.
func RandomByteStringSliceChoice(s [][]byte) []byte {
	return s[rand.Intn(len(s))]
}

// RandomInt64SliceChoice returns a random int64 from an int64 slice.
func RandomInt64SliceChoice(s []int64) int64 {
	return s[rand.Intn(len(s))]
}
