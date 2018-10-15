package devops

import "math/rand"

func randomByteStringSliceChoice(s [][]byte) []byte {
	return s[rand.Intn(len(s))]
}

func randomInt64SliceChoice(s []int64) int64 {
	return s[rand.Intn(len(s))]
}
