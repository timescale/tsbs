package common

import (
	"bytes"
	"testing"
)

func testIfInByteStringSlice(t *testing.T, arr [][]byte, choice []byte) {
	for _, x := range arr {
		if bytes.Equal(x, choice) {
			return
		}
	}
	t.Errorf("could not find choice in array: %s", choice)
}

func TestRandomByteSliceChoice(t *testing.T) {
	arr := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	}
	// One million attempts ought to catch it?
	for i := 0; i < 1000000; i++ {
		choice := RandomByteStringSliceChoice(arr)
		testIfInByteStringSlice(t, arr, choice)
	}
}

func testIfInInt64Slice(t *testing.T, arr []int64, choice int64) {
	for _, x := range arr {
		if x == choice {
			return
		}
	}
	t.Errorf("could not find choice in array: %d", choice)
}

func TestRandomInt64Choice(t *testing.T) {
	arr := []int64{0, 10000, 9999}
	// One million attempts ought to catch it?
	for i := 0; i < 1000000; i++ {
		choice := RandomInt64SliceChoice(arr)
		testIfInInt64Slice(t, arr, choice)
	}
}
