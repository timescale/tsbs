package inputs

import (
	"testing"
	"time"
)

func TestIsIn(t *testing.T) {
	arr := []string{"foo", "bar", "baz"}
	arr2 := []string{"oof", "foo ", "nada", "123"}

	// Test positive cases
	for _, s := range arr {
		if !isIn(s, arr) {
			t.Errorf("%s not found in %v incorrectly", s, arr)
		}
	}
	for _, s := range arr2 {
		if !isIn(s, arr2) {
			t.Errorf("%s not found in %v incorrectly", s, arr)
		}
	}

	// Test negative cases
	for _, s := range arr {
		if isIn(s, arr2) {
			t.Errorf("%s found in %v incorrectly", s, arr)
		}
	}
	for _, s := range arr2 {
		if isIn(s, arr) {
			t.Errorf("%s found in %v incorrectly", s, arr)
		}
	}

}

const (
	correctTimeStr   = "2016-01-01T00:00:00Z"
	incorrectTimeStr = "2017-01-01"
)

var correctTime = time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestParseUTCTime(t *testing.T) {
	parsedTime, err := ParseUTCTime(correctTimeStr)
	if err != nil {
		t.Errorf("unexpected error: got %v", err)
	} else if parsedTime != correctTime {
		t.Errorf("did not get correct time back: got %v want %v", parsedTime, correctTime)
	}

	_, err = ParseUTCTime(incorrectTimeStr)
	if err == nil {
		t.Errorf("unexpected lack of error")
	}
}
