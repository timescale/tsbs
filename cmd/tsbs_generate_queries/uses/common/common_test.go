package common

import (
	"testing"
	"time"

	"github.com/timescale/tsbs/internal/utils"
)

func TestNewCore(t *testing.T) {
	s := time.Now()
	e := time.Now()
	c, err := NewCore(s, e, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := c.Interval.Start().UnixNano(); got != s.UnixNano() {
		t.Errorf("NewCore does not have right start time: got %d want %d", got, s.UnixNano())
	}
	if got := c.Interval.EndUnixNano(); got != e.UnixNano() {
		t.Errorf("NewCore does not have right end time: got %d want %d", got, e.UnixNano())
	}
	if got := c.Scale; got != 10 {
		t.Errorf("NewCore does not have right scale: got %d want %d", got, 10)
	}
}

func TestNewCoreEndBeforeStart(t *testing.T) {
	e := time.Now()
	s := e.Add(time.Second)
	_, err := NewCore(s, e, 10)
	if got := err.Error(); got != utils.ErrEndBeforeStart {
		t.Errorf("NewCore did not error correctly:\ngot\n%s\nwant\n%s", got, utils.ErrEndBeforeStart)
	}
}
