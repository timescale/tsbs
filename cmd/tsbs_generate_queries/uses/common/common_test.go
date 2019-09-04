package common

import (
	"sort"
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

func TestGetRandomSubsetPerm(t *testing.T) {
	cases := []struct {
		scale  int
		nItems int
	}{
		{scale: 10, nItems: 0},
		{scale: 10, nItems: 1},
		{scale: 10, nItems: 5},
		{scale: 10, nItems: 10},
		{scale: 1000, nItems: 1000},
	}

	for _, c := range cases {
		ret, err := GetRandomSubsetPerm(c.nItems, c.scale)
		if err != nil {
			t.Fatalf("unexpected error: got %v", err)
		}
		if len(ret) != c.nItems {
			t.Errorf("return list not long enough: got %d want %d (scale %d)", len(ret), c.nItems, c.scale)
		}
		sort.Ints(ret)
		prev := -1
		for _, x := range ret {
			if x == prev {
				t.Errorf("duplicate int found in sorted result (scale %d nItems %d)", c.scale, c.nItems)
			}
			prev = x
		}
	}
}

func TestGetRandomSubsetPermError(t *testing.T) {
	ret, err := GetRandomSubsetPerm(11, 10)
	if ret != nil {
		t.Errorf("return was non-nil: %v", ret)
	}
	if got := err.Error(); got != errMoreItemsThanScale {
		t.Errorf("incorrect output:\ngot\n%s\nwant\n%s", got, errMoreItemsThanScale)
	}
}
