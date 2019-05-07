package utils

import (
	"fmt"
	"testing"
	"time"
)

var (
	// From godoc example for time:
	// China doesn't have daylight saving. It uses a fixed 8 hour offset from UTC.
	secondsEastOfUTC = int((8 * time.Hour).Seconds())
	beijing          = time.FixedZone("Beijing Time", secondsEastOfUTC)
)

func TestNewTimeInterval(t *testing.T) {
	cases := []struct {
		desc   string
		start  time.Time
		end    time.Time
		errMsg string
	}{
		{
			desc:   "error on end before start",
			start:  time.Date(2016, time.January, 1, 1, 30, 15, 0, time.UTC),
			end:    time.Date(2016, time.January, 1, 1, 0, 0, 0, time.UTC),
			errMsg: ErrEndBeforeStart,
		},
		{
			desc:  "both in UTC",
			start: time.Date(2016, time.January, 1, 1, 30, 15, 0, time.UTC),
			end:   time.Date(2016, time.January, 2, 1, 30, 15, 0, time.UTC),
		},
		{
			desc:  "start not in UTC",
			start: time.Date(2016, time.January, 1, 1, 30, 15, 0, beijing),
			end:   time.Date(2016, time.January, 10, 1, 30, 15, 0, time.UTC),
		},
		{
			desc:  "end not in UTC",
			start: time.Date(2016, time.January, 1, 1, 30, 15, 0, time.UTC),
			end:   time.Date(2016, time.January, 10, 1, 30, 15, 0, beijing),
		},

		{
			desc:  "both not in UTC",
			start: time.Date(2016, time.January, 1, 1, 30, 15, 0, beijing),
			end:   time.Date(2016, time.January, 10, 1, 30, 15, 0, beijing),
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			ti, err := NewTimeInterval(c.start, c.end)
			if c.errMsg == "" {
				if err != nil {
					t.Errorf("unexpected error: got %v", err)
				} else {
					wantStart := c.start.UTC()
					wantEnd := c.end.UTC()
					wantDuration := c.end.Sub(c.start)
					if got := ti.Start(); got != wantStart {
						t.Errorf("incorrect start: got %v want %v", got, wantStart)
					}
					if got := ti.End(); got != wantEnd {
						t.Errorf("incorrect end: got %v want %v", got, wantEnd)
					}
					if got := ti.Duration(); got != wantDuration {
						t.Errorf("incorrect duration: got %v want %v", got, wantDuration)
					}
				}
			} else if c.errMsg != "" {
				if err == nil {
					t.Errorf("unexpected lack of error")
				} else if got := err.Error(); got != c.errMsg {
					t.Errorf("unexpected error:\ngot\n%v\nwant\n%v", got, c.errMsg)
				}
			}
		})
	}
}

func TestTimeIntervalStartAndEndFuncs(t *testing.T) {
	start := time.Date(2016, time.January, 1, 12, 30, 45, 100, beijing)
	end := time.Date(2016, time.February, 1, 12, 30, 45, 100, beijing)
	ti, err := NewTimeInterval(start, end)
	if err != nil {
		t.Fatalf("unexpected error creating TimeInterval: got %v", err)
	}

	startUTC := start.UTC()
	endUTC := end.UTC()
	if got := ti.StartUnixNano(); got != startUTC.UnixNano() {
		t.Errorf("incorrect start unix nano: got %v want %v", got, startUTC.UnixNano())
	}
	if got := ti.EndUnixNano(); got != endUTC.UnixNano() {
		t.Errorf("incorrect end unix nano: got %v want %v", got, endUTC.UnixNano())
	}
	if got := ti.StartUnixMillis(); got != startUTC.UnixNano()/int64(time.Millisecond) {
		t.Errorf("incorrect start unix millis: got %v want %v",
			got, startUTC.UnixNano()/int64(time.Millisecond))
	}
	if got := ti.EndUnixMillis(); got != endUTC.UnixNano()/int64(time.Millisecond) {
		t.Errorf("incorrect end unix millis: got %v want %v",
			got, endUTC.UnixNano()/int64(time.Millisecond))
	}

	if got := ti.StartString(); got != startUTC.Format(time.RFC3339) {
		t.Errorf("incorrect start string: got %s want %s", got, startUTC.Format(time.RFC3339))
	}
	if got := ti.EndString(); got != endUTC.Format(time.RFC3339) {
		t.Errorf("incorrect start string: got %s want %s", got, endUTC.Format(time.RFC3339))
	}
}

func TestTimeIntervalOverlap(t *testing.T) {
	cases := []struct {
		desc        string
		start1      string
		end1        string
		start2      string
		end2        string
		wantOverlap bool
	}{
		{
			desc:        "completely disjoint",
			start1:      "2016-01-01",
			end1:        "2016-02-01",
			start2:      "2016-03-01",
			end2:        "2016-04-01",
			wantOverlap: false,
		},
		{
			desc:        "disjoint because of exclusive end",
			start1:      "2016-01-01",
			end1:        "2016-02-01",
			start2:      "2016-02-01",
			end2:        "2016-03-01",
			wantOverlap: false,
		},
		{
			desc:        "disjoint because of exclusive end #2",
			start1:      "2016-02-01",
			end1:        "2016-03-01",
			start2:      "2016-01-01",
			end2:        "2016-02-01",
			wantOverlap: false,
		},
		{
			desc:        "complete overlap",
			start1:      "2016-01-01",
			end1:        "2016-02-01",
			start2:      "2016-01-01",
			end2:        "2016-02-01",
			wantOverlap: true,
		},
		{
			desc:        "1 inside of 2",
			start1:      "2016-02-01",
			end1:        "2016-03-01",
			start2:      "2016-01-01",
			end2:        "2016-04-01",
			wantOverlap: true,
		},
		{
			desc:        "2 inside of 1",
			start1:      "2016-01-01",
			end1:        "2016-06-01",
			start2:      "2016-04-01",
			end2:        "2016-05-01",
			wantOverlap: true,
		},
		{
			desc:        "1 starts first, 2 ends later",
			start1:      "2016-01-01",
			end1:        "2016-03-01",
			start2:      "2016-02-01",
			end2:        "2016-04-01",
			wantOverlap: true,
		},
		{
			desc:        "1 starts later, 2 ends early",
			start1:      "2016-02-01",
			end1:        "2016-04-01",
			start2:      "2016-01-01",
			end2:        "2016-03-01",
			wantOverlap: true,
		},
	}
	layout := "2006-01-02"
	parse := func(s string) time.Time {
		x, err := time.Parse(layout, s)
		if err != nil {
			t.Fatalf("could not parse %v into time", s)
		}
		return x
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			ti1, err := NewTimeInterval(parse(c.start1), parse(c.end1))
			if err != nil {
				t.Errorf("could not create ti1: got %v", err)
			}
			ti2, err := NewTimeInterval(parse(c.start2), parse(c.end2))
			if err != nil {
				t.Errorf("could not create ti2: got %v", err)
			}
			if got := ti1.Overlap(ti2); got != c.wantOverlap {
				t.Errorf("incorrect overlap with ti1: got %v want %v", got, c.wantOverlap)
			}
			if got := ti2.Overlap(ti1); got != c.wantOverlap {
				t.Errorf("incorrect overlap with ti2: got %v want %v", got, c.wantOverlap)
			}
		})
	}
}

type randWindowCase struct {
	desc   string
	window time.Duration
	errMsg string
}

func (c randWindowCase) checkTimeInterval(t *testing.T, bigTI *TimeInterval, randTI *TimeInterval) {
	if got := randTI.Duration(); got != c.window {
		t.Errorf("incorrect duration: got %v want %v", got, c.window)
	}
	if randTI.Start().Before(bigTI.Start()) {
		t.Errorf("window start too early: %v is before %v", randTI.Start(), bigTI.Start())
	}
	if randTI.End().After(bigTI.End()) {
		t.Errorf("window end too late: %v is after %v", randTI.End(), bigTI.End())
	}
}

var rwCases = []randWindowCase{
	{
		desc:   "too large window",
		window: 2 * time.Hour,
		errMsg: fmt.Sprintf(errWindowTooLargeFmt, 2*time.Hour, 1*time.Hour),
	},
	{
		desc:   "window is exact",
		window: 1 * time.Hour,
		errMsg: fmt.Sprintf(errWindowTooLargeFmt, 1*time.Hour, 1*time.Hour),
	},
	{
		desc:   "window is just under",
		window: time.Hour - time.Second,
	},
	{
		desc:   "window is small",
		window: time.Second,
	},
	{
		desc:   "window is zero",
		window: 0,
	},
	{
		desc:   "window is negative",
		window: -1 * time.Second,
		errMsg: fmt.Sprintf(ErrEndBeforeStart),
	},
}

func TestTimeIntervalRandWindow(t *testing.T) {
	start := time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2016, time.January, 1, 1, 0, 0, 0, time.UTC)
	ti, err := NewTimeInterval(start, end) // 1 hour duration
	if err != nil {
		t.Fatalf("unexpected error creating TimeInterval: got %v", err)
	}

	for _, c := range rwCases {
		t.Run(c.desc, func(t *testing.T) {
			x, err := ti.RandWindow(c.window)
			if c.errMsg == "" {
				if err != nil {
					t.Errorf("unexpected error: got %v", err)
				} else {
					c.checkTimeInterval(t, ti, x)
				}
			} else {
				if err == nil {
					t.Errorf("unexpected lack of error")
				} else if got := err.Error(); got != c.errMsg {
					t.Errorf("unexpected error:\ngot\n%v\nwant\n%v", got, c.errMsg)
				}
			}
		})
	}
}

func TestTimeIntervalMustRandWindow(t *testing.T) {
	start := time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2016, time.January, 1, 1, 0, 0, 0, time.UTC)
	ti, err := NewTimeInterval(start, end) // 1 hour duration
	if err != nil {
		t.Fatalf("unexpected error creating TimeInterval: got %v", err)
	}

	for _, c := range rwCases {
		t.Run(c.desc, func(t *testing.T) {
			if c.errMsg != "" {
				defer func() {
					r := recover()
					if r == nil {
						t.Errorf("unexpected lack of panic")
					} else if got := r.(string); got != c.errMsg {
						t.Errorf("unexpected panic:\ngot\n%v\nwant\n%v", got, c.errMsg)
					}
				}()
			}
			x := ti.MustRandWindow(c.window)
			if c.errMsg == "" {
				c.checkTimeInterval(t, ti, x)
			}
		})
	}
}
