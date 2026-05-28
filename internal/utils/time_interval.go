package utils

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	// ErrEndBeforeStart is the error message for when a TimeInterval's end time
	// would be before its start.
	ErrEndBeforeStart = "end time before start time"

	errWindowTooLargeFmt = "random window equal to or larger than TimeInterval: window %v, interval %v"
)

// TimeInterval represents an interval of time in UTC. That is, regardless of
// what timezone(s) are used for the beginning and end times, they will be
// converted to UTC and methods will return them as such.
type TimeInterval struct {
	start time.Time
	end   time.Time
}

// NewTimeInterval creates a new TimeInterval for a given start and end. If end
// is a time.Time before start, then an error is returned.
func NewTimeInterval(start, end time.Time) (*TimeInterval, error) {
	if end.Before(start) {
		return nil, fmt.Errorf(ErrEndBeforeStart)
	}
	return &TimeInterval{start.UTC(), end.UTC()}, nil
}

// Duration returns the time.Duration of the TimeInterval.
func (ti *TimeInterval) Duration() time.Duration {
	return ti.end.Sub(ti.start)
}

// Overlap detects whether the given TimeInterval overlaps with this
// TimeInterval, assuming an inclusive start boundary and exclusive end
// boundary.
func (ti *TimeInterval) Overlap(other *TimeInterval) bool {
	s1 := ti.Start()
	e1 := ti.End()

	s2 := other.Start()
	e2 := other.End()

	// If the two TimeIntervals share opposite boundaries, then they do not
	// overlap since the end is exclusive
	if e1 == s2 || e2 == s1 {
		return false
	}

	// If the start and end of the first are both before the start of the
	// second, they do not overlap.
	if s1.Before(s2) && e1.Before(s2) {
		return false
	}

	// Same as the previous case, just reversed.
	if s2.Before(s1) && e2.Before(s1) {
		return false
	}

	// Everything else must overlap
	return true
}

// RandWindow creates a TimeInterval of duration `window` at a uniformly-random
// start time within the time period represented by this TimeInterval.
func (ti *TimeInterval) RandWindow(window time.Duration) (*TimeInterval, error) {
	lower := ti.start.UnixNano()
	upper := ti.end.Add(-window).UnixNano()

	if upper <= lower {
		return nil, fmt.Errorf(errWindowTooLargeFmt, window, ti.end.Sub(ti.start))

	}

	start := lower + rand.Int63n(upper-lower)
	end := start + window.Nanoseconds()

	x, err := NewTimeInterval(time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	} else if x.Duration() != window {
		// Unless the logic above this changes, this should not happen, so
		// we panic in that case.
		panic("generated TimeInterval's duration does not equal window")
	}

	return x, nil
}

// MustRandWindow is the form of RandWindow that cannot error; if it does error,
// it causes a panic.
func (ti *TimeInterval) MustRandWindow(window time.Duration) *TimeInterval {
	res, err := ti.RandWindow(window)
	if err != nil {
		panic(err.Error())
	}
	return res
}

// Start returns the starting time in UTC.
func (ti *TimeInterval) Start() time.Time {
	return ti.start
}

// StartUnixNano returns the start time as nanoseconds.
func (ti *TimeInterval) StartUnixNano() int64 {
	return ti.start.UnixNano()
}

// StartUnixMillis returns the start time as milliseconds.
func (ti *TimeInterval) StartUnixMillis() int64 {
	return ti.start.UTC().UnixNano() / int64(time.Millisecond)
}

// StartString formats the start of the TimeInterval according to RFC3339.
func (ti *TimeInterval) StartString() string {
	return ti.start.Format(time.RFC3339)
}

// End returns the end time in UTC.
func (ti *TimeInterval) End() time.Time {
	return ti.end
}

// EndUnixNano returns the end time as nanoseconds.
func (ti *TimeInterval) EndUnixNano() int64 {
	return ti.end.UnixNano()
}

// EndUnixMillis returns the end time as milliseconds.
func (ti *TimeInterval) EndUnixMillis() int64 {
	return ti.end.UTC().UnixNano() / int64(time.Millisecond)
}

// EndString formats the end of the TimeInterval according to RFC3339.
func (ti *TimeInterval) EndString() string {
	return ti.end.Format(time.RFC3339)
}
