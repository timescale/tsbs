package main

// TimeInterval represents an interval of time.
type TimeInterval struct {
	Start, End time.Time
}

// StartString formats the start of the time interval.
func (ti *TimeInterval) StartString() string {
	return ti.Start.Format(time.RFC3339)
}

// EndString formats the end of the time interval.
func (ti *TimeInterval) EndString() string {
	return ti.End.Format(time.RFC3339)
}

// TimeIntervals holds a slice of TimeInterval objects.
type TimeIntervals []TimeInterval

// RandChoice chooses a time interval (uniformly distribution).
func (tis TimeIntervals) RandChoice() *TimeInterval {
	return &tis[rand.Intn(len(tis))]
}

// NewTimeIntervals builds a slice of TimeInterval objects that slide over
// the given time span in increments of `window`.
func NewTimeIntervals(start, end time.Time, window time.Duration) TimeIntervals {
	xs := TimeIntervals{}
	for start.Add(window).Before(end) || start.Add(window).Equal(end) {
		x := TimeInterval{
			Start: start,
			End:   start.Add(window),
		}
		xs = append(xs, x)

		start = start.Add(window)
	}

	return xs
}
