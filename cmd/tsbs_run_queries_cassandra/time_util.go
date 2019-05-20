package main

import (
	"fmt"
	"time"

	"github.com/timescale/tsbs/internal/utils"
)

type TimeIntervals []*utils.TimeInterval

// implement sort.Interface
func (x TimeIntervals) Len() int      { return len(x) }
func (x TimeIntervals) Swap(i, j int) { x[i], x[j] = x[j], x[i] }
func (x TimeIntervals) Less(i, j int) bool {
	return x[i].Start().Before(x[j].Start())
}

// bucketTimeIntervals is a helper that creates a slice of TimeInterval
// over the given span of time, in chunks of duration `window`.
func bucketTimeIntervals(start, end time.Time, window time.Duration) []*utils.TimeInterval {
	if end.Before(start) {
		panic("logic error in bucketTimeIntervals: bad input times")
	}
	ret := []*utils.TimeInterval{}

	start = start.Truncate(window)
	for start.Before(end) {
		ti, err := utils.NewTimeInterval(start, start.Add(window))
		if err != nil {
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
		ret = append(ret, ti)
		start = start.Add(window)
	}

	// sanity check
	tis := TimeIntervals(ret)
	for i := 0; i < len(tis)-1; i++ {
		if !tis.Less(i, i+1) {
			panic("logic error: unsorted buckets in bucketTimeIntervals")
		}
	}

	return ret
}
