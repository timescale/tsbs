package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsGroupByOrderByLimit struct {
	IobeamDevops
}

func NewIobeamDevopsGroupByOrderByLimit(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsGroupByOrderByLimit{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
