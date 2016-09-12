package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsMultipleOrsByHost struct {
	IobeamDevops
}

func NewIobeamDevopsMultipleOrsByHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsMultipleOrsByHost{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsMultipleOrsByHost) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MultipleMemOrsByHost(q, scaleVar)
	return q
}
