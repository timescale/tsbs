package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsHighCPU struct {
	IobeamDevops
}

func NewIobeamDevopsHighCPU(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsHighCPU{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsHighCPU) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.HighCPU(q, scaleVar)
	return q
}
