package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsHighCpu struct {
	IobeamDevops
}

func NewIobeamDevopsHighCpu(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsHighCpu{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsHighCpu) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.HighCPU(q, scaleVar)
	return q
}
