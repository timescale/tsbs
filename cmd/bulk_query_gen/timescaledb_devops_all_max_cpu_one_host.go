package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsAllMaxCPUOneHost struct {
	IobeamDevops
}

func NewIobeamDevopsAllMaxCPUOneHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsAllMaxCPUOneHost{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsAllMaxCPUOneHost) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MaxAllCPUHourByMinuteOneHost(q, scaleVar)
	return q
}
