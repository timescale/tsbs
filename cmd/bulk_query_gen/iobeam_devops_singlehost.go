package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsSingleHost struct {
	IobeamDevops
}

func NewIobeamDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsSingleHost{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsSingleHost) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
