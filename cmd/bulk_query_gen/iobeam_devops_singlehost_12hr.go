package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsSingleHost12hr struct {
	IobeamDevops
}

func NewIobeamDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsSingleHost12hr{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsSingleHost12hr) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
