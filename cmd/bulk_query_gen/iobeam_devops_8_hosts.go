package main

import "time"

// IobeamDevops8Hosts produces Iobeam-specific queries for the devops groupby case.
type IobeamDevops8Hosts struct {
	IobeamDevops
}

func NewIobeamDevops8Hosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevops8Hosts{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevops8Hosts) Dispatch(_, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
