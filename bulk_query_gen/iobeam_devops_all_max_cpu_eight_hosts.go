package main

import "time"

// IobeamDevopsSingleHost produces Iobeam-specific queries for the devops single-host case.
type IobeamDevopsAllMaxCPUEightHosts struct {
	IobeamDevops
}

func NewIobeamDevopsAllMaxCPUEightHosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsAllMaxCPUEightHosts{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsAllMaxCPUEightHosts) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MaxAllCPUHourByMinuteEightHosts(q, scaleVar)
	return q
}
