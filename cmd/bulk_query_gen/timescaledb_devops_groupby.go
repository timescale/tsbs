package main

import "time"

// IobeamDevopsGroupby produces Iobeam-specific queries for the devops groupby case.
type IobeamDevopsGroupby struct {
	IobeamDevops
}

func NewIobeamDevopsGroupBy(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsGroupby{
		IobeamDevops: *underlying,
	}

}

func (d *IobeamDevopsGroupby) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
