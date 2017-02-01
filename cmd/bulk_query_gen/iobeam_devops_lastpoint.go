package main

import "time"

// IobeamDevopsGroupby produces Iobeam-specific queries for the devops groupby case.
type IobeamDevopsLastPointPerHost struct {
	IobeamDevops
}

func NewIobeamDevopsLastPointPerHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsLastPointPerHost{
		IobeamDevops: *underlying,
	}

}

func (d *IobeamDevopsLastPointPerHost) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.LastPointPerHost(q, scaleVar)
	return q
}
