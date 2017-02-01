package main

import "time"

type IobeamDevopsMultipleOrs struct {
	IobeamDevops
}

func NewIobeamDevopsMultipleOrs(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsMultipleOrs{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsMultipleOrs) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MultipleMemOrs(q, scaleVar)
	return q
}
