package main

import "time"

type IobeamDevopsHighCPUAndField struct {
	IobeamDevops
}

func NewIobeamDevopsHighCPUAndField(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newIobeamDevopsCommon(dbConfig, start, end).(*IobeamDevops)
	return &IobeamDevopsHighCPUAndField{
		IobeamDevops: *underlying,
	}
}

func (d *IobeamDevopsHighCPUAndField) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
