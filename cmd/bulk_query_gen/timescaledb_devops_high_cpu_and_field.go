package main

import "time"

type TimescaleDBDevopsHighCPUAndField struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsHighCPUAndField(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsHighCPUAndField{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsHighCPUAndField) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.HighCPUAndField(q, scaleVar)
	return q
}
