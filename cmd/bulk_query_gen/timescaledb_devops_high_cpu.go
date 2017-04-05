package main

import "time"

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsHighCPU struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsHighCPU(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsHighCPU{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsHighCPU) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.HighCPU(q, scaleVar)
	return q
}
